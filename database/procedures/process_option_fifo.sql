CREATE OR REPLACE PROCEDURE process_option_fifo()
LANGUAGE plpgsql
AS $$
DECLARE
    rec                   RECORD;
    contracts_to_close    NUMERIC;
    lot_cursor            RECORD;
    consumed_qty          NUMERIC;
    new_open_qty          NUMERIC;
    total_cost_portion    NUMERIC;
    proceeds_calc         NUMERIC;
BEGIN
    RAISE NOTICE 'Starting Option FIFO processing...';

    -- 1) Clear existing data (full rebuild approach)
    TRUNCATE TABLE option_realized_gains, fifo_option_lots RESTART IDENTITY CASCADE;

    -- 2) Process enriched option transactions in FIFO order.
    -- (Uses our new table "options_transactions_enriched", which already includes normalized_instrument
    -- and the new ordering id, option_transaction_id.)
    FOR rec IN
        SELECT *
        FROM options_transactions_enriched
        ORDER BY 
            expiration_date, 
            option_type, 
            strike_price, 
            description,
            CASE 
                WHEN raw_trans_code IN ('BTO', 'STO') THEN 1
                WHEN raw_trans_code IN ('STC', 'BTC') THEN 2
                WHEN raw_trans_code = 'OEXP' THEN 3
                ELSE 99
            END,
            option_transaction_id
    LOOP
        ---------------------------------------------------------------------------
        -- Case: Open a LONG position (BTO)
        ---------------------------------------------------------------------------
        IF rec.raw_trans_code = 'BTO' THEN
            INSERT INTO fifo_option_lots (
                instrument,
                description,
                expiration_date,
                option_type,
                strike_price,
                open_contracts,
                total_cost,
                avg_cost,
                open_date,
                open_transaction_id
            )
            VALUES (
                rec.normalized_instrument,   -- use the normalized instrument value
                rec.description,
                rec.expiration_date,
                rec.option_type,
                rec.strike_price,
                rec.quantity,                -- positive for long positions
                (-rec.amount),               -- convert negative debit to positive cost basis
                (-rec.amount) / rec.quantity,
                rec.corrected_activity_date::date,
                rec.option_transaction_id    -- using the new ordering id
            );

        ---------------------------------------------------------------------------
        -- Case: Open a SHORT position (STO)
        ---------------------------------------------------------------------------
        ELSIF rec.raw_trans_code = 'STO' THEN
            INSERT INTO fifo_option_lots (
                instrument,
                description,
                expiration_date,
                option_type,
                strike_price,
                open_contracts,
                total_cost,
                avg_cost,
                open_date,
                open_transaction_id
            )
            VALUES (
                rec.normalized_instrument,
                rec.description,
                rec.expiration_date,
                rec.option_type,
                rec.strike_price,
                -rec.quantity,               -- negative to indicate short position
                -rec.amount,                 -- credit stored as negative cost basis
                ((-rec.amount) / (-rec.quantity)),
                rec.corrected_activity_date::date,
                rec.option_transaction_id
            );

        ---------------------------------------------------------------------------
        -- Case: Close a LONG position (STC)
        ---------------------------------------------------------------------------
        ELSIF rec.raw_trans_code = 'STC' THEN
            contracts_to_close := rec.quantity;
            FOR lot_cursor IN
                SELECT *
                FROM fifo_option_lots
                WHERE instrument = rec.normalized_instrument
                  AND expiration_date = rec.expiration_date
                  AND option_type = rec.option_type
                  AND strike_price = rec.strike_price
                  AND open_contracts > 0
                ORDER BY lot_id
            LOOP
                EXIT WHEN contracts_to_close <= 0;
                IF lot_cursor.open_contracts <= contracts_to_close THEN
                    consumed_qty := lot_cursor.open_contracts;
                ELSE
                    consumed_qty := contracts_to_close;
                END IF;
                total_cost_portion := lot_cursor.avg_cost * consumed_qty;
                proceeds_calc := (rec.amount / rec.quantity) * consumed_qty;
                INSERT INTO option_realized_gains (
                    instrument,
                    description,
                    expiration_date,
                    option_type,
                    strike_price,
                    close_transaction_id,
                    lot_id,
                    allocated_contracts,
                    allocated_cost,
                    proceeds,
                    realized_gain
                )
                VALUES (
                    lot_cursor.instrument,
                    lot_cursor.description,
                    lot_cursor.expiration_date,
                    lot_cursor.option_type,
                    lot_cursor.strike_price,
                    rec.option_transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    total_cost_portion,
                    proceeds_calc,
                    (proceeds_calc - total_cost_portion)
                );
                new_open_qty := lot_cursor.open_contracts - consumed_qty;
                UPDATE fifo_option_lots
                  SET open_contracts = new_open_qty,
                      total_cost = lot_cursor.avg_cost * new_open_qty,
                      updated_at = NOW()
                  WHERE lot_id = lot_cursor.lot_id;
                contracts_to_close := contracts_to_close - consumed_qty;
            END LOOP;
            IF contracts_to_close > 0 THEN
                RAISE WARNING 
                  'STC leftover=% for normalized_instrument=% | desc=% | exp=% | strike=% | option_trans_id=%',
                    contracts_to_close,
                    rec.normalized_instrument,
                    rec.description,
                    rec.expiration_date,
                    rec.strike_price,
                    rec.option_transaction_id;
            END IF;

        ---------------------------------------------------------------------------
        -- Case: Close a SHORT position (BTC)
        ---------------------------------------------------------------------------
        ELSIF rec.raw_trans_code = 'BTC' THEN
            contracts_to_close := rec.quantity;
            FOR lot_cursor IN
                SELECT *
                FROM fifo_option_lots
                WHERE instrument = rec.normalized_instrument
                  AND expiration_date = rec.expiration_date
                  AND option_type = rec.option_type
                  AND strike_price = rec.strike_price
                  AND open_contracts < 0
                ORDER BY lot_id
            LOOP
                EXIT WHEN contracts_to_close <= 0;
                IF ABS(lot_cursor.open_contracts) <= contracts_to_close THEN
                    consumed_qty := ABS(lot_cursor.open_contracts);
                ELSE
                    consumed_qty := contracts_to_close;
                END IF;
                total_cost_portion := lot_cursor.avg_cost * consumed_qty;
                proceeds_calc := (rec.amount / rec.quantity) * consumed_qty;
                INSERT INTO option_realized_gains (
                    instrument,
                    description,
                    expiration_date,
                    option_type,
                    strike_price,
                    close_transaction_id,
                    lot_id,
                    allocated_contracts,
                    allocated_cost,
                    proceeds,
                    realized_gain
                )
                VALUES (
                    lot_cursor.instrument,
                    lot_cursor.description,
                    lot_cursor.expiration_date,
                    lot_cursor.option_type,
                    lot_cursor.strike_price,
                    rec.option_transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    total_cost_portion,
                    proceeds_calc,
                    (proceeds_calc - total_cost_portion)
                );
                new_open_qty := lot_cursor.open_contracts + consumed_qty;
                UPDATE fifo_option_lots
                  SET open_contracts = new_open_qty,
                      total_cost = lot_cursor.avg_cost * new_open_qty,
                      updated_at = NOW()
                  WHERE lot_id = lot_cursor.lot_id;
                contracts_to_close := contracts_to_close - consumed_qty;
            END LOOP;
            IF contracts_to_close > 0 THEN
                RAISE WARNING 
                  'BTC leftover=% for normalized_instrument=% | desc=% | exp=% | strike=% | option_trans_id=%',
                    contracts_to_close,
                    rec.normalized_instrument,
                    rec.description,
                    rec.expiration_date,
                    rec.strike_price,
                    rec.option_transaction_id;
            END IF;

        ---------------------------------------------------------------------------
        -- Case: Option Expiration (OEXP)
        ---------------------------------------------------------------------------
        ELSIF rec.raw_trans_code = 'OEXP' THEN
            -- First, determine the effective expired quantity.
            DECLARE
                oexp_qty NUMERIC;
                total_open NUMERIC;
            BEGIN
                IF rec.raw_quantity IS NULL OR rec.raw_quantity ~ '^\d+(\.\d+)?$' THEN
                    oexp_qty := rec.quantity;
                ELSE
                    oexp_qty := regexp_replace(rec.raw_quantity, '[^0-9\.]', '', 'g')::NUMERIC;
                END IF;
                
                -- Optional: check the total open quantity for this option.
                SELECT COALESCE(SUM(ABS(open_contracts)), 0)
                  INTO total_open
                  FROM fifo_option_lots
                  WHERE instrument = rec.normalized_instrument
                    AND expiration_date = rec.expiration_date
                    AND option_type = rec.option_type
                    AND strike_price = rec.strike_price;
                    
                IF oexp_qty <> total_open THEN
                    RAISE NOTICE 'For OEXP, total open=% does not equal oexp_qty=% for normalized_instrument=% | exp=% | strike=% | desc=%',
                        total_open, oexp_qty, rec.normalized_instrument, rec.expiration_date, rec.strike_price, rec.description;
                    -- Depending on your business rules, you might adjust oexp_qty here.
                END IF;
                
                -- Process matching lots using oexp_qty.
                FOR lot_cursor IN
                    SELECT *
                    FROM fifo_option_lots
                    WHERE instrument = rec.normalized_instrument
                      AND expiration_date = rec.expiration_date
                      AND option_type = rec.option_type
                      AND strike_price = rec.strike_price
                      AND open_contracts != 0
                    ORDER BY lot_id
                LOOP
                    EXIT WHEN oexp_qty <= 0;
                    IF ABS(lot_cursor.open_contracts) <= oexp_qty THEN
                        consumed_qty := ABS(lot_cursor.open_contracts);
                    ELSE
                        consumed_qty := oexp_qty;
                    END IF;
                    proceeds_calc := 0;  -- No proceeds on expiration.
                    total_cost_portion := lot_cursor.avg_cost * consumed_qty;
                    
                    INSERT INTO option_realized_gains (
                        instrument,
                        description,
                        expiration_date,
                        option_type,
                        strike_price,
                        close_transaction_id,
                        lot_id,
                        allocated_contracts,
                        allocated_cost,
                        proceeds,
                        realized_gain
                    )
                    VALUES (
                        lot_cursor.instrument,
                        lot_cursor.description,
                        lot_cursor.expiration_date,
                        lot_cursor.option_type,
                        lot_cursor.strike_price,
                        rec.option_transaction_id,
                        lot_cursor.lot_id,
                        consumed_qty,
                        total_cost_portion,
                        proceeds_calc,
                        (proceeds_calc - total_cost_portion)
                    );
                    
                    IF lot_cursor.open_contracts > 0 THEN
                        new_open_qty := lot_cursor.open_contracts - consumed_qty;
                    ELSE
                        new_open_qty := lot_cursor.open_contracts + consumed_qty;
                    END IF;
                    UPDATE fifo_option_lots
                        SET open_contracts = new_open_qty,
                            total_cost = lot_cursor.avg_cost * new_open_qty,
                            updated_at = NOW()
                        WHERE lot_id = lot_cursor.lot_id;
                    
                    oexp_qty := oexp_qty - consumed_qty;
                END LOOP;
                
                IF oexp_qty > 0 THEN
                    RAISE WARNING 
                      'OEXP leftover=% for normalized_instrument=% | desc=% | exp=% | strike=% | option_trans_id=%',
                      oexp_qty,
                      rec.normalized_instrument,
                      rec.description,
                      rec.expiration_date,
                      rec.strike_price,
                      rec.option_transaction_id;
                END IF;
            END;
            
        ELSE
            RAISE NOTICE 'Unhandled code: % for normalized_instrument=% exp=%',
                         rec.raw_trans_code,
                         rec.normalized_instrument,
                         rec.expiration_date;
        END IF;
    END LOOP;
    
    -- Post-Processing Cleanup:
    -- Optionally, after the main loop, if there remain any lots in fifo_option_lots that are expired
    -- (i.e. there is an OEXP record for that option) but still have nonzero open_contracts, force-close them.
    FOR lot_cursor IN
        SELECT fl.*
        FROM fifo_option_lots fl
        JOIN options_transactions_enriched ote
          ON fl.instrument = ote.normalized_instrument
         AND fl.expiration_date = ote.expiration_date
         AND fl.option_type = ote.option_type
         AND fl.strike_price = ote.strike_price
         AND ote.raw_trans_code = 'OEXP'
        WHERE fl.open_contracts <> 0
        ORDER BY fl.lot_id
    LOOP
        consumed_qty := ABS(lot_cursor.open_contracts);
        proceeds_calc := 0;
        total_cost_portion := lot_cursor.avg_cost * consumed_qty;
        
        INSERT INTO option_realized_gains (
             instrument,
             description,
             expiration_date,
             option_type,
             strike_price,
             close_transaction_id,
             lot_id,
             allocated_contracts,
             allocated_cost,
             proceeds,
             realized_gain
        )
        VALUES (
             lot_cursor.instrument,
             lot_cursor.description,
             lot_cursor.expiration_date,
             lot_cursor.option_type,
             lot_cursor.strike_price,
             (SELECT MAX(option_transaction_id)
                FROM options_transactions_enriched ote2
               WHERE ote2.normalized_instrument = lot_cursor.instrument
                 AND ote2.expiration_date = lot_cursor.expiration_date
                 AND ote2.option_type = lot_cursor.option_type
                 AND ote2.strike_price = lot_cursor.strike_price
                 AND ote2.raw_trans_code = 'OEXP'),
             lot_cursor.lot_id,
             consumed_qty,
             total_cost_portion,
             proceeds_calc,
             (proceeds_calc - total_cost_portion)
        );
        
        UPDATE fifo_option_lots
            SET open_contracts = 0,
                total_cost = 0,
                updated_at = NOW()
            WHERE lot_id = lot_cursor.lot_id;
    END LOOP;
    
    RAISE NOTICE 'Option FIFO processing complete.';
END;
$$;