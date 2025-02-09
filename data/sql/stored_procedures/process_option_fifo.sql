CREATE OR REPLACE PROCEDURE process_option_fifo()
LANGUAGE plpgsql
AS $$
DECLARE
    rec              RECORD;
    contracts_to_close NUMERIC;
    lot_cursor       RECORD;
    consumed_qty     NUMERIC;
    new_open_qty     NUMERIC;
    total_cost_portion NUMERIC;
    proceeds_calc    NUMERIC;
BEGIN
    RAISE NOTICE 'Starting Option FIFO processing...';

    -- 1) Clear existing data (for a full rebuild approach)
    TRUNCATE TABLE option_realized_gains, fifo_option_lots RESTART IDENTITY CASCADE;

    -- 2) Process in chronological order
    FOR rec IN
        SELECT *
        FROM option_transactions
        ORDER BY corrected_activity_date, transaction_id
    LOOP

        IF rec.raw_trans_code = 'BTO' THEN
            ------------------------------------------------------------------------------
            -- Opening a LONG position: rec.amount is typically negative => we paid premium
            ------------------------------------------------------------------------------
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
                rec.instrument,
                rec.description,
                rec.expiration_date,
                rec.option_type,
                rec.strike_price,
                
                rec.quantity,  -- positive for a long
                
                /* rec.amount is negative for a cost outflow,
                   so total_cost is the positive "cost basis"  */
                (-rec.amount),
                
                /* average cost = total_cost / open_contracts */
                ((-rec.amount) / rec.quantity),
                
                rec.corrected_activity_date::date,
                rec.transaction_id
            );

        ELSIF rec.raw_trans_code = 'STO' THEN
            ------------------------------------------------------------------------------
            -- Opening a SHORT position: rec.amount is typically positive => we received premium
            ------------------------------------------------------------------------------
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
                rec.instrument,
                rec.description,
                rec.expiration_date,
                rec.option_type,
                rec.strike_price,
                
                /* negative open_contracts to indicate a short position */
                -rec.quantity,
                
                /* rec.amount is positive for a credit => store negative cost basis if you prefer */
                -rec.amount,
                
                ((-rec.amount) / (-rec.quantity)),
                
                rec.corrected_activity_date::date,
                rec.transaction_id
            );

        ELSIF rec.raw_trans_code = 'STC' THEN
            ------------------------------------------------------------------------------
            -- Closing a LONG position
            ------------------------------------------------------------------------------
            contracts_to_close := rec.quantity;

            FOR lot_cursor IN
                SELECT *
                FROM fifo_option_lots
                WHERE instrument = rec.instrument
                  AND expiration_date = rec.expiration_date
                  AND option_type = rec.option_type
                  AND strike_price = rec.strike_price
                  AND open_contracts > 0   -- only lots with positive (long) quantity
                ORDER BY lot_id
            LOOP
                EXIT WHEN contracts_to_close <= 0;

                IF lot_cursor.open_contracts <= contracts_to_close THEN
                    consumed_qty := lot_cursor.open_contracts;
                ELSE
                    consumed_qty := contracts_to_close;
                END IF;

                -- portion of cost basis allocated
                total_cost_portion := lot_cursor.avg_cost * consumed_qty;

                -- proceeds from this close
                proceeds_calc := (rec.amount / rec.quantity) * consumed_qty;

                -- realized gain = proceeds - cost portion
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
                    rec.transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    total_cost_portion,
                    proceeds_calc,
                    (proceeds_calc - total_cost_portion)
                );

                -- update leftover open contracts & cost
                new_open_qty := lot_cursor.open_contracts - consumed_qty;
                
                UPDATE fifo_option_lots
                  SET open_contracts = new_open_qty,
                      total_cost    = (lot_cursor.avg_cost * new_open_qty),
                      updated_at    = now()
                  WHERE lot_id = lot_cursor.lot_id;

                contracts_to_close := contracts_to_close - consumed_qty;
            END LOOP;

            IF contracts_to_close > 0 THEN
                /*
                 * More descriptive warning message:
                 * Include leftover count, instrument, description, expiration_date,
                 * strike, transaction_id to identify the mismatch more easily.
                 */
                RAISE WARNING 
                  'STC leftover=% for instrument=% | desc=% | exp=% | strike=% | trans_id=%',
                  contracts_to_close,
                  rec.instrument,
                  rec.description,
                  rec.expiration_date,
                  rec.strike_price,
                  rec.transaction_id;
            END IF;

        ELSIF rec.raw_trans_code = 'BTC' THEN
            ------------------------------------------------------------------------------
            -- Closing a SHORT position
            ------------------------------------------------------------------------------
            contracts_to_close := rec.quantity;

            FOR lot_cursor IN
                SELECT *
                FROM fifo_option_lots
                WHERE instrument = rec.instrument
                  AND expiration_date = rec.expiration_date
                  AND option_type = rec.option_type
                  AND strike_price = rec.strike_price
                  AND open_contracts < 0  -- only lots with negative (short) quantity
                ORDER BY lot_id
            LOOP
                EXIT WHEN contracts_to_close <= 0;

                IF ABS(lot_cursor.open_contracts) <= contracts_to_close THEN
                    consumed_qty := ABS(lot_cursor.open_contracts);
                ELSE
                    consumed_qty := contracts_to_close;
                END IF;

                total_cost_portion := lot_cursor.avg_cost * consumed_qty;

                -- rec.amount is likely negative if we pay to close
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
                    rec.transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    total_cost_portion,
                    proceeds_calc,
                    proceeds_calc - total_cost_portion
                );

                -- update leftover open contracts
                new_open_qty := lot_cursor.open_contracts + consumed_qty; 
                UPDATE fifo_option_lots
                  SET open_contracts = new_open_qty,
                      total_cost    = (lot_cursor.avg_cost * new_open_qty),
                      updated_at    = now()
                  WHERE lot_id = lot_cursor.lot_id;

                contracts_to_close := contracts_to_close - consumed_qty;
            END LOOP;

            IF contracts_to_close > 0 THEN
                RAISE WARNING 
                  'BTC leftover=% for instrument=% | desc=% | exp=% | strike=% | trans_id=%',
                  contracts_to_close,
                  rec.instrument,
                  rec.description,
                  rec.expiration_date,
                  rec.strike_price,
                  rec.transaction_id;
            END IF;

        ELSIF rec.raw_trans_code = 'OEXP' THEN
            ------------------------------------------------------------------------------
            -- Expiration: worthless or assigned. 
            -- For simplicity, assume worthless => realize leftover cost/credit
            ------------------------------------------------------------------------------
            FOR lot_cursor IN
                SELECT *
                FROM fifo_option_lots
                WHERE instrument = rec.instrument
                  AND expiration_date = rec.expiration_date
                  AND option_type = rec.option_type
                  AND strike_price = rec.strike_price
                  AND open_contracts != 0
            LOOP
                consumed_qty := ABS(lot_cursor.open_contracts);

                -- Realize the leftover cost or credit:
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
                    rec.transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    total_cost_portion,
                    proceeds_calc,
                    proceeds_calc - total_cost_portion
                );

                -- set the lot to zero
                UPDATE fifo_option_lots
                  SET open_contracts = 0,
                      total_cost    = 0,
                      updated_at    = now()
                  WHERE lot_id = lot_cursor.lot_id;
            END LOOP;

        ELSE
            RAISE NOTICE 'Unhandled code: % for instrument % expiration %', 
                         rec.raw_trans_code, rec.instrument, rec.expiration_date;
        END IF;

    END LOOP;

    RAISE NOTICE 'Option FIFO processing complete.';
END;
$$;