CREATE OR REPLACE PROCEDURE process_equity_fifo()
LANGUAGE plpgsql
AS $$
DECLARE
    rec  RECORD;
    -- Temporary variables
    shares_to_sell  NUMERIC;
    lot_cursor      RECORD;
    consumed_qty    NUMERIC;
    ratio           NUMERIC;
    new_open_qty    NUMERIC;
BEGIN

    RAISE NOTICE 'Starting FIFO processing...';

    -- 1) Clear existing data (for a full rebuild approach)
    TRUNCATE TABLE fifo_equity_lots RESTART IDENTITY;
    TRUNCATE TABLE equity_realized_gains RESTART IDENTITY;

    -- 2) Process transactions in chronological order
    FOR rec IN
        SELECT *
        FROM equity_transactions
        ORDER BY instrument, corrected_activity_date, transaction_id
    LOOP

        -- Treat both Buy and REC as opening a new lot
        IF rec.raw_trans_code IN ('Buy', 'REC') THEN

            /*
             * Insert a new lot for these shares
             * If 'REC' shares have a different basis (e.g. 0),
             * adjust the calculation below as needed.
             */
            INSERT INTO fifo_equity_lots (
                instrument,
                buy_transaction_id,
                lot_open_date,
                open_quantity,
                total_cost,
                avg_price
            )
            VALUES (
                rec.instrument,
                rec.transaction_id,
                rec.corrected_activity_date::date,
                rec.quantity,
                (rec.price * rec.quantity),  -- total cost
                rec.price
            );

        ELSIF rec.raw_trans_code = 'Sell' THEN
            /*
             * We need to sell FIFO: find earliest lots for this instrument
             * and consume shares until rec.quantity is allocated
             */
            shares_to_sell := rec.quantity;  -- the quantity we want to sell

            -- Cursor over the earliest open lots for this instrument
            FOR lot_cursor IN
                SELECT *
                FROM fifo_equity_lots
                WHERE instrument = rec.instrument
                  AND open_quantity > 0
                ORDER BY lot_id  -- earliest lot first
            LOOP
                EXIT WHEN shares_to_sell <= 0;

                IF lot_cursor.open_quantity <= shares_to_sell THEN
                    -- We'll consume the entire lot_cursor
                    consumed_qty := lot_cursor.open_quantity;
                ELSE
                    -- We'll only consume part of this lot
                    consumed_qty := shares_to_sell;
                END IF;

                -- Calculate allocated cost for these shares
                INSERT INTO equity_realized_gains (
                    instrument,
                    sell_transaction_id,
                    lot_id,
                    allocated_quantity,
                    allocated_cost,
                    proceeds,
                    realized_gain
                )
                VALUES (
                    lot_cursor.instrument,
                    rec.transaction_id,
                    lot_cursor.lot_id,
                    consumed_qty,
                    (lot_cursor.avg_price * consumed_qty),
                    (rec.price * consumed_qty),
                    (rec.price * consumed_qty)
                    - (lot_cursor.avg_price * consumed_qty)
                );

                -- Update the lot's leftover quantity and cost
                new_open_qty := lot_cursor.open_quantity - consumed_qty;
                UPDATE fifo_equity_lots
                  SET open_quantity = new_open_qty,
                      total_cost    = lot_cursor.avg_price * new_open_qty,
                      updated_at    = now()
                  WHERE lot_id = lot_cursor.lot_id;

                -- Decrement shares_to_sell
                shares_to_sell := shares_to_sell - consumed_qty;
            END LOOP;

            IF shares_to_sell > 0 THEN
                RAISE WARNING 'Trying to sell more shares than we have for instrument %, leftover = %',
                              rec.instrument, shares_to_sell;
                -- In a robust system, you might throw an exception or handle negative positions
            END IF;

        ELSIF rec.raw_trans_code = 'SPL' THEN
            /*
             * A stock split transaction:
             * e.g. if quantity=3, that means a 4-for-1 split (1 existing share becomes 4 total).
             * So the ratio = (1 + quantity) or maybe parse the ratio from the data feed.
             * 
             * Example: "quantity=3" => 4:1 split => ratio=4.
             */
            ratio := (rec.quantity + 1.0);

            UPDATE fifo_equity_lots
              SET open_quantity = open_quantity * ratio,
                  avg_price     = avg_price / ratio,
                  total_cost    = total_cost -- effectively the same total cost
            WHERE instrument = rec.instrument;

        ELSE
            /*
             * If we see codes besides Buy, REC, Sell, SPL in equity_transactions,
             * we log a notice. But typically, the equity_transactions view should
             * only have these four codes now.
             */
            RAISE NOTICE 'Encountered unhandled trans_code: % for instrument % on %',
                         rec.raw_trans_code, rec.instrument, rec.corrected_activity_date;
        END IF;

    END LOOP;

    RAISE NOTICE 'FIFO processing complete.';
END;
$$;