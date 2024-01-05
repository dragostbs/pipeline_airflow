INSERT INTO trades(trade_id, trade_date, trade_name, trade_type, trade_result)
VALUES (UNNEST(%(trade_id)s), UNNEST(%(trade_date)s), UNNEST(%(trade_name)s), UNNEST(%(trade_type)s), UNNEST(%(trade_result)s))
ON CONFLICT (trade_id) DO UPDATE
SET
    trade_date = EXCLUDED.trade_date,
    trade_name = EXCLUDED.trade_name,
    trade_type = EXCLUDED.trade_type,
    trade_result = EXCLUDED.trade_result;