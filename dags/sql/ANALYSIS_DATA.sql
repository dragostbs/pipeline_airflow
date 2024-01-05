WITH cte AS (
    SELECT
        trade_name,
        COUNT(*) AS total_trades,
        COUNT(CASE WHEN trade_type = 'Win' THEN 1 END) AS win_trades,
        COUNT(CASE WHEN trade_type = 'Loss' THEN 1 END) AS loss_trades,
        ROUND(SUM(CASE WHEN trade_result >= 0 THEN trade_result ELSE 0 END)) AS profit,
        ROUND(SUM(CASE WHEN trade_result <= 0 THEN trade_result ELSE 0 END)) AS loss
    FROM
        trades
    GROUP BY trade_name
)
SELECT
    trade_name,
    profit || ' $' AS profit,
    loss || ' $' AS Loss,
    ROUND(SUM(profit + loss)) || ' $' AS profit_loss,
    CASE
	    WHEN profit = 0 THEN 'N/A'
	    ELSE ROUND(((profit + loss) / profit) * 100) || ' %'
    END AS change_profit_loss,
    win_trades,
    loss_trades,
    total_trades
FROM
    cte
GROUP BY trade_name, profit, loss, win_trades, loss_trades, total_trades;