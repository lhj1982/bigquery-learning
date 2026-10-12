Query

```
SELECT
    event_date,
    COUNT(*) AS orders,
    ROUND(SUM(amount), 2) AS revenue
FROM `bigquery-sandbox-496312.dataflow_lab.orders`
WHERE event_date BETWEEN '2026-10-01' AND '2026-10-03'
  AND status = 'PAID'
GROUP BY event_date;
```