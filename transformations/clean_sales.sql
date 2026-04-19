-- Students can wire this into the execution engine.
CREATE OR REPLACE TABLE clean_sales_orders AS
SELECT
    order_id,
    customer_id,
    amount,
    created_at,
    status,
    CURRENT_TIMESTAMP AS processed_at
FROM raw_sales_orders
WHERE status = 'completed'
  AND amount > 0;
