WITH checks AS (
    SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT order_no) AS unique_orders,
        MIN(order_no) AS first_order,
        MAX(order_no) AS last_order
    FROM {{ ref('stg_fragments') }}
)

SELECT *
FROM checks
WHERE total != 21
   OR unique_orders != 21
   OR first_order != 1
   OR last_order != 21