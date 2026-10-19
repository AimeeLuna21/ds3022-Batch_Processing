WITH numbered AS (
    SELECT
        TRY_CAST(order_no AS INTEGER) AS order_no,
        word,
        received_at
    FROM {{ source('raw', 'fragments') }}
),

deduplicated AS (
    SELECT
        order_no,
        word
    FROM numbered
    WHERE order_no IS NOT NULL
      AND word IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_no
        ORDER BY received_at
    ) = 1
)

SELECT
    order_no,
    word
FROM deduplicated