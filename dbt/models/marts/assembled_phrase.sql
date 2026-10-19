SELECT
    STRING_AGG(word, ' ' ORDER BY order_no) AS phrase
FROM {{ ref('stg_fragments') }}