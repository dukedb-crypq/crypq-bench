/*
Partition the range of all transaction values into quartiles,
and partition the range of current balances of all addresses into quartiles.
For each combination of (transaction value partition, from address balance partition, to address balance partition),
show the total number of transactions with this particular combination.

Note: In case there are duplicates when partitioning, adjust boundaries and/or merge partitions as needed.
Each partition should be identified by
its lower (inclusive) and upper (exclusive except for the last partition) percentile bounds.
*/
WITH value_starts(v) AS (
    (SELECT CAST(-1 AS NUMERIC)) UNION ALL
    (SELECT UNNEST(PERCENTILE_DISC(ARRAY(SELECT GENERATE_SERIES(0.25, 1.0, 0.25)))
                   WITHIN GROUP (ORDER BY value))
     FROM transactions)
),
value_bounds(l_val, u_val) AS ( -- (l_val, u_val]
    SELECT DISTINCT l.v, u.v
    FROM value_starts l, value_starts u
    WHERE l.v < u.v AND NOT EXISTS(SELECT * FROM value_starts WHERE l.v < v AND v < u.v)
),
value_partitions(l_pct, u_pct, l_val, u_val, count) AS ( -- [l_pct, u_pct) or ] for last partition, [l_val, u_val]
    SELECT 100 - ROUND((SELECT COUNT(*) FROM transactions WHERE value > l_val) * 100
                       / (SELECT COUNT(*) FROM transactions)),
           ROUND((SELECT COUNT(*) FROM transactions WHERE value <= u_val) * 100
                 / (SELECT COUNT(*) FROM transactions)),
           MIN(value), MAX(value), COUNT(*)
    FROM transactions, value_bounds
    WHERE l_val < value AND value <= u_val
    GROUP BY l_val, u_val
),
balance_starts(v) AS (
    (SELECT CAST(-1 AS NUMERIC)) UNION ALL
    (SELECT UNNEST(PERCENTILE_DISC(ARRAY(SELECT GENERATE_SERIES(0.25, 1.0, 0.25)))
                  WITHIN GROUP (ORDER BY eth_balance))
     FROM addresses)
),
balance_bounds(l_val, u_val) AS ( -- (l_val, u_val]
    SELECT DISTINCT l.v, u.v
    FROM balance_starts l, balance_starts u
    WHERE l.v < u.v AND NOT EXISTS(SELECT * FROM balance_starts WHERE l.v < v AND v < u.v)
),
balance_partitions(l_pct, u_pct, l_val, u_val, count) AS ( -- [l_pct, u_pct) or ] for last partition, [l_val, u_val]
    SELECT 100 - ROUND((SELECT COUNT(*) FROM addresses WHERE eth_balance > l_val) * 100
                       / (SELECT COUNT(*) FROM addresses)),
           ROUND((SELECT COUNT(*) FROM addresses WHERE eth_balance <= u_val) * 100
                 / (SELECT COUNT(*) FROM addresses)),
           MIN(eth_balance), MAX(eth_balance), COUNT(*)
    FROM addresses, balance_bounds
    WHERE l_val < eth_balance AND eth_balance <= u_val
    GROUP BY l_val, u_val
)
SELECT v_p.l_pct AS "[value%", v_p.u_pct AS "value%]",
       from_p.l_pct AS "[frombal%", from_p.u_pct AS "frombal%]",
       to_p.l_pct AS "[tobal%", to_p.u_pct AS "tobal%]",
       (SELECT COUNT(*)
        FROM Transactions tx, Addresses from_a, Addresses to_a
        WHERE tx.from_address = from_a.address AND tx.to_address = to_a.address
        AND (tx.value BETWEEN v_p.l_val AND v_p.u_val)
        AND (from_a.eth_balance BETWEEN from_p.l_val AND from_p.u_val)
        AND (to_a.eth_balance BETWEEN to_p.l_val AND to_p.u_val))
FROM value_partitions v_p, balance_partitions from_p, balance_partitions to_p
ORDER BY 1, 3, 5;