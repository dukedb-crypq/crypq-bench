/*
Find longest loops of different transacting addresses, where A_1 transacted with A_2,
A_2 then transacted with A_3, ..., A_{k-1} then transacted with A_k, and
finally A_k transacted with A_1, in temporal order.
Ignore transactions between an address and itself.
We are only interested in a loop if
each pair of transacting addresses therein has transacted in more than 5 times with each other
(considering both directions, within the database slice).
Output the list of k addresses representing each loop as an array, along with the number of instantiations,
where each instantiation is list of transactions matching the pattern.
If multiple loops tie for the maximum length, order them by descending number of instantiations.
*/
WITH RECURSIVE pairs(a1, a2) AS ( -- where a1 <= a2
    SELECT LEAST(from_address, to_address), GREATEST(from_address, to_address)
    FROM Transactions
    WHERE from_address <> to_address
    GROUP BY LEAST(from_address, to_address), GREATEST(from_address, to_address)
    HAVING COUNT(*) > 5
),
edges(u, v, ts) AS (
    SELECT from_address, to_address, ROW(b.number, tx.transaction_index)
    FROM Transactions tx, Blocks b
    WHERE tx.block_hash = b.hash
    AND (LEAST(from_address, to_address), GREATEST(from_address, to_address)) IN (SELECT * FROM pairs)
),
paths(nodes, last_ts, count) AS (
    (SELECT ARRAY[u, v], ts, CAST(1 AS BIGINT) FROM edges)
    UNION
    (SELECT nodes, last_ts, CAST(SUM(count) AS BIGINT)
     FROM (SELECT p.nodes || e.v AS nodes, e.ts AS last_ts, p.count AS count
           FROM paths p, edges e
           WHERE e.u = p.nodes[ARRAY_UPPER(p.nodes, 1)]
           AND NOT(e.v = ANY(p.nodes))
           AND p.last_ts < e.ts) t
     GROUP BY nodes, last_ts)
),
loops(nodes, count) AS (
    SELECT nodes, SUM(count)
    FROM paths p, edges e
    WHERE e.u = p.nodes[ARRAY_UPPER(p.nodes, 1)]
    AND e.v = p.nodes[1]
    AND p.last_ts < e.ts
    GROUP BY nodes
)
SELECT *
FROM loops
WHERE ARRAY_LENGTH(nodes, 1) = (SELECT MAX(ARRAY_LENGTH(nodes, 1)) FROM loops)
ORDER BY count DESC, nodes;

-- Useful for debugging/checking on blocks [19005000, 19006999]:

-- SELECT nodes, SUM(count) FROM paths GROUP BY nodes;

-- SELECT ARRAY[e1.u, e2.u, e3.u, e4.u, e4.v], COUNT(*)
-- FROM edges e1, edges e2, edges e3, edges e4
-- WHERE e1.v = e2.u AND e2.v = e3.u AND e3.v = e4.u
-- AND e1.ts < e2.ts AND e2.ts < e3.ts AND e3.ts < e4.ts
-- AND (e1.u, e2.u, e3.u, e4.u, e4.v) = ('\x2fc617e933a52713247ce25730f6695920b3befe','\x50426ee5e65206541e8e71ae7267c8541c0a7daf','\xe4570a94ddd551699714e72bd510f4c73796552a','\x28c6c06298d514db089934071355e5743bf21d60','\x514910771af9ca656af840dff83e8264ecf986ca')
-- GROUP BY e1.u, e2.u, e3.u, e4.u, e4.v; -- should be 18963

-- SELECT ARRAY[e1.u, e2.u, e3.u, e3.v], COUNT(*)
-- FROM edges e1, edges e2, edges e3
-- WHERE e1.v = e2.u AND e2.v = e3.u
-- AND e1.ts < e2.ts AND e2.ts < e3.ts
-- AND (e1.u, e2.u, e3.u, e3.v) = ('\x264bd8291fae1d75db2c5f573b07faa6715997b5','\x5215b37befc735c059b9ab419273a8db8b8d89f2','\xbd0fccdc19bc3b979e8e256b7b88aae7c77a5bec','\x2bbd6ad0ef9d522c862bc60081f92372eacbcc6f')
-- GROUP BY e1.u, e2.u, e3.u, e3.v; -- should be 149

-- SELECT ARRAY[e1.u, e2.u, e2.v], COUNT(*)
-- FROM edges e1, edges e2
-- WHERE e1.v = e2.u
-- AND e1.ts < e2.ts
-- AND (e1.u, e2.u, e2.v) = ('\x388cf31bdd90af5885846363d51bd820aaec8b70','\xb2f5ff286216841e433954a4e7ad98f68525550c','\x388cf31bdd90af5885846363d51bd820aaec8b70')
-- GROUP BY e1.u, e2.u, e2.v; -- should be 255
