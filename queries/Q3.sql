/*
Compute the average duration, measured in the number of blocks passed,
between the creation of a contract and its first use.
*/
WITH Creation(address, version, number) AS (
	SELECT c.address, c.version, b.number
	FROM Contracts c, Blocks b
	WHERE c.block_hash = b.hash
),
First_Used(address, version, number) AS (
	SELECT c.address, c.version, MIN(b.number)
	FROM Contracts c, Blocks b, Transactions t
	WHERE (t.from_address = c.address OR t.to_address = c.address)
	AND t.block_hash = b.hash
	GROUP BY c.address, c.version
)
SELECT AVG(First_used.number - Creation.number)
FROM Creation, First_used
WHERE Creation.address = First_used.address
AND Creation.version = First_used.version;