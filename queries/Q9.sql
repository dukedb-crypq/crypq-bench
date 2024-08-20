/*
Find function signature hashes of smart contracts that are no longer supported by later contract versions.
More precisely, for each such hash, output the contract address, current version, the hash,
and the first later version where this hash is no longer included in function_sighashes.
Ignore cases where the later version has a NULL function_sighashes.
*/
WITH contract_sigs(address, version, sighash) AS (
    SELECT c.address, c.version, t.sighash
    FROM Contracts c,
    LATERAL (SELECT SUBSTRING(c.function_sighashes FROM i*4+1 FOR 4) AS sighash
             FROM GENERATE_SERIES(0, LENGTH(c.function_sighashes)/4-1) AS s(i)) t
)
SELECT cs1.address, cs1.version, cs1.sighash, MIN(c2.version)
FROM contract_sigs cs1, Contracts c2
WHERE cs1.address = c2.address
AND cs1.version < c2.version
AND c2.function_sighashes IS NOT NULL
AND cs1.sighash NOT IN
    (SELECT sighash
     FROM contract_sigs
     WHERE address = c2.address
     AND version = c2.version)
GROUP BY cs1.address, cs1.version, cs1.sighash;
