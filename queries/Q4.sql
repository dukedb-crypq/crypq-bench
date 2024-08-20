/*
Count the number of distinct token addresses involved in transactions within a range of blocks,
compard with the total number of token transactions therein.

Note: In the following, subqueries are used to set :MIN_BLOCK and :MAX_BLOCK,
but concrete literals can be used when running a specific database slice.
*/
SELECT COUNT(DISTINCT tk_tx.token_address), COUNT(*)
FROM Blocks b, Transactions tx, token_transactions tk_tx
WHERE b.hash = tx.block_hash
AND tx.hash = tk_tx.transaction_hash
AND b.number BETWEEN (SELECT MIN(number)+(MAX(number)-MIN(number))/4 FROM Blocks) /* :MIN_BLOCK */
                 AND (SELECT MAX(number)-(MAX(number)-MIN(number))/4 FROM Blocks) /* :MAX_BLOCK */;