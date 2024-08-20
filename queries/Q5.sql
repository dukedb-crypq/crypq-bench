/*
For the 5 most popular token addresses (ordered by the number of transactions they were involved in),
find the top 10 users who sent the most of each of these tokens.
*/
WITH Popular_Tokens(token_rank, token_address, token_symbol) AS (
	SELECT RANK() OVER w, tk.address, tk.symbol
	FROM Tokens tk, Transactions tx, Token_Transactions tk_tx
	WHERE tx.hash = tk_tx.transaction_hash
	AND tk_tx.token_address = tk.address
	GROUP BY tk.address, tk.symbol
	WINDOW w AS (ORDER BY COUNT(*) DESC)
)
SELECT *
FROM (
SELECT ptk.token_rank, ptk.token_address, ptk.token_symbol,
       RANK() OVER w AS originator_rank,
	   tx.from_address,
	   SUM(tk_tx.value) AS total_value
FROM Transactions tx, Token_Transactions tk_tx, Popular_Tokens ptk
WHERE tx.hash = tk_tx.transaction_hash
AND tk_tx.token_address = ptk.token_address
AND ptk.token_rank <= 5
GROUP BY ptk.token_rank, ptk.token_address, ptk.token_symbol, tx.from_address
WINDOW w AS (PARTITION BY ptk.token_address ORDER BY SUM(tk_tx.value) DESC)
) t
WHERE originator_rank <= 10
ORDER BY token_rank, token_address, originator_rank;