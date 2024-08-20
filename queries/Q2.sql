/*
Find any single token transfer whose value is within
a prescribed range of percentages of the total token supply.
Order output rows by the percentage in descending order.
Besides information about the token transfer,
show also the token symbol, name, total supply, and the percentage transferred.
Note that each unit value is 1/10^Tokens.decimals of one taken.
If Tokens.decimals is NULL, default it to 18 (assuming ERC20).
Igore tokens with 0 total supply.
*/
WITH Temp AS (
    SELECT tk_tx.*, tk.symbol, tk.name, tk.total_supply,
        tk_tx.value * 100 / POWER(10, COALESCE(tk.decimals, 18)) / tk.total_supply AS percentage
    FROM Token_Transactions tk_tx, Tokens tk
    WHERE tk_tx.token_address = tk.address
    AND tk.total_supply <> 0
)
SELECT *
FROM Temp
WHERE percentage BETWEEN 0.01 /* :LOW_PCT */ AND 100 /* :HIGH_PCT */
ORDER BY percentage DESC;
