/*
Count the number of large token transactions conducted by users with significant activities and balances.
Such transactions may be of interest to those analyzing the behavior of major stakeholders.

More specifically, the query returns the number of token transactions
(excluding those with :NEG_TK_PATTERN in the token name)
in which between :MIN_VAL and :MAX_VAL tokens (normalized by token decimals)
were sent to an ERC20 contract by a user account
who has transacted between :MIN_NONCE and :MAX_NONCE times in the past
and currently has a Ether balance over :MIN_BALANCE.

Note: The specific settings of :MIN_VAL and :MAX_VAL below assumes that tk.decimals is 18 (the ERC20 default);
in general, tk_tx.value of x means x/10^tk.decimals number (possibly fractional) of tokens.
*/
SELECT COUNT(*)
FROM Transactions tx, Tokens tk, Token_Transactions tk_tx, Contracts c, Addresses a
WHERE tx.hash = tk_tx.transaction_hash
AND tk_tx.token_address = tk.address
AND tx.to_address = c.address
AND tx.from_address = a.address
AND tx.nonce BETWEEN 2100000 /* :MIN_NONCE */ AND 4200000 /* :MAX_NONCE */
AND tk_tx.value BETWEEN 1000000000 /* :MIN_VAL */ AND 10000000000 /* :MAX_VAL */
AND tk.name NOT LIKE '%US%' /* :NEG_TK_PATTERN */
AND c.is_erc20 = TRUE
AND a.eth_balance >= 25000000000000000 /* :MIN_BALANCE */;
