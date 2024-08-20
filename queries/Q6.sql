/*
Find transactions whose value involved least 75% of the sender's current Ethereum balance
(not necessarily the balance at the time of the transaction),
and the priority gas fee was higher than the average that others were willing to pay within the same block.
Show the transaction hash, from and to addresses, value transacted, and the priority gas fee.
Perhaps the sender was really in a hurry?
*/
SELECT tx.hash, tx.from_address, tx.to_address, tx.value, tx.max_priority_fee_per_gas
FROM Transactions tx, Addresses a
WHERE tx.from_address = a.address
AND tx.value >= a.eth_balance * 0.75
AND tx.max_priority_fee_per_gas >=
    (SELECT AVG(max_priority_fee_per_gas)
	 FROM Transactions
	 WHERE block_hash = tx.block_hash);
