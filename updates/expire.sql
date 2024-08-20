/*
Expire old data in the current database slice so remaining blocks start from number :BLK_START (set by the caller).
We proceed in reverse of the table declaration order.
*/

-- Precompute the set of transactions to delete to simplify things:
CREATE UNLOGGED TABLE deleted_transactions AS
SELECT tx.hash
FROM Transactions tx, Blocks b
WHERE tx.block_hash = b.hash
AND b.number < :BLK_START;

DELETE FROM Token_Transactions
WHERE transaction_hash IN (SELECT hash FROM deleted_transactions);
-- No need to track deleted token_address values, as addresses don't expire.

DELETE FROM Tokens
WHERE FALSE;
-- Tokens don't expire.

DELETE FROM Transactions
WHERE hash IN (SELECT hash FROM deleted_transactions);
-- No need to track deleted from_address, to_address, and receipt_contract_address, as addresses don't expire.

DELETE FROM Contracts
WHERE FALSE;
-- Contracts don't expire.

DELETE FROM Withdrawals
WHERE hash IN (SELECT hash FROM Blocks WHERE number < :BLK_START);
-- No need to track deleted validator addresses, as addresses don't expire.

DELETE FROM Blocks
WHERE number < :BLK_START;
-- No need to track deleted miner addresses, as addresses don't expire.

DELETE FROM Addresses
WHERE FALSE;
-- Addresses don't expire.

DROP TABLE deleted_transactions;
