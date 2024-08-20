/*
Truncates the current database slice to right before the block numbered :BLK_START (set by the caller).
We proceed in reverse of the table declaration order.
*/

-- Precompute the set of transactions to delete to simplify things and record some useful info:
CREATE UNLOGGED TABLE deleted_transactions AS
SELECT tx.hash, tx.receipt_contract_address
FROM Transactions tx, Blocks b
WHERE tx.block_hash = b.hash
AND b.number >= :BLK_START;

-- Pre-apply balance adjustments by "walking back" deleted transactions:
WITH nets(address, net) AS (
    SELECT address, SUM(delta)
    FROM crypq_balance_deltas(:BLK_START,
                              (SELECT MAX(number)-:BLK_START+1 FROM Blocks)) deltas
    GROUP BY address
)
UPDATE Addresses
SET eth_balance = eth_balance - nets.net
FROM nets
WHERE Addresses.address = nets.address;

DELETE FROM Token_Transactions
WHERE transaction_hash IN (SELECT hash FROM deleted_transactions);

DELETE FROM Tokens
WHERE block_hash IN (SELECT hash FROM Blocks WHERE number >= :BLK_START);

DELETE FROM Transactions
WHERE hash IN (SELECT hash FROM deleted_transactions);

DELETE FROM Contracts
WHERE address IN (SELECT receipt_contract_address FROM deleted_transactions)
OR block_hash IN (SELECT hash FROM Blocks WHERE number >= :BLK_START);

DELETE FROM Withdrawals
WHERE hash IN (SELECT hash FROM Blocks WHERE number >= :BLK_START);

DELETE FROM Blocks WHERE number >= :BLK_START;

-- Temproarily drop constraints to improve DELETE FROM Addresses performance:
ALTER TABLE Transactions DROP CONSTRAINT transactions_from_address_fkey;
ALTER TABLE Transactions DROP CONSTRAINT transactions_receipt_contract_address_fkey;
ALTER TABLE Transactions DROP CONSTRAINT transactions_to_address_fkey;
-- Materialize and index needed addresses to improve DELETE FROM Addresses performance:
CREATE UNLOGGED TABLE needed_addresses AS
SELECT address FROM crypq_addresses_referenced_before(:BLK_START);
CREATE INDEX ON needed_addresses(address);
-- Now do it:
DELETE FROM Addresses
WHERE address NOT IN (SELECT address FROM needed_addresses);
-- Clean up / restore:
DROP TABLE needed_addresses;
ALTER TABLE Transactions ADD CONSTRAINT transactions_from_address_fkey
    FOREIGN KEY (from_address) REFERENCES addresses(address);
ALTER TABLE Transactions ADD CONSTRAINT transactions_receipt_contract_address_fkey
    FOREIGN KEY (receipt_contract_address) REFERENCES addresses(address);
ALTER TABLE Transactions ADD CONSTRAINT transactions_to_address_fkey
    FOREIGN KEY (to_address) REFERENCES addresses(address);

DROP TABLE deleted_transactions;
