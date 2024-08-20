/*
Prepare INSERT and UPDATE statements for adding new data pertaining to :NUM_BLKS (>=1) blocks
starting with the block numbered :BLK_START (both parameters set by the caller).
The current database must already contain the above window of blocks.
The script will not modify any tables, but will create unlogged tables whose names start with "new_" or "update_"
which can then be dumped/post-processed to generate INSERT/UPDATE statements.
We proceed in the table declaration order.
*/

-- Apply balance adjustments by "walking back" transactions after the window:
DROP TABLE IF EXISTS new_Addresses;
CREATE UNLOGGED TABLE new_Addresses AS
WITH needed_addresses(address) AS (
    SELECT * FROM crypq_addresses_referenced_before(:BLK_START+:NUM_BLKS)
),
nets(address, net) AS (
    SELECT needed_addresses.address, COALESCE(SUM(delta), 0)
    FROM needed_addresses LEFT OUTER JOIN
         crypq_balance_deltas(:BLK_START+:NUM_BLKS, (SELECT MAX(number) /* safe upper bound */ FROM Blocks)) deltas
         ON needed_addresses.address = deltas.address
    GROUP BY needed_addresses.address
)
SELECT nets.address, eth_balance - nets.net AS eth_balance, crypq_adjusted
FROM Addresses, nets
WHERE Addresses.address = nets.address;
-- Some of these addresses are not new, and therefore need to be updated:
DROP TABLE IF EXISTS update_Addresses;
CREATE UNLOGGED TABLE update_Addresses AS
WITH prev_needed_addresses(address) AS (
    SELECT * FROM crypq_addresses_referenced_before(:BLK_START)
)
SELECT new_Addresses.address, eth_balance
FROM new_Addresses, prev_needed_addresses
WHERE new_Addresses.address = prev_needed_addresses.address;
-- ... instead of inserted:
DELETE FROM new_Addresses
USING update_Addresses
WHERE new_Addresses.address = update_Addresses.address;
-- Finally, there is no need to update an address whose balance hasn't changed:
WITH unchanged(address) AS (
    (SELECT address FROM update_Addresses) EXCEPT
    (SELECT DISTINCT address FROM crypq_balance_deltas(:BLK_START, :NUM_BLKS))
)
DELETE FROM update_Addresses
USING unchanged
WHERE update_Addresses.address = unchanged.address;

DROP TABLE IF EXISTS new_Blocks;
CREATE UNLOGGED TABLE new_Blocks AS
SELECT *
FROM Blocks
WHERE :BLK_START <= number AND number < :BLK_START+:NUM_BLKS;

DROP TABLE IF EXISTS new_Withdrawals;
CREATE UNLOGGED TABLE new_Withdrawals AS
SELECT w.*
FROM Withdrawals w, new_Blocks b
WHERE w.hash = b.hash;

DROP TABLE IF EXISTS new_Contracts;
CREATE UNLOGGED TABLE new_Contracts AS
SELECT c.*
FROM Contracts c, new_Blocks b
WHERE c.block_hash = b.hash;

DROP TABLE IF EXISTS new_Transactions;
CREATE UNLOGGED TABLE new_Transactions AS
SELECT tx.*
FROM Transactions tx, new_Blocks b
WHERE tx.block_hash = b.hash;

DROP TABLE IF EXISTS new_Tokens;
CREATE UNLOGGED TABLE new_Tokens AS
SELECT t.*
FROM Tokens t, new_Blocks b
WHERE t.block_hash = b.hash;

DROP TABLE IF EXISTS new_Token_Transactions;
CREATE UNLOGGED TABLE new_Token_Transactions AS
SELECT tk_tx.*
FROM Token_Transactions tk_tx, new_Transactions tx
WHERE tk_tx.transaction_hash = tx.hash;
