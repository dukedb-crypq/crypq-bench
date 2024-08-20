/*
Note:
- The benchmark database, constrained by its size, may only contain a "slice" of the blockchain,
  which consists of a subsequence of continugous blocks.
- A "Wei" is the smallest part unit of Ether: 10^18 Wei = 1 Ether.
- A "Gwei" is the typical unit used to express fees: 10^9 Wei = 1 GWei.
- Instead of the SQL standard BIT(n) or BIT VARYING, we use BYTEA (PostgreSQL), which is better supported.
*/

CREATE TABLE Addresses( -- each row represents a user or a contract
	address BYTEA /* BIT(160) */ PRIMARY KEY, -- 20-byte identifier derived from the account's public key
	eth_balance NUMERIC NOT NULL, -- current balance of the address in Wei
        -- (extraplated; may not be accurate for a slice)
    crypq_adjusted BOOLEAN NOT NULL -- if TRUE, eth_balance has been adjusted to avoid underflow
        -- when replaying transactions within the slice
);

CREATE TABLE Blocks( -- each row corresponds to a block in the blockchain
    hash BYTEA /* BIT(256) */ PRIMARY KEY, -- 32-byte hash that uniquely identifies a block on the chain
    number NUMERIC UNIQUE NOT NULL CHECK (number >= 0), -- sequential number of this block among all existing blocks
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- timestamp when this block was added to the chain
    extra_data BYTEA /* BIT(256) */ NOT NULL, -- extra 32-byte field included in the block
    base_fee_per_gas NUMERIC NOT NULL CHECK (base_fee_per_gas >= 0), -- base fee in Gwei
        -- that all transactions in this block had to pay;
        -- it may be adjusted up/down based on the number of transactions in the previous block
    size INT NOT NULL, -- size of this block in bytes
    miner BYTEA /* BIT(160) */ NOT NULL REFERENCES Addresses(address) -- address of the user who successfully validated this block
);

CREATE TABLE Withdrawals( -- each row corresponds to a staking withdrawal;
        -- a validator is a user who has lock up the required amount of Ether to participate in staking;
        -- note that this table has no data prior to September 2022, as staking did not exist yet
    hash BYTEA /* BIT(256) */ REFERENCES Blocks(hash), -- hash of the block in which the withdrawal occurred
    withdrawal_index NUMERIC PRIMARY KEY CHECK (withdrawal_index >= 0), -- index of this withdrawal in the log for this block
    validator_index NUMERIC NOT NULL CHECK (validator_index >= 0), -- an index assigned to the participating validator
    address BYTEA /* BIT(160) */ REFERENCES Addresses(address), --- address of the validator
    amount NUMERIC NOT NULL CHECK (amount >= 0) -- amount withdrawn in Gwei
);

CREATE TABLE Contracts( -- each row correponds to a smart contract, or more precisely, a particular version thereof
    address BYTEA /* BIT(160) */ NOT NULL REFERENCES Addresses(address), -- identifier of the contract
    version INT NOT NULL CHECK (version >= 0), -- ordinal version number
    function_sighashes BYTEA /* BIT VARYING */, -- concatenation of hashes of the function signatures provided by the contract;
       --- each hash is exactly 4 bytes or 32 bits
    bytecode BYTEA /* BIT VARYING */, -- bytecode of the contract
    is_erc20 BOOLEAN NOT NULL, -- whether this contract is of type ERC20
    is_erc721 BOOLEAN NOT NULL, -- whether this contract is of type ERC721
    block_hash BYTEA /* BIT(256) */, -- hash of the block in which this (version of the) contract was created;
        -- would REFERENCES Blocks(hash), except when that block isn't included in the slice;
        -- note that the contract address may be in the system previously, e.g., because of multiple versions
    crypq_block_outside_slice BOOLEAN NOT NULL, -- TRUE iff block_hash is outside the slice
    PRIMARY KEY (address, version)
);

CREATE TABLE Transactions( -- each row corresponds to a transaction recorded in a block
    hash BYTEA /* BIT(256) */ PRIMARY KEY, -- 32-byte hash that uniquely identifies a transaction on the blockchain
    transaction_index INT NOT NULL, -- sequential number of this transaction among all transactions in the same block
    value NUMERIC NOT NULL, -- total value transferred in Wei in this transaction
    from_address BYTEA /* BIT(160) */ NOT NULL REFERENCES Addresses(address), -- sender address
    to_address BYTEA /* BIT(160) */ REFERENCES Addresses(address), -- receiver address,
        -- or NULL if this transaction creates a contract
    gas NUMERIC NOT NULL CHECK (gas >= 0), -- gas in Gwei required for this transaction
    max_priority_fee_per_gas NUMERIC CHECK (max_priority_fee_per_gas >= 0), -- amount in Gwei that the sender
        -- may wish to pay on top of base_fee_per_gas for the block to
        -- incentivize validators to include their transaction in the given block, if any
    input BYTEA /* BIT VARYING */, -- data sent along with the transaction, if any
    receipt_contract_address BYTEA /* BIT(160) */ REFERENCES Addresses(address), -- address of the contract created
        -- if this transaction creates a contract, or NULL otherwise;
        -- should reference Contracts(address) but it is not a primary key there
    block_hash BYTEA /* BIT(256) */ NOT NULL REFERENCES Blocks(hash), -- hash of the block in which this transaction took place
    transaction_type INT NOT NULL CHECK (transaction_type BETWEEN 0 AND 127), -- one of the transaction types in:
        -- https://ethereum.org/en/developers/docs/transactions/
    nonce INT NOT NULL CHECK (nonce >= 0), -- number of transactions from the sender up to this point;
        -- used to prevent double spending
    UNIQUE (block_hash, transaction_index),
    UNIQUE (from_address, nonce)
);

CREATE TABLE Tokens( -- each row corresponds to the creation of a token supply by a smart contract
    address BYTEA /* BIT(160) */ NOT NULL PRIMARY KEY REFERENCES Addresses(address), -- contract responsible for this token supply;
       -- should reference Contracts(address) but it is not a primary key there
    symbol VARCHAR(60), -- symbol of the token type, if given
    name VARCHAR(600), -- name of the token type, if given
    decimals INT CHECK (decimals >= 0), -- number of decimal places;
        -- one token = 10^decimals units used by total_supply and Token_Trasnactions.value 
    total_supply NUMERIC CHECK (total_supply >= 0), -- total supply of your tokens, if any
        -- (per standard, the contract stops creating new tokens when the limit is reached)
    block_hash BYTEA /* BIT(256) */ NOT NULL, -- hash of the block in which this token supply was created;
        -- would REFERENCES Blocks(hash), except when that block isn't included in the slice
    crypq_block_outside_slice BOOLEAN -- TRUE iff block_hash is outside the slice
);

CREATE TABLE Token_Transactions( -- each row corresponds to one token transfer event within a transaction
    transaction_hash BYTEA /* BIT(256) */ NOT NULL REFERENCES Transactions(hash), -- hash of the transaction in which this event occurred
    log_index INT NOT NULL CHECK (log_index >= 0), -- index of this event within the logs of the block in which this event occurred;
        -- there can be multiple token transfers within the same transaction
    token_address BYTEA /* BIT(160) */, -- address of the contract responsible for tokens transacted,
        -- which either references Tokens(address) or is an authorized minting contract;
        -- should reference Contracts(address) but it is not a primary key there
    value NUMERIC NOT NULL CHECK (value >= 0), -- quantity of tokens transacted;
        -- divide by 10^Tokens.decimals to obain the (possibly fractional) number of tokens transacted
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE OR REPLACE FUNCTION crypq_addresses_referenced_before(BLK_START Blocks.number%TYPE)
-- Return the set of addresses that referenced by activities in blocks numbered less than BLK_START.
RETURNS TABLE(address Addresses.address%TYPE) AS $$
WITH blocks_before AS (
    SELECT * FROM Blocks WHERE number < $1
),
transactions_before AS (
    SELECT tx.*
    FROM Transactions tx, blocks_before b
    WHERE tx.block_hash = b.hash
)
(SELECT miner FROM blocks_before) UNION
(SELECT address FROM withdrawals w, blocks_before b WHERE w.hash = b.hash) UNION
(SELECT address FROM Contracts WHERE crypq_block_outside_slice) UNION
(SELECT c.address FROM Contracts c, blocks_before b WHERE c.block_hash = b.hash) UNION
(SELECT from_address FROM transactions_before) UNION
(SELECT to_address FROM transactions_before WHERE to_address IS NOT NULL) UNION
(SELECT receipt_contract_address FROM transactions_before WHERE receipt_contract_address IS NOT NULL) UNION
(SELECT address FROM Tokens WHERE crypq_block_outside_slice) UNION
(SELECT t.address FROM Tokens t, blocks_before b WHERE t.block_hash = b.hash) UNION
(SELECT tk_tx.token_address FROM Token_Transactions tk_tx, transactions_before tx WHERE tk_tx.transaction_hash = tx.hash);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION crypq_balance_deltas(BLK_START Blocks.number%TYPE, NUM_BLKS Blocks.number%TYPE)
-- Return all expenses/payments/incomes incurred by activities in blocks numbered [BLK_START, BLK_START+NUM_BLKS).
RETURNS TABLE(address Addresses.address%TYPE,
              delta Addresses.eth_balance%TYPE,
              block_number Blocks.number%TYPE,
              transaction_index Transactions.transaction_index%TYPE) AS $$
-- Note the use of NUMERIC to preserve precision:
(SELECT from_address, - gas * CAST(POWER(10, 9) AS NUMERIC) - value, Blocks.number, transaction_index
    FROM Transactions, Blocks
    WHERE block_hash = Blocks.hash
    AND $1 <= Blocks.number AND Blocks.number < $1+$2)
UNION ALL
(SELECT to_address, value, Blocks.number, transaction_index
    FROM Transactions, Blocks
    WHERE block_hash = Blocks.hash
    AND $1 <= Blocks.number AND Blocks.number < $1+$2)
$$ LANGUAGE sql;
