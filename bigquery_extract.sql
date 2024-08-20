SET @@dataset_id = 'crypto_ethereum_slice_extract';

/*
A contiguous range of blocks defines the database slice.
By default we extract the latest 1000 blocks;
user may change the criterion as needed.
*/
CREATE OR REPLACE TABLE blocks AS
SELECT *
FROM `bigquery-public-data.crypto_ethereum.blocks` ORDER BY timestamp DESC
LIMIT 1000;

CREATE OR REPLACE TABLE withdrawals AS
SELECT
  b.hash,
  w.index AS withdrawal_index,
  w.validator_index AS validator_index,
  w.address AS address,
  w.amount AS amount
FROM
  blocks AS b,
  UNNEST(b.withdrawals) AS w;

CREATE OR REPLACE TABLE transactions AS
SELECT *
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_hash IN (SELECT blocks.hash FROM blocks);

CREATE OR REPLACE TABLE token_transactions AS
SELECT *
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE transaction_hash IN (SELECT transactions.hash FROM transactions);

CREATE OR REPLACE TABLE tokens AS
SELECT Tokens.*
FROM `bigquery-public-data.crypto_ethereum.tokens` AS Tokens
WHERE address IN (SELECT token_address FROM token_transactions)
   OR block_hash IN (SELECT blocks.hash FROM blocks);

CREATE OR REPLACE TABLE contracts AS
SELECT *
FROM `bigquery-public-data.crypto_ethereum.contracts`
WHERE address IN (SELECT from_address FROM transactions)
   OR address IN (SELECT to_address FROM transactions)
   OR address IN (SELECT receipt_contract_address FROM transactions)
   OR address IN (SELECT address FROM tokens)
   OR address IN (SELECT token_address FROM token_transactions)
   OR block_hash IN (SELECT blocks.hash FROM blocks);

CREATE OR REPLACE TABLE addresses AS
WITH needed_addresses AS (
    (SELECT from_address AS address FROM transactions) UNION DISTINCT
    (SELECT to_address AS address FROM transactions) UNION DISTINCT
    (SELECT receipt_contract_address AS address FROM transactions) UNION DISTINCT
    (SELECT miner AS address FROM blocks) UNION DISTINCT
    (SELECT address FROM contracts) UNION DISTINCT
    (SELECT address FROM withdrawals)
)
SELECT *
FROM `bigquery-public-data.crypto_ethereum.balances` RIGHT OUTER JOIN needed_addresses USING (address)
WHERE address IS NOT NULL;
