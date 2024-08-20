/*

This SQL file is supposed to be executed on a freshly made crypq database,
from the directory containing JSON Lines files exported from BigQuery.
There are some additional assumptions/dependencies:
- It uses psql's \COPY to import JSONL data into tables.
- It relies on PostgreSQL's support for JSON, BYTEA (non-standard SQL), and user-defined functions.

*/

CREATE OR REPLACE FUNCTION convert_hex0x_to_bytea(IN hex0x_str VARCHAR)
RETURNS BYTEA
AS $$
BEGIN
    RETURN CASE
        WHEN hex0x_str IS NULL THEN NULL
        ELSE DECODE(RIGHT(hex0x_str, LENGTH(hex0x_str)-2), 'hex')
    END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE AGGREGATE bytea_aggr(bytea) (
    sfunc = byteacat,
    stype = bytea,
    initcond = E''
);

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'addresses.jsonl';
INSERT INTO Addresses
    SELECT
        convert_hex0x_to_bytea(doc->>'address'),
        COALESCE(CAST(doc->>'eth_balance' AS NUMERIC), 0),
        doc->>'eth_balance' IS NULL
    FROM JSON_IMPORT;
-- We will come back to fix the balances more after loading everything else.
DROP TABLE JSON_IMPORT;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'blocks.jsonl';
INSERT INTO Blocks
    SELECT
        convert_hex0x_to_bytea(doc->>'hash'),
        CAST(doc->>'number' AS NUMERIC),
        CAST(doc->>'timestamp' AS TIMESTAMP WITH TIME ZONE),
        convert_hex0x_to_bytea(doc->>'extra_data'),
        COALESCE(CAST(doc->>'base_fee_per_gas' AS NUMERIC), 0),
        CAST(doc->>'size' AS INT),
        convert_hex0x_to_bytea(doc->>'miner')
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'withdrawals.jsonl';
INSERT INTO Withdrawals
    SELECT
        convert_hex0x_to_bytea(doc->>'hash'),
        CAST(doc->>'withdrawal_index' AS NUMERIC),
        CAST(doc->>'validator_index' AS NUMERIC),
        convert_hex0x_to_bytea(doc->>'address'),
        CAST(doc->>'amount' AS NUMERIC)
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'contracts.jsonl';
INSERT INTO Contracts
    SELECT
        convert_hex0x_to_bytea(doc->>'address'),
        RANK() OVER (PARTITION BY doc->>'address'
                     ORDER BY CAST(doc->>'block_timestamp' AS TIMESTAMP WITH TIME ZONE)),
        (SELECT bytea_aggr(convert_hex0x_to_bytea(hash))
         FROM json_array_elements_text(doc->'function_sighashes') AS hashes(hash)),
        convert_hex0x_to_bytea(doc->>'bytecode'),
        CAST(doc->>'is_erc20' AS BOOLEAN),
        CAST(doc->>'is_erc721' AS BOOLEAN),
        convert_hex0x_to_bytea(doc->>'block_hash'),
        NOT EXISTS(SELECT * FROM Blocks WHERE hash = convert_hex0x_to_bytea(doc->>'block_hash'))
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'transactions.jsonl';
INSERT INTO Transactions
    SELECT
        convert_hex0x_to_bytea(doc->>'hash'),
        CAST(doc->>'transaction_index' AS INT),
        CAST(doc->>'value' AS NUMERIC),
        convert_hex0x_to_bytea(doc->>'from_address'),
        convert_hex0x_to_bytea(doc->>'to_address'),
        CAST(doc->>'gas' AS NUMERIC),
        CAST(doc->>'max_priority_fee_per_gas' AS NUMERIC),
        convert_hex0x_to_bytea(doc->>'input'),
        convert_hex0x_to_bytea(doc->>'receipt_contract_address'),
        convert_hex0x_to_bytea(doc->>'block_hash'),
        COALESCE(CAST(doc->>'transaction_type' AS INT), 0),
        CAST(doc->>'nonce' AS INT)
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;
-- Check that receipt_contract_address references Contracts(address):
DO $$ DECLARE
    dangling_ref BYTEA;
BEGIN
    SELECT receipt_contract_address STRICT INTO dangling_ref
    FROM Transactions
    WHERE receipt_contract_address NOT IN (SELECT address FROM Contracts)
    LIMIT 1;
    IF dangling_ref IS NOT NULL THEN
        RAISE WARNING 'Transactions.receipt_contract_address = ''%'' not a valid reference to Contracts(address)', dangling_ref
            USING HINT = 'This sometimes happens when the new contract has not been reflected in BigQuery';
    END IF;
END $$;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'tokens.jsonl';
INSERT INTO Tokens
    SELECT
        convert_hex0x_to_bytea(doc->>'address'),
        CAST(doc->>'symbol' AS VARCHAR),
        CAST(doc->>'name' AS VARCHAR),
        CAST(doc->>'decimals' AS INT),
        CAST(doc->>'total_supply' AS NUMERIC),
        convert_hex0x_to_bytea(doc->>'block_hash'),
        NOT EXISTS(SELECT * FROM Blocks WHERE hash = convert_hex0x_to_bytea(doc->>'block_hash'))
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;
-- Check that address references Contracts(address):
DO $$ DECLARE
    dangling_ref BYTEA;
BEGIN
    SELECT address STRICT INTO dangling_ref
    FROM Tokens
    WHERE address NOT IN (SELECT address FROM Contracts)
    LIMIT 1;
    IF dangling_ref IS NOT NULL THEN
        RAISE EXCEPTION 'Tokens.address = ''%'' not a valid reference to Contracts(address)', dangling_ref;
    END IF;
END $$;

CREATE UNLOGGED TABLE JSON_IMPORT(doc JSON);
\COPY JSON_IMPORT FROM 'token_transactions.jsonl';
INSERT INTO Token_Transactions
    SELECT
        convert_hex0x_to_bytea(doc->>'transaction_hash'),
        CAST(doc->>'log_index' AS INT),
        convert_hex0x_to_bytea(doc->>'token_address'),
        CAST(doc->>'value' AS NUMERIC)
    FROM JSON_IMPORT;
DROP TABLE JSON_IMPORT;
-- Check that token_address references Contracts(address):
DO $$ DECLARE
    dangling_ref BYTEA;
BEGIN
    SELECT token_address STRICT INTO dangling_ref
    FROM Token_Transactions
    WHERE token_address NOT IN (SELECT address FROM Contracts)
    LIMIT 1;
    IF dangling_ref IS NOT NULL THEN
        RAISE EXCEPTION 'Token_Transactions.token_address = ''%'' not a valid reference to Contracts(address)', dangling_ref;
    END IF;
END $$;

/*
Finally, fix balances to ensure that, for each address,
there exists a series of balance values over the extracted slice
that are consistent with the transactions involving this address and at no point underflow (fall below 0).
There is no guarantee of consistency with respect to transactions outside the slice.
If the extracted balance works out, it will be kept;
otherwise, we will make the smallest adjustment possible and set crypq_adjusted.
*/
WITH balance_series(address, balance, seqno) AS (
    -- historical balances for each address, in reverse chronological order,
    -- inferred from its current (last in the slice) balance:
    (SELECT address, eth_balance, 0 FROM Addresses)
    UNION ALL
    (SELECT deltas.address, eth_balance - SUM(delta) OVER w, ROW_NUMBER() OVER w
     FROM crypq_balance_deltas((SELECT MIN(number) FROM Blocks),
                               (SELECT MAX(number)-MIN(number)+1 FROM Blocks)) deltas,
          Addresses
     WHERE deltas.address = Addresses.address
     WINDOW w AS
         (PARTITION BY deltas.address
          ORDER BY block_number DESC, transaction_index DESC, delta DESC))
),
neg_min_balances(address, min_balance) AS (
    SELECT address, MIN(balance)
    FROM balance_series
    GROUP BY address
    HAVING MIN(balance) < 0
)
UPDATE Addresses
SET crypq_adjusted = TRUE,
    eth_balance = eth_balance - neg_min_balances.min_balance
FROM neg_min_balances
WHERE Addresses.address = neg_min_balances.address;
