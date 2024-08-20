#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "illegal number of arguments"
    echo "usage:"
    echo "$0 DB BLK_START NUM_BLKS OUTDIR"
    echo "* DB: database name; should contain the full slice to generate all updates from"
    echo "* BLK_START: initial state will contain blocks up to BLK_START-1"
    echo "* NUM_BLKS: number of blocks in each update batch"
    echo "* OUTDIR: directory to write output files (old contents, if any, won't be cleaned)"
    exit 1
fi
DB=$1
BLK_START_INITIAL=$2
NUM_BLKS=$3
OUTDIR=$4
mkdir -p $OUTDIR || exit 1

BLK_MIN=`psql $DB -Atc "SELECT MIN(number) from Blocks"`
BLK_MAX=`psql $DB -Atc "SELECT MAX(number) from Blocks"`
TABLES=('addresses' 'blocks' 'withdrawals' 'contracts' 'transactions' 'tokens' 'token_transactions')
# note the use of lower case above because of PostgreSQL internal representation

function stats_tables {
    for TABLE in "${TABLES[@]}"; do
        echo "$TABLE: `psql $DB -Atc "SELECT COUNT(*) FROM $TABLE"` rows"
    done
}

function drop_upsert_tables {
    for TABLE in "${TABLES[@]}"; do
        psql $DB -c "DROP TABLE IF EXISTS new_$TABLE" >/dev/null 2>&1
    done
    psql $DB -c "DROP TABLE IF EXISTS update_Addresses" >/dev/null 2>&1
}

function stats_upsert_tables {
    for TABLE in "${TABLES[@]}"; do
        echo "new_$TABLE: `psql $DB -Atc "SELECT COUNT(*) FROM new_$TABLE"` rows"
    done
    echo "update_Addresses: `psql $DB -Atc "SELECT COUNT(*) FROM update_Addresses"` rows"
}

echo "***** backing up full slice *****"
pg_dump $DB | gzip -9 > "$OUTDIR/$DB-$BLK_MIN-$BLK_MAX.sql.gz"
stats_tables
echo "***** full slice saved in $OUTDIR/$DB-$BLK_MIN-$BLK_MAX.sql.gz *****"

for ((BLK_START=BLK_START_INITIAL; BLK_START<=BLK_MAX; BLK_START+=NUM_BLKS)); do
    echo "***** processing $NUM_BLKS blocks starting at $BLK_START *****"
    psql $DB -v BLK_START=$BLK_START -v NUM_BLKS=$NUM_BLKS -f prepsert.sql >/dev/null
    FILE="$OUTDIR/upserts-$BLK_START.sql"
    rm -f "$FILE"; touch "$FILE"
    for TABLE in "${TABLES[@]}"; do
        pg_dump --table=new_$TABLE --data-only --column-inserts $DB\
            | grep 'INSERT INTO'\
            | sed "s/^INSERT INTO public.new_$TABLE/INSERT INTO $TABLE/"\
            >>"$FILE"
    done
    psql $DB -Atc "SELECT 'UPDATE addresses SET eth_balance = ' || CAST(eth_balance AS VARCHAR) || ' WHERE address = ''' || CAST(address AS VARCHAR) || ''';' FROM update_addresses"\
            >>"$FILE"
    stats_upsert_tables
done
drop_upsert_tables

echo "***** truncating to initial state *****"
psql $DB -v BLK_START=$BLK_START_INITIAL -f truncate.sql # >/dev/null slow, so let's see some progress indicator
pg_dump $DB | gzip -9 > "$OUTDIR/$DB-$BLK_MIN-$((BLK_START_INITIAL-1)).sql.gz"
echo "***** initial state saved in $OUTDIR/$DB-$BLK_MIN-$((BLK_START_INITIAL-1)).sql.gz *****"
