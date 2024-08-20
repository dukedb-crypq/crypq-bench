#!/bin/bash

if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
    echo "illegal number of arguments"
    echo "usage:"
    echo "$0 FULL_DUMP_SQL_GZ_FILE INIT_STATE_DUMP_SQL_GZ_FILE NUM_BLKS [expire]"
    echo "* FULL_DUMP_SQL_GZ_FILE: .sql.gz file containing the full slice dump"
    echo "* INIT_STATE_DUMP_SQL_GZ_FILE: .sql.gz file containing the initial state dump;"
    echo "  its path and file name will be used to infer location of upserts*.sql files and window configuration,"
    echo "  so make sure it hasn't been moved around or renamed"
    echo "* NUM_BLKS: number of blocks in each update batch;"
    echo "  must be consistent with how upserts*.sql files were generated"
    echo "* expire (optional): if this keyword is given as the 4th argument,"
    echo "  expire the oldest blocks to keep the number of blocks in the database constant"
    exit 1
fi
FULL_DUMP_SQL_GZ_FILE="$1"
INIT_STATE_DUMP_SQL_GZ_FILE="$2"
NUM_BLKS=$3
EXPIRE=$4

OUTDIR=$(dirname "$INIT_STATE_DUMP_SQL_GZ_FILE")
DB=`echo $(basename "$INIT_STATE_DUMP_SQL_GZ_FILE") | sed 's/^\([^-]*\)-\([0-9]\+\)-\([0-9]\+\)\.sql\.gz/\1/'`
BLK_MIN=`echo $(basename "$INIT_STATE_DUMP_SQL_GZ_FILE") | sed 's/^\([^-]*\)-\([0-9]\+\)-\([0-9]\+\)\.sql\.gz/\2/'`
BLK_MAX=`echo $(basename "$FULL_DUMP_SQL_GZ_FILE") | sed 's/^\([^-]*\)-\([0-9]\+\)-\([0-9]\+\)\.sql\.gz/\3/'`
BLK_START_INITIAL=`echo $(basename "$INIT_STATE_DUMP_SQL_GZ_FILE") | sed 's/^\([^-]*\)-\([0-9]\+\)-\([0-9]\+\)\.sql\.gz/\3/'`
((BLK_START_INITIAL++))
TABLES=('addresses' 'blocks' 'withdrawals' 'contracts' 'transactions' 'tokens' 'token_transactions')
# note the use of lower case above because of PostgreSQL internal representation

function stats_tables {
    for TABLE in "${TABLES[@]}"; do
        echo "$TABLE: `psql $DB -Atc "SELECT COUNT(*) FROM $TABLE"` rows"
    done
}

echo "***** restoring initial state: blocks [$BLK_MIN, $BLK_START_INITIAL) *****"
dropdb $DB
createdb $DB
gunzip -c $INIT_STATE_DUMP_SQL_GZ_FILE | psql $DB >/dev/null
stats_tables

BLK_MIN_ITER=$BLK_MIN
for ((BLK_START=BLK_START_INITIAL; BLK_START<=BLK_MAX; BLK_START+=NUM_BLKS)); do
    if [[ "$EXPIRE" == 'expire' ]]; then
        ((BLK_MIN_ITER+=NUM_BLKS))
        echo "***** expiring blocks in [-infty, $BLK_MIN_ITER) *****"
        psql $DB -v BLK_START=$BLK_MIN_ITER -f expire.sql >/dev/null
    fi
    echo "***** adding blocks [$BLK_START, $((BLK_START+NUM_BLKS))) *****"
    psql $DB -f "$OUTDIR/upserts-$BLK_START.sql" >/dev/null
    stats_tables
done

echo "***** restoring to full state for comparison: blocks [$BLK_MIN, $BLK_MAX] *****"
dropdb $DB
createdb $DB
gunzip -c $FULL_DUMP_SQL_GZ_FILE | psql $DB >/dev/null
stats_tables
