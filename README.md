# CrypQ: A Database Benchmark Based on Dynamic, Ever-Evolving Ethereum Data

## Setting up a Database Slice

### 1. Extracting a Slice from Google BigQuery

The goal of this step is to produce a collection of JSON Lines data files for loading by subsequent steps.
A collection of such files (as a `tar.gz` ball) for a sample data slice is provided
[here](https://drive.google.com/file/d/17vzVf5XhU8uFLR3JTf_lm2EiDmbWliWw/view?usp=sharing),
which contains data on 2000 blocks numbered [19005000, 19007000).

To extract a different slice, follow the procedure below:

* If you'd like to interact with BigQuery through the command line, install gcloud CLI here:

  - https://cloud.google.com/sdk/docs/install
  - https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python

  Downloads can also be made through their web interface.
* Create a dataset on BigQuery, named the same as the value of `@@dataset_id`
  that is set at the beginning of the script `bigquery_extract.sql`.
* Run `bigquery_extract.sql` in BigQuery to populate your dataset
  using the Google-maintained public dataset `bigquery-public-data.crypto_ethereum`.
  - **Note:**
    The structure of the Google-maintained dataset may change from time to time and is beyond our control.
    We will continuously monitor it and modify `bigquery_extract.sql` to maintain compatibility with the CrypQ schema and the remaining setup steps.
    (The last update happened in August 2024.)
* Export the data files from all tables in the extracted dataset in the **JSON Lines** format,
  and make them available under a directory for the next step.
  Name each file as `tablename.jsonl`, where `tablename` is the name of the table (all lower-case).
  - **Note:**
    If you have downloaded the data files in JSON instead of JSON Lines format,
    you can use [jq](https://jqlang.github.io/jq/), a light-weight command-line JSON processor,
    to convert a JSON file into JSONL file:
    ```
    jq -c '.[]' tablename.json > tablename.jsonl
    ```

### 2. Creating and Loading Your Database

The following instructions assumes PostgreSQL; steps for other database systems may vary.
Pick a name for your database, say `crypq`.

* Create the database schema with empty tables:
  ```
  dropdb crypq
  createdb crypq
  psql crypq -f create.sql
  ```
  The file `create.sql` is extensively documented with explanation of how data is represented.

* From the directory containing JSON Lines files:
  ```
  psql crypq -f load.sql
  ```

Congrats!
Your slice of the Etheurem database is now ready to go!

## Benchmark Queries

The benchmark queries reside in the `queries/` subdirectory as `.sql` files.
Some of the queries are parameterized: they have concrete default settings but can be overridden.

## Update Workload

To prepare an update workload, you need to first extract a larger slice of data:
then, the database will start out containing some blocks at the beginning of the extracted slice,
and then gradually evolve according to the subsequent blocks.

In the following, we assume that you have already prepared a slice with 2000 blocks numbered [19005000, 19007000)
in a database named `crypq`.
Scripts related to updates reside in the `updates/` subdirectory,
where the following should be run.

The following command would make `crypq` contain the first 1000 blocks initially,
and output a sequence of `.sql` files under `outdir/`
that you can use to "play forward" the updates 1 blocks at a time:
```
./gen_updates.sh crypq 19006000 1 outdir
```
The output `outdir/upserts-*.sql` files should be executed in sequence starting from the initial database state, as follows:
```
psql crypq -f outdir/upserts-19006000.sql
psql crypq -f outdir/upserts-19006001.sql
# ...
psql crypq -f outdir/upserts-19006999.sql
```
Optionally, if you prefer to keep the database size roughly constant over time,
you can call the `expire.sql` script with appropriate parameters to remove data pertaining to old blocks.
For example, to keep the database at 1000 blocks, you would do:
```
psql crypq -v BLK_START=19005001 -f expire.sql
psql crypq -f outdir/upserts-19006000.sql
psql crypq -v BLK_START=19005002 -f expire.sql
psql crypq -f outdir/upserts-19006001.sql
# ...
psql crypq -v BLK_START=19006000 -f expire.sql
psql crypq -f outdir/upserts-19006999.sql
```
Finally, the `gen_updates.sh` script saves the full slice and the initial database state in `.sql.gz` files,
which you can use to restore these states later (see the contents of `run_updates.sh` for usage examples).

We also provide an example script `run_updates.sh` that executes the generated update workload.
By default, it will start by loading the initial database state and play through the sequence of updates.
The final state can then be compared with the full slice ---
they should be the same if no expiration was done.
Type `./run_updates.sh` for help.

## Citing CrypQ

If you would like to cite this benchmark in your work, please use:

* Vincent Capol, Yuxi Liu, Haibo Xiu, and Jun Yang. "CrypQ: A Database Benchmark Based on Dynamic, Ever-Evolving Ethereum Data." In Proceedings of the Sixteenth TPC Technology Conference on Performance Evaluation & Benchmarking (TPCTC 2024), Guangzhou, China, August 2024.

This paper can be accessed as [PDF here](https://github.com/dukedb-crypq/crypq-bench/blob/main/tpctc24-CapolLXY-CrypQ.pdf).
Figure 1 of the paper shows a schema diagram for CrypQ, which you may find helpful.

```
@inproceedings{tpctc24-CapolLXY-CrypQ,
  author       = {Vincent Capol and Yuxi Liu and Haibo Xiu and Jun Yang},
  editor       = {Raghunath Nambiar and Meikel Poess},
  title        = {{CrypQ}: A Database Benchmark Based on Dynamic, Ever-Evolving {Ethereum} Data},
  booktitle    = {Proceedings of the Sixteenth {TPC} Technology Conference on Performance Evaluation and Benchmarking ({TPCTC} 2024)},
  address      = {Guangzhou, China},
  month        = {August},
  year         = {2024},
  note         = {\url{https://github.com/dukedb-crypq}}
}
```
