# Assignment Walkthrough

## Introduction

This assignment simulates a small real-world data pipeline.

The flow is:

```text
Shared source file on gateway node
        |
        v
Local landing folder
        |
        v
Filter records with Linux command
        |
        v
Local staging folder
        |
        v
Upload to HDFS landing
        |
        v
Move to HDFS staging
        |
        v
Simulate processed result
        |
        v
Upload result to HDFS
        |
        v
Download result to local
        |
        v
Rename final output
        |
        v
Clean up
```

This is a mini version of a production pipeline:

```text
Source -> Landing -> Staging -> Processing -> Results -> Consumer
```

## Important Path Concepts

Local gateway paths:

```text
/home/<username>
/home/<username>/landing
/home/<username>/staging
/data/retail_db/orders
```

HDFS paths:

```text
/user/<username>
/user/<username>/data/landing
/user/<username>/data/staging
/user/<username>/data/results
```

Shortcut:

When using `hadoop fs`, this:

```bash
hadoop fs -ls data/landing
```

usually means:

```text
/user/<username>/data/landing
```

## Step 1: Login To Gateway Node

Use SSH:

```bash
ssh <username>@<gateway-host>
```

Enter password when prompted.

Security note:

Do not store actual lab password in Git or notes.

## Step 2: Check Home Directory

```bash
pwd
```

Expected output:

```text
/home/<username>
```

Why:

We need to know where local files will be created.

## Step 3: Create Local Landing And Staging Folders

```bash
cd ~
mkdir landing
mkdir staging
```

Why:

- Landing stores raw incoming file.
- Staging stores filtered/intermediate file.

Production meaning:

Landing is usually raw data. Staging is data prepared for further processing.

## Step 4: Copy Orders File To Local Landing

```bash
cp /data/retail_db/orders/part-00000 landing
```

Why:

The assignment says a third-party service drops `orders.csv` into landing. Since we are simulating, we copy existing lab data.

Check:

```bash
ls -l landing
```

## Step 5: Preview PENDING_PAYMENT Records

```bash
grep PENDING_PAYMENT /home/<username>/landing/part-00000 | head
```

Why:

- `grep` filters matching lines.
- `head` previews first 10 records.

Expected sample:

```text
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
```

## Step 6: Save Filtered Records To Local Staging

```bash
grep PENDING_PAYMENT /home/<username>/landing/part-00000 > /home/<username>/staging/orders_filtered.csv
```

Important:

Use `>` when creating fresh output.

Use `>>` only when you intentionally want to append.

Common mistake:

If you run `>>` multiple times, duplicate records are appended again and again.

Check count:

```bash
wc -l /home/<username>/staging/orders_filtered.csv
```

## Step 7: Create HDFS Landing Directory

```bash
hadoop fs -mkdir -p data/landing
```

Why `-p`:

It creates parent directories if they do not exist.

## Step 8: Upload Filtered File To HDFS Landing

```bash
hadoop fs -put /home/<username>/staging/orders_filtered.csv data/landing
```

Check:

```bash
hadoop fs -ls data/landing
```

## Step 9: Count Records In HDFS File

```bash
hadoop fs -cat data/landing/orders_filtered.csv | wc -l
```

Why:

This confirms that records reached HDFS.

Production habit:

Always validate counts after movement.

## Step 10: List HDFS File With Human-Readable Size

```bash
hadoop fs -ls -h -S data/landing
```

Meaning:

- `-h` shows KB/MB/GB.
- `-S` sorts by size.

## Step 11: Change HDFS File Permission

Requirement:

- Owner: read, write, execute.
- Group: read, write.
- Others: read.

Command:

```bash
hadoop fs -chmod 764 data/landing/orders_filtered.csv
```

Breakdown:

```text
7 = rwx
6 = rw-
4 = r--
```

## Step 12: Move File From HDFS Landing To HDFS Staging

Create staging:

```bash
hadoop fs -mkdir -p data/staging
```

Move:

```bash
hadoop fs -mv data/landing/orders_filtered.csv data/staging
```

Check:

```bash
hadoop fs -ls data/staging
hadoop fs -ls data/landing
```

Production meaning:

Once a file is validated, it may move from landing to staging.

## Step 13: Create Simulated Spark Output File Locally

Create file:

```bash
cd ~
vi orders_result.csv
```

Press `i`, paste/type:

```text
3617,2013-08-15 00:00:00.0,8889,PENDING_PAYMENT
68714,2013-09-06 00:00:00.0,8889,PENDING_PAYMENT
```

Save:

```text
Esc
:wq
```

Check:

```bash
cat orders_result.csv
```

## Step 14: Upload Result File To HDFS Results

Create results directory:

```bash
hadoop fs -mkdir -p data/results
```

Upload:

```bash
hadoop fs -put /home/<username>/orders_result.csv data/results
```

Check:

```bash
hadoop fs -ls data/results
```

## Step 15: Download Result File Back To Local

Create local results folder:

```bash
mkdir -p data/results
```

Download:

```bash
hadoop fs -get /user/<username>/data/results/orders_result.csv /home/<username>/data/results
```

or:

```bash
hadoop fs -get data/results/orders_result.csv /home/<username>/data/results
```

Check:

```bash
ls -l /home/<username>/data/results
```

## Step 16: Rename Local Result File

```bash
mv data/results/orders_result.csv data/results/final_results.csv
```

Check:

```bash
ls -l data/results
cat data/results/final_results.csv
```

## Step 17: Clean Up Local Files

Use carefully:

```bash
rm -R landing
rm -R staging
rm -R data/results
rm orders_result.csv
```

Best practice:

Run `ls` before cleanup so you know what you are deleting.

## Step 18: Clean Up HDFS Files

```bash
hadoop fs -rm -R data/landing
hadoop fs -rm -R data/staging
hadoop fs -rm -R data/results
```

Check:

```bash
hadoop fs -ls data
```

## Complete Command Flow

```bash
pwd
cd ~
mkdir landing
mkdir staging

cp /data/retail_db/orders/part-00000 landing

grep PENDING_PAYMENT /home/<username>/landing/part-00000 | head
grep PENDING_PAYMENT /home/<username>/landing/part-00000 > /home/<username>/staging/orders_filtered.csv

hadoop fs -mkdir -p data/landing
hadoop fs -put /home/<username>/staging/orders_filtered.csv data/landing

hadoop fs -cat data/landing/orders_filtered.csv | wc -l
hadoop fs -ls data/landing
hadoop fs -ls -h -S data/landing
hadoop fs -chmod 764 data/landing/orders_filtered.csv

hadoop fs -mkdir -p data/staging
hadoop fs -mv data/landing/orders_filtered.csv data/staging

cd ~
vi orders_result.csv

hadoop fs -mkdir -p data/results
hadoop fs -put /home/<username>/orders_result.csv data/results

mkdir -p data/results
hadoop fs -get data/results/orders_result.csv /home/<username>/data/results

mv data/results/orders_result.csv data/results/final_results.csv
```

## Common Errors And Fixes

### Permission Denied While Creating File

Cause:

You may be in a directory where you do not have write permission.

Fix:

```bash
cd ~
```

Then create files.

### Password Not Pasting In SSH

Cause:

Some terminals do not support `Ctrl+V` in password prompt.

Fix:

Right-click to paste. Password may not visibly appear.

### MySQL Connection Error

Use correct host and user provided by lab.

General form:

```bash
mysql -u retail_user -h <mysql-host> -p
```

Then use:

```sql
show databases;
use retail_export;
```

### `cd /landing` Fails

Cause:

`/landing` means directory under root.

Fix:

If landing is inside home:

```bash
cd ~/landing
```

or:

```bash
cd landing
```

### HDFS `pwd` And `cd` Do Not Work

HDFS does not work like Linux shell current directory.

Use full or relative HDFS paths:

```bash
hadoop fs -ls /user/<username>/data
```

## Real Project Perspective

This assignment maps to a real ingestion pipeline:

```text
Vendor drops file
      |
      v
Landing zone
      |
      v
Validation and filtering
      |
      v
Staging zone
      |
      v
Spark processing
      |
      v
Results zone
      |
      v
Consumer download/report/API
```

Production improvements:

- Add file arrival checks.
- Validate schema.
- Count input and output records.
- Move bad records to reject folder.
- Use timestamped folders.
- Avoid overwrite unless intentional.
- Make pipeline idempotent.
- Log every step.
- Send alerts on failure.

## Interview Questions

### Beginner Questions

- What is landing folder?
- What is staging folder?
- How do you filter records using `grep`?
- How do you upload local file to HDFS?
- How do you count HDFS file records?

### Intermediate Questions

- Why should we validate record counts after upload?
- What is the difference between `>` and `>>`?
- Why do we separate local landing and HDFS landing?
- How do you move a file inside HDFS?
- What does `chmod 764` mean?

### Senior Data Engineer Questions

- How would you productionize this assignment?
- How would you handle duplicate file arrival?
- How would you make this pipeline idempotent?
- How would you handle bad records?
- How would you monitor this pipeline?

## Quick Revision

```text
Local source     -> /data/retail_db/orders/part-00000
Local landing    -> ~/landing
Local staging    -> ~/staging/orders_filtered.csv
HDFS landing     -> data/landing
HDFS staging     -> data/staging
HDFS results     -> data/results
Local final file -> ~/data/results/final_results.csv
```

```text
grep filters records
put uploads local to HDFS
cat + wc -l counts HDFS records
chmod 764 sets permissions
mv renames or moves
rm -R deletes recursively
```
