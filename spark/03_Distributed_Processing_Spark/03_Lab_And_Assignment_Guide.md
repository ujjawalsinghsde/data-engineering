# Lab And Assignment Guide

## Introduction

This lab work has three parts:

1. Run MapReduce JAR programs.
2. Run PySpark word count.
3. Build LinkedIn profile views logic in PySpark.

This note is written like a practical checklist.

## Important Safety Note

Use placeholders in notes:

```text
<username>
<gateway-host>
```

Do not store real lab passwords in Git or study notes.

## Part 1: Run MapReduce JARs

### Step 1: Check JAR Location

```bash
ls /data/ujjawalsingh/mapreduce_jars
```

Expected JARs:

```text
mapreduce_prog.jar
mapreduce_prog_0_reducer.jar
mapreduce_prog_2_reducer.jar
mapreduce_prog_cpartitioner.jar
mapreduce_prog_combiner.jar
```

Common mistake:

Wrong:

```bash
ls data/ujjawalsingh/mapreduce_jars
```

Correct:

```bash
ls /data/ujjawalsingh/mapreduce_jars
```

The leading `/` matters.

### Step 2: Create HDFS Input Directory

```bash
hadoop fs -mkdir -p /user/<username>/data/input
```

### Step 3: Create Local Input File

```bash
cd ~
vi inputfile.txt
```

Add:

```text
big data is interesting
big data is trending technology
```

Save in `vi`:

```text
Esc
:wq
```

### Step 4: Upload Input File To HDFS

```bash
hadoop fs -put /home/<username>/inputfile.txt /user/<username>/data/input
```

Check:

```bash
hadoop fs -ls /user/<username>/data/input
```

### Step 5: Run Normal Word Count JAR

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output_program_1
```

Check output:

```bash
hadoop fs -ls /user/<username>/data/output_program_1
hadoop fs -cat /user/<username>/data/output_program_1/part-r-00000
```

Output folder contains:

```text
_SUCCESS
part-r-00000
```

### Step 6: Run Zero Reducer JAR

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog_0_reducer.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output_program_0_reducer
```

Check:

```bash
hadoop fs -ls /user/<username>/data/output_program_0_reducer
hadoop fs -cat /user/<username>/data/output_program_0_reducer/part-m-00000
```

Why `part-m`?

Because there is no reducer. Mapper output becomes final output.

### Step 7: Run Two Reducer JAR

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog_2_reducer.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output_program_2_reducers
```

Check:

```bash
hadoop fs -ls /user/<username>/data/output_program_2_reducers
hadoop fs -cat /user/<username>/data/output_program_2_reducers/part-r-00000
hadoop fs -cat /user/<username>/data/output_program_2_reducers/part-r-00001
```

Why two part files?

Because there are two reducers.

### Step 8: Run Custom Partitioner JAR

Custom logic:

```text
word length <= 3 -> reducer 0
word length > 3  -> reducer 1
```

Command:

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog_cpartitioner.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output_program_custom_partitioner
```

Check:

```bash
hadoop fs -cat /user/<username>/data/output_program_custom_partitioner/part-r-00000
hadoop fs -cat /user/<username>/data/output_program_custom_partitioner/part-r-00001
```

### Step 9: Run Combiner JAR

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog_combiner.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output_program_combiner
```

Check:

```bash
hadoop fs -cat /user/<username>/data/output_program_combiner/part-r-00000
```

### Optional: Try Big Log File

Upload:

```bash
hadoop fs -put /data/ujjawalsingh/bigLog.txt /user/<username>/data/input
```

Run:

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog_combiner.jar \
  /user/<username>/data/input/bigLog.txt \
  /user/<username>/data/output_biglog_combiner
```

Preview:

```bash
hadoop fs -head /user/<username>/data/output_biglog_combiner/part-r-00000
```

## Part 2: Run PySpark Word Count

Open PySpark3/Jupyter notebook in lab.

Create SparkSession:

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession.builder
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

Run word count:

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/inputfile.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda word: (word, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

rdd4.take(10)
```

Save:

```python
rdd4.saveAsTextFile(f"/user/{username}/data/newoutput")
```

Check from terminal:

```bash
hadoop fs -ls /user/<username>/data/newoutput
hadoop fs -cat /user/<username>/data/newoutput/*
```

## Part 3: LinkedIn Profile Views Assignment

### Requirement

Create a file:

```text
linkedin_views.csv
```

Structure:

```text
id,from_member,to_member
```

But course records may not include header:

```text
1,Manasa,Sumit
2,Deepa,Sumit
3,Sumit,Manasa
4,Manasa,Deepa
5,Deepa,Manasa
6,Shilpy,Manasa
```

Goal:

Find how many times each profile was viewed.

Expected result:

```text
Sumit  -> 2
Manasa -> 3
Deepa  -> 1
```

### Step 1: Create Local Directory And File

```bash
mkdir -p /home/<username>/data/input
vi /home/<username>/data/input/linkedin_views.csv
```

Add:

```text
1,Manasa,Sumit
2,Deepa,Sumit
3,Sumit,Manasa
4,Manasa,Deepa
5,Deepa,Manasa
6,Shilpy,Manasa
```

Check:

```bash
cat /home/<username>/data/input/linkedin_views.csv
```

### Step 2: Upload To HDFS

```bash
hadoop fs -mkdir -p /user/<username>/data/input
hadoop fs -put /home/<username>/data/input/linkedin_views.csv /user/<username>/data/input
```

Check:

```bash
hadoop fs -ls /user/<username>/data/input
hadoop fs -cat /user/<username>/data/input/linkedin_views.csv
```

### Step 3: Run PySpark Logic

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/linkedin_views.csv")

rdd2 = rdd1.filter(lambda row: row.strip() != "")

rdd3 = rdd2.map(lambda row: row.split(",")[2])

rdd4 = rdd3.map(lambda name: (name, 1))

rdd5 = rdd4.reduceByKey(lambda x, y: x + y)

rdd5.take(10)
```

Expected output:

```text
[('Sumit', 2), ('Manasa', 3), ('Deepa', 1)]
```

### Step 4: Save Output

```python
rdd5.saveAsTextFile(f"/user/{username}/data/output")
```

Check:

```bash
hadoop fs -ls /user/<username>/data/output
hadoop fs -cat /user/<username>/data/output/*
```

## Better LinkedIn Code With Header Handling

If the file has a header:

```text
id,from_member,to_member
1,Manasa,Sumit
```

Use:

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/linkedin_views.csv")
header = rdd1.first()

result = (
    rdd1
    .filter(lambda row: row != header)
    .filter(lambda row: row.strip() != "")
    .map(lambda row: row.split(","))
    .filter(lambda cols: len(cols) == 3)
    .map(lambda cols: (cols[2].strip(), 1))
    .reduceByKey(lambda x, y: x + y)
)

result.saveAsTextFile(f"/user/{username}/data/output")
```

Why this is better:

- Removes header.
- Handles empty lines.
- Avoids index error.
- Trims spaces.

## Clean Up Before Rerun

Output directories must not exist before Spark or MapReduce writes.

```bash
hadoop fs -rm -R /user/<username>/data/output_program_1
hadoop fs -rm -R /user/<username>/data/output_program_0_reducer
hadoop fs -rm -R /user/<username>/data/output_program_2_reducers
hadoop fs -rm -R /user/<username>/data/output_program_custom_partitioner
hadoop fs -rm -R /user/<username>/data/output_program_combiner
hadoop fs -rm -R /user/<username>/data/newoutput
hadoop fs -rm -R /user/<username>/data/output
```

Only delete your own directories.

## Common Lab Errors

### JAR Files Not Found

Use absolute path:

```bash
ls /data/ujjawalsingh/mapreduce_jars
```

### Output Directory Already Exists

Fix:

```bash
hadoop fs -rm -R /user/<username>/data/output
```

or choose a new output path.

### NameNode Is In Safe Mode

Check:

```bash
hdfs dfsadmin -safemode get
```

Leave safe mode:

```bash
hdfs dfsadmin -safemode leave
```

In shared lab, if permission is denied, contact support.

### SparkSession Error

Try:

```python
spark.stop()
```

Then restart kernel and rerun boilerplate.

### Boilerplate Syntax Error

Avoid blank lines after `\`.

Use parentheses style instead.

### LinkedIn Code Fails With PythonException

Likely reason:

Empty line at bottom of file.

Fix:

```python
.filter(lambda row: row.strip() != "")
```

## Real Project Perspective

This lab is a mini version of production distributed processing.

Production flow:

```text
Raw input file
    |
    v
Distributed storage
    |
    v
Spark job
    |
    v
Output path/table
    |
    v
Validation and monitoring
```

Production improvements:

- Validate schema.
- Handle bad records.
- Add logging.
- Add record counts.
- Add retries.
- Use date-partitioned output paths.
- Make jobs idempotent.
- Monitor Spark UI metrics.
- Avoid `collect()` for large output.

## Interview Questions

### Beginner Questions

- How do you run a Hadoop JAR?
- What is `_SUCCESS` file?
- What is `part-r-00000`?
- How do you create SparkSession?
- How do you read a text file using RDD?
- How do you save RDD output to HDFS?

### Intermediate Questions

- Why does Spark fail if output path already exists?
- Why does zero reducer output create `part-m-*`?
- How do you count profile views using RDD?
- How do you handle empty lines in input?
- Why use `reduceByKey` for counting?

### Senior Data Engineer Questions

- How would you productionize the LinkedIn profile view job?
- How would you monitor this Spark job?
- How would you make the job rerunnable?
- How would you handle malformed CSV rows?
- How would you avoid too many small output files?

## Quick Revision

```text
MapReduce:
hadoop jar <jar> <input_hdfs_path> <output_hdfs_path>

Spark word count:
textFile -> flatMap -> map -> reduceByKey -> saveAsTextFile

LinkedIn:
split CSV -> take to_member -> (name, 1) -> reduceByKey

Common issue:
output path already exists
```
