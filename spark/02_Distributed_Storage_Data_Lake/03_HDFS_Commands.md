# HDFS Commands

## Introduction

HDFS commands look similar to Linux commands, but they operate on Hadoop Distributed File System instead of local machine storage.

Two command styles are commonly used:

```bash
hadoop fs -<command>
hdfs dfs -<command>
```

For most file system operations, both work similarly.

## Local File System Vs HDFS

This is the most important thing to understand before running commands.

```text
Local path on gateway node:
    /home/<username>

HDFS path:
    /user/<username>
```

They may look similar, but they are completely different file systems.

```text
Gateway local file system
    /home/<username>/landing/orders.csv

HDFS
    /user/<username>/data/landing/orders.csv
```

Use Linux commands for local files:

```bash
ls /home/<username>
cp local_file another_file
```

Use HDFS commands for HDFS files:

```bash
hadoop fs -ls /user/<username>
hadoop fs -put local_file hdfs_path
```

## Gateway Node Vs Hadoop Nodes

Gateway node, also called edge node, is the machine users log in to.

It is used to:

- Run shell commands.
- Submit Hadoop/Spark jobs.
- Run HDFS commands.
- Access cluster services.

Hadoop nodes are machines that actually run services like:

- NameNode.
- DataNode.
- ResourceManager.
- NodeManager.

Simple view:

```text
User
  |
  v
Gateway Node
  |
  v
Hadoop Cluster
```

## Why HDFS Does Not Have `cd` And `pwd`

Linux has current working directory.

HDFS commands usually require explicit paths.

These do not work like Linux:

```bash
hadoop fs -pwd
hadoop fs -cd /user/<username>
```

Instead, always specify path:

```bash
hadoop fs -ls /user/<username>
hadoop fs -ls /public
```

If no path is given:

```bash
hadoop fs -ls
```

It usually lists your HDFS home directory.

## Listing Files

List HDFS home:

```bash
hadoop fs -ls
```

List specific HDFS path:

```bash
hadoop fs -ls /user/<username>
```

List HDFS root:

```bash
hadoop fs -ls /
```

Sort by time reversed:

```bash
hadoop fs -ls -t -r /
```

Sort by size and show human-readable size:

```bash
hadoop fs -ls -S -h /order_results
```

Filter output:

```bash
hadoop fs -ls /user | grep <username>
```

Important:

`hadoop fs -ls` mainly talks to NameNode because it needs metadata, not actual file content.

## Creating Directories

Create one directory:

```bash
hadoop fs -mkdir /user/<username>/retail_db
```

Create nested directories:

```bash
hadoop fs -mkdir -p /user/<username>/dir1/dir2
```

Without `-p`, this fails if parent directory does not exist.

Common shortcut:

If you are working inside your HDFS home, relative HDFS path is allowed:

```bash
hadoop fs -mkdir -p data/landing
```

This usually maps to:

```text
/user/<username>/data/landing
```

## Uploading Files: Local To HDFS

Use `put`:

```bash
hadoop fs -put <local_file_path> <hdfs_path>
```

Example:

```bash
hadoop fs -put /home/<username>/staging/orders_filtered.csv data/landing
```

Use `copyFromLocal`:

```bash
hadoop fs -copyFromLocal <local_file_path> <hdfs_path>
```

Example:

```bash
hadoop fs -copyFromLocal /data/ujjawalsingh/bigLog.txt /user/<username>/data
```

`put` and `copyFromLocal` are commonly treated as similar for normal upload use.

## Downloading Files: HDFS To Local

Use `get`:

```bash
hadoop fs -get <hdfs_path> <local_path>
```

Example:

```bash
hadoop fs -get /user/<username>/data/results/orders_result.csv /home/<username>/data/results
```

Use `copyToLocal`:

```bash
hadoop fs -copyToLocal <hdfs_path> <local_path>
```

Example:

```bash
hadoop fs -copyToLocal data/results/orders_result.csv .
```

## Copying Within HDFS

Copy from one HDFS path to another:

```bash
hadoop fs -cp data/landing/file.csv data/staging/
```

This does not involve local file system.

## Moving Within HDFS

Move from one HDFS path to another:

```bash
hadoop fs -mv data/landing/orders_filtered.csv data/staging
```

Use this for HDFS-to-HDFS move.

Do not use local `mv` for HDFS files.

## Reading Files In HDFS

View file:

```bash
hadoop fs -cat data/landing/orders_filtered.csv
```

Preview first lines:

```bash
hadoop fs -cat data/landing/orders_filtered.csv | head
```

Count records:

```bash
hadoop fs -cat data/landing/orders_filtered.csv | wc -l
```

Important:

Commands like `cat` read actual data blocks, so they interact with DataNodes after locating metadata through NameNode.

## Disk Space

Check HDFS space:

```bash
hadoop fs -df -h /user/<username>
```

`-h` means human-readable.

## Permissions In HDFS

Change permissions:

```bash
hadoop fs -chmod 764 data/landing/orders_filtered.csv
```

Meaning:

```text
owner  = 7 = rwx
group  = 6 = rw-
others = 4 = r--
```

## Delete Files And Directories

Delete file:

```bash
hadoop fs -rm data/landing/file.csv
```

Delete directory recursively:

```bash
hadoop fs -rm -R data/landing
```

Be careful:

HDFS delete commands remove data from distributed storage. Always list the path before deleting.

## Check Block Locations

Use `fsck`:

```bash
hdfs fsck data/biglog.txt -files -blocks -locations
```

This shows:

- File status.
- Blocks.
- Block size.
- DataNode locations.

Use when:

- You want to understand how a file is distributed.
- You are debugging block replication.
- You want to see where data lives.

## Lab Paths To Remember

Common course paths:

```text
Local shared data:
    /data/retail_db/orders
    /data/ujjawalsingh

HDFS public data:
    /public

Local home:
    /home/<username>

HDFS home:
    /user/<username>
```

## MySQL Lab Commands

Connect:

```bash
mysql -u retail_user -h <mysql-host> -p
```

Then enter password.

Common commands:

```sql
show databases;
use retail_db;
use retail_export;
```

Course note:

- `retail_db` may be read-only.
- Use `retail_export` for creating your own tables.

Security note:

Do not store real lab passwords in notes.

## Common Mistakes

- Using `ls` when you mean `hadoop fs -ls`.
- Using local `/home/<username>` path as if it is HDFS.
- Using HDFS `/user/<username>` path as if it is local.
- Forgetting `-p` when creating nested HDFS directories.
- Running `hadoop fs -mkdir data/dir1/dir2` when `data/dir1` does not exist.
- Expecting `hadoop fs -cd` or `hadoop fs -pwd` to work.
- Using local `cp` to copy HDFS files.
- Forgetting that `cat` can be expensive for huge files.

## Best Practices

- Always know whether your file is local or HDFS.
- Use `hadoop fs -ls` to confirm target path before upload/delete.
- Use relative HDFS paths only when you are clear they map to HDFS home.
- Keep local landing/staging separate from HDFS landing/staging.
- Use `-h` for readable file sizes.
- Use `fsck` to understand block placement when learning.
- Avoid putting sensitive credentials in command history or notes.

## HDFS Command Cheat Sheet

| Task | Command |
|---|---|
| List HDFS files | `hadoop fs -ls <path>` |
| Make directory | `hadoop fs -mkdir -p <path>` |
| Upload local to HDFS | `hadoop fs -put <local> <hdfs>` |
| Download HDFS to local | `hadoop fs -get <hdfs> <local>` |
| Copy within HDFS | `hadoop fs -cp <src> <dst>` |
| Move within HDFS | `hadoop fs -mv <src> <dst>` |
| View file | `hadoop fs -cat <path>` |
| Count lines | `hadoop fs -cat <path> \| wc -l` |
| Disk space | `hadoop fs -df -h <path>` |
| Change permission | `hadoop fs -chmod 764 <path>` |
| Delete recursively | `hadoop fs -rm -R <path>` |
| Block locations | `hdfs fsck <path> -files -blocks -locations` |

## Interview Questions

### Beginner Questions

- What is the difference between local path and HDFS path?
- What is the difference between `hadoop fs` and `hdfs dfs`?
- How do you upload a file to HDFS?
- How do you download a file from HDFS?
- How do you list files in HDFS?

### Intermediate Questions

- Why does HDFS not support `cd` like Linux?
- Which HDFS commands talk mainly to NameNode?
- Which commands read data from DataNodes?
- How do you check HDFS block locations?
- What is the difference between `put` and `copyFromLocal`?

### Scenario-Based Questions

You copied a file to `/home/<username>/landing`, but `hadoop fs -ls /user/<username>/landing` does not show it. Why?

Answer:

Because `/home/<username>/landing` is local gateway storage. `/user/<username>/landing` is HDFS. You must upload using `hadoop fs -put`.

## Quick Revision

```text
Local home = /home/<username>
HDFS home  = /user/<username>

Local -> HDFS = put / copyFromLocal
HDFS -> Local = get / copyToLocal
HDFS -> HDFS  = cp / mv

ls metadata mostly from NameNode
cat reads actual data from DataNodes
```
