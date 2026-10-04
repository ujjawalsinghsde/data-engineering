# HDFS Deep Dive

## Introduction

HDFS stands for Hadoop Distributed File System.

It is the storage layer of Hadoop. It is designed to store very large files across many machines in a cluster.

HDFS follows a master-slave architecture:

```text
HDFS Cluster

NameNode  -> master, stores metadata
    |
    +-- DataNode 1 -> stores actual blocks
    +-- DataNode 2 -> stores actual blocks
    +-- DataNode 3 -> stores actual blocks
    +-- DataNode 4 -> stores actual blocks
```

The NameNode does not store the actual file content. It stores metadata, which means information about the files.

DataNodes store the actual data blocks.

## Why HDFS Is Needed

A normal file system stores a file on one machine.

That works for small or medium files, but Big Data files may be hundreds of GB, TB, or PB.

Problems with storing huge files on one machine:

- One machine may not have enough disk.
- One machine becomes a bottleneck.
- If that machine fails, the data is unavailable.
- Processing the file on one machine is slow.

HDFS solves this by:

- Splitting files into blocks.
- Storing blocks across many DataNodes.
- Replicating blocks for fault tolerance.
- Allowing processing near the data.

## HDFS Components

### NameNode

The NameNode is like the index page of a book.

It knows:

- File names.
- Directory structure.
- File permissions.
- Which blocks belong to which file.
- Which DataNodes store each block.

Example metadata table:

```text
File       Block    DataNode
file1      block1   DN1
file1      block2   DN2
file1      block3   DN3
file1      block4   DN4
```

Analogy:

If a book has 1000 pages, the index tells where a topic is located. The index is not the actual content. Similarly, NameNode is the index and DataNodes are the actual pages.

### DataNode

DataNodes store real data blocks.

They also:

- Send heartbeat messages to NameNode.
- Report block information.
- Serve read requests.
- Accept write requests.
- Replicate blocks when instructed.

## Blocks In HDFS

HDFS stores large files by dividing them into blocks.

Default block size in modern Hadoop is commonly `128 MB`.

Example:

```text
File size = 500 MB

Block 1 = 128 MB
Block 2 = 128 MB
Block 3 = 128 MB
Block 4 = 116 MB
```

Another example:

```text
File size = 1 GB = 1024 MB
Block size = 128 MB

Number of blocks = 1024 / 128 = 8 blocks
```

Important:

Each DataNode can store many blocks from many files.

## Why Default Block Size Is 128 MB

This is a very common interview question.

If block size is too small:

```text
1 GB file with 128 MB blocks -> 8 blocks
1 GB file with 64 MB blocks  -> 16 blocks
1 GB file with 32 MB blocks  -> 32 blocks
1 GB file with 1 MB blocks   -> 1024 blocks
```

More blocks means:

- More metadata in NameNode memory.
- More block reports.
- More scheduling overhead.
- NameNode becomes overloaded.

Example:

If a 10 GB file uses 1 MB block size:

```text
10 GB = 10240 MB
Block size = 1 MB
Number of blocks = 10240
```

The NameNode must maintain metadata for all those blocks. Now imagine this for thousands of files.

If block size is too large:

```text
1 GB file with 128 MB blocks -> 8 blocks
1 GB file with 256 MB blocks -> 4 blocks
1 GB file with 512 MB blocks -> 2 blocks
```

Fewer blocks means:

- Less metadata burden.
- But less parallelism.
- Fewer mappers can run for the file.

So `128 MB` is a balanced default:

- Not too many blocks.
- Enough parallelism.
- Manageable metadata size.

In real production, some systems use larger block sizes like `256 MB` depending on file size and workload.

## How Client Reads A File From HDFS

When a client wants to read `file1`:

```text
Client
  |
  v
NameNode: "Where are file1 blocks?"
  |
  v
NameNode returns block locations
  |
  v
Client reads directly from DataNodes
```

Step-by-step:

1. Client sends read request to NameNode.
2. NameNode checks metadata.
3. NameNode returns block locations.
4. Client reads blocks directly from DataNodes.

Important:

The actual data does not flow through the NameNode. This keeps the NameNode from becoming a data transfer bottleneck.

## Replication Factor

Replication factor means how many copies of each block HDFS stores.

Default replication factor is usually `3`.

Example:

```text
Block 1
    |
    +-- Copy 1 on DataNode 1
    +-- Copy 2 on DataNode 2
    +-- Copy 3 on DataNode 3
```

Why replication is needed:

- If one DataNode fails, data is still available.
- HDFS can automatically create a new replica.
- Read performance can improve because clients may read the nearest copy.

Common interview point:

HDFS should not store two replicas of the same block on the same DataNode. Replicas should be spread across different nodes for fault tolerance.

## DataNode Failure

DataNodes send heartbeats to NameNode.

Heartbeat means:

"I am alive."

Typical flow:

```text
DataNode -> heartbeat every few seconds -> NameNode
```

If the NameNode does not receive heartbeats for a configured period, it marks the DataNode as dead.

Then:

- Blocks on that DataNode are treated as unavailable.
- NameNode checks if enough replicas exist.
- If replication factor is below target, HDFS creates new replicas on other DataNodes.

Example:

```text
Replication factor = 3

Block A copies:
DN1, DN2, DN3

DN2 fails

Available copies:
DN1, DN3

NameNode instructs another DataNode to create a new copy:
DN4
```

## NameNode Failure

NameNode is critical because it stores metadata.

If NameNode is unavailable, clients cannot locate blocks.

Course note mentions Secondary NameNode. Important clarification:

Secondary NameNode is not a hot standby backup. It mainly helps with checkpointing NameNode metadata.

Modern production Hadoop uses NameNode High Availability:

```text
Active NameNode
Standby NameNode
Shared edits / JournalNodes
ZooKeeper for failover
```

Interview trap:

Do not say Secondary NameNode is a direct replacement for NameNode in modern production. It is better to explain both the course concept and the modern HA concept.

## NameNode Federation

NameNode federation means having multiple NameNodes, each managing a part of the namespace.

Why it is needed:

- Metadata grows as data grows.
- One NameNode has memory limits.
- Multiple NameNodes split metadata load.
- Improves scalability.

Simple view:

```text
NameNode 1 -> /user data
NameNode 2 -> /logs data
NameNode 3 -> /warehouse data
```

Important difference:

```text
Secondary NameNode -> fault-tolerance/checkpointing concept
NameNode Federation -> scalability concept
NameNode HA         -> production failover concept
```

## Rack Awareness

A rack is a group of machines physically close to each other, usually connected to the same network switch.

Course examples used racks in different locations:

```text
Rack 1 -> Bangalore
Rack 2 -> San Francisco
Rack 3 -> Australia
```

In real data centers, racks are usually physical server racks inside one or more data centers. The key idea is failure isolation.

HDFS tries not to put all replicas in the same rack.

Why:

- If a rack switch fails, all machines in that rack may become unreachable.
- If all replicas are in one rack, data can become unavailable.
- Spreading replicas improves fault tolerance.

Typical idea:

```text
Replication factor = 3

Copy 1 -> Rack 1
Copy 2 -> Rack 1 or Rack 2
Copy 3 -> Rack 2
```

The exact policy depends on Hadoop configuration.

## Data Locality

Data locality means processing data where it is stored.

Question:

Should data go to code, or code go to data?

In Big Data, data is huge and code is small. So code usually goes to data.

```text
Block stored on DN2
    |
    v
Run mapper on DN2 if resources are available
```

Why it matters:

- Reduces network traffic.
- Improves performance.
- Uses cluster resources efficiently.

This is a core Hadoop design principle.

## HDFS Vs Local File System

| Point | Local File System | HDFS |
|---|---|---|
| Storage | One machine | Multiple machines |
| Scale | Limited by one machine | Scales by adding nodes |
| Fault tolerance | Usually no block replication | Replication across DataNodes |
| Processing | Local only | Supports distributed processing |
| File access | Normal OS commands | `hadoop fs` / `hdfs dfs` commands |
| Best for | Small/local files | Large distributed datasets |

## HDFS Vs Cloud Object Storage

Cloud alternatives:

- AWS S3.
- Azure ADLS Gen2.
- Google Cloud Storage.

HDFS is a distributed file system.

S3 and ADLS Gen2 are object storage systems.

Object storage stores data as objects:

```text
Object
  |
  |-- id/key
  |-- value/content
  |-- metadata
```

Comparison:

| Feature | HDFS | S3 / ADLS Gen2 |
|---|---|---|
| Storage type | Distributed file system | Object storage |
| Data layout | Blocks | Objects |
| Coupling | Storage tied to cluster compute | Storage separated from compute |
| Persistence | Depends on cluster lifecycle | Persistent independent storage |
| Access | Usually one Hadoop cluster | Many clusters/services can access |
| Scaling | Add DataNodes | Provider-managed scale |
| Best fit | On-prem Hadoop | Cloud data lake |

Important:

In Hadoop, storage and compute are tightly coupled. If you add DataNodes for storage, you also add compute. In cloud object storage, storage and compute are decoupled. Many Spark, Databricks, Athena, or Redshift clusters can read the same data.

## Practice Lab Setup Concepts

There are three ways to practice Hadoop:

### 1. Manual Installation

Install Hadoop, Hive, Sqoop, Spark, Kafka, and related services manually.

Good for:

- Admin-level learning.

Not ideal for:

- Beginners.
- Fast course practice.

### 2. Pseudo Distributed Setup

Example:

- Cloudera QuickStart VM.
- Oracle VirtualBox.

NameNode and DataNode run on the same VM.

Good for:

- Basic command practice.

Limitations:

- Slow on personal laptop.
- Does not feel like production.
- NameNode and DataNode are not truly distributed.

### 3. Fully Distributed Multi-Node Cluster

This is closest to production.

Users log in through a gateway node, also called edge node.

```text
User laptop
    |
    v
Gateway / Edge Node
    |
    v
Hadoop Cluster
```

Gateway node is an entry point. It is used to run commands and submit jobs, but it may not store HDFS blocks itself.

Security note:

Do not commit real lab usernames or passwords into Git notes. Use placeholders like:

```bash
ssh <username>@<gateway-host>
```

## HDFS Vs Non-HDFS Space

Example from lab:

```text
3 DataNodes
Each DataNode has 10 TB disk

Per DataNode:
    9 TB for HDFS
    1 TB for local/non-HDFS

Total raw disk:
    30 TB

HDFS area:
    27 TB

Local/non-HDFS area:
    3 TB
```

Important:

Raw HDFS capacity is not the same as usable capacity.

If replication factor is 3:

```text
Raw HDFS = 27 TB
Replication factor = 3
Usable logical data capacity is roughly 9 TB
```

This is because every block is stored three times.

## Common Mistakes

- Thinking NameNode stores actual data.
- Thinking Secondary NameNode is a full backup NameNode.
- Setting block size too small and overloading NameNode.
- Ignoring small files problem.
- Forgetting replication reduces usable capacity.
- Confusing local path `/home/user` with HDFS path `/user/user`.
- Thinking HDFS and S3 behave the same way.
- Assuming all HDFS commands read DataNodes. Metadata commands like `ls` mainly talk to NameNode.

## Best Practices

- Store large files in HDFS, not many tiny files.
- Use sensible block size.
- Use replication factor based on fault tolerance and cost.
- Enable NameNode HA in production.
- Configure rack awareness.
- Monitor NameNode heap memory.
- Monitor under-replicated blocks.
- Avoid direct manual changes on DataNode storage directories.
- Use HDFS commands instead of trying to access raw block files.

## Performance Tips

- Larger files are better than many small files.
- Block size affects parallelism and metadata.
- Data locality improves performance.
- Compression reduces storage and network cost.
- Choose file formats like Parquet/ORC for analytics.
- Monitor NameNode memory because metadata lives in memory.

## Interview Questions

### Beginner Questions

- What is HDFS?
- What is NameNode?
- What is DataNode?
- What is HDFS block size?
- What is replication factor?
- How does a client read a file from HDFS?

### Intermediate Questions

- Why is HDFS block size 128 MB?
- What happens if block size is very small?
- What happens if a DataNode fails?
- What is rack awareness?
- What is data locality?
- What is the difference between local file system and HDFS?
- What is the difference between HDFS and S3?

### Senior Data Engineer Questions

- How do you solve the HDFS small files problem?
- How do you design HDFS for high availability?
- How do you estimate usable HDFS capacity?
- How does NameNode federation improve scalability?
- How would you migrate HDFS data to cloud object storage?

## Scenario-Based Questions

### Scenario 1: NameNode Memory Pressure

Your NameNode memory is increasing and cluster becomes slow. What do you check?

Answer points:

- Number of files.
- Number of blocks.
- Small files problem.
- NameNode heap size.
- Metadata growth.
- Whether compaction is needed.
- Whether NameNode federation is needed.

### Scenario 2: DataNode Failure

A DataNode goes down. What happens?

Answer points:

- Heartbeat missing.
- NameNode marks DataNode dead.
- Blocks from that node become under-replicated.
- HDFS creates new replicas on healthy DataNodes.
- Jobs may retry failed tasks.

## Quick Revision

```text
HDFS = Hadoop Distributed File System
NameNode = metadata
DataNode = actual blocks
Block size = commonly 128 MB
Replication factor = commonly 3
Data locality = code goes to data
NameNode federation = scalability
NameNode HA = failover
```
