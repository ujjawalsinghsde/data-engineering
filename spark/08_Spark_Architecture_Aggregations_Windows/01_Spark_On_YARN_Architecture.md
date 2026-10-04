# Spark On YARN Architecture

## Introduction

Spark can run on different cluster managers.

A cluster manager is the system that gives resources to Spark applications.

Common cluster managers:

- YARN
- Kubernetes
- Spark Standalone
- Mesos, older and less common now

In Hadoop-based environments, Spark commonly runs on YARN.

YARN means:

```text
Yet Another Resource Negotiator
```

Its job is to manage cluster resources like CPU and memory.

## Hadoop Components Recap

Hadoop has three important parts:

```text
HDFS       -> Distributed storage
MapReduce  -> Older distributed processing engine
YARN       -> Resource manager
```

HDFS components:

```text
NameNode  -> master for metadata
DataNode  -> workers that store actual blocks
```

YARN components:

```text
ResourceManager -> master for resource allocation
NodeManager     -> worker-side resource manager
Container       -> allocated bundle of CPU and memory
```

## Why YARN Is Needed

In a cluster, many users and applications may run at the same time.

Examples:

- Spark job from data engineering team
- Hive query from analytics team
- MapReduce job from legacy system
- PySpark notebook from a developer

All these jobs need:

- CPU cores
- memory
- containers

YARN decides who gets what.

Analogy:

YARN is like an operating system for the cluster.

Just like a laptop OS manages CPU and memory for applications, YARN manages CPU and memory across cluster nodes.

## YARN Architecture

```text
                    Client / Gateway Node
                             |
                             v
                    ResourceManager
                    /      |       \
                   v       v        v
          NodeManager  NodeManager  NodeManager
              |            |            |
          Container    Container    Container
```

## ResourceManager

ResourceManager is the master service in YARN.

It:

- receives application requests
- decides where containers can run
- tracks available cluster resources
- coordinates with NodeManagers
- applies scheduler rules

If a user submits a job, the request first goes to ResourceManager.

## NodeManager

NodeManager runs on every worker node.

It:

- manages containers on that node
- reports node health
- reports available memory and CPU
- starts/stops containers
- sends heartbeats to ResourceManager

ResourceManager controls the whole cluster.

NodeManager controls one worker node.

## Container

A container is a bundle of resources.

Example:

```text
2 GB memory
1 vCore
```

Another example:

```text
8 GB memory
4 vCores
```

Spark executors and ApplicationMaster run inside containers.

## What Happens When A Hadoop Job Is Submitted?

Example:

```bash
hadoop jar my_program.jar
```

Flow:

```text
Client
  |
  v
ResourceManager
  |
  v
NodeManager creates first container
  |
  v
ApplicationMaster starts
  |
  v
ApplicationMaster asks ResourceManager for more containers
  |
  v
NodeManagers start containers
  |
  v
Tasks run
```

## ApplicationMaster

ApplicationMaster is a per-application manager.

Every application gets its own ApplicationMaster.

If 20 applications are running, there can be 20 ApplicationMasters.

ApplicationMaster:

- manages one application
- negotiates resources with ResourceManager
- coordinates task execution
- talks to NameNode to understand data locations
- tries to use data locality

## Data Locality

Data locality means:

```text
move compute near data instead of moving data near compute
```

If HDFS block is on Worker Node 2, it is better to run the task on Worker Node 2.

This reduces network transfer.

ApplicationMaster can use block location information from NameNode to request containers near the data.

## Uber Mode

Uber mode is a YARN optimization for very small jobs.

Normally:

```text
ApplicationMaster container
      |
      +-- asks for task containers
```

In Uber mode:

```text
ApplicationMaster container itself runs the job
```

This avoids creating extra containers.

Use case:

- very small job
- small input
- few tasks
- overhead of launching separate containers is bigger than the work itself

When not suitable:

- large datasets
- many map/reduce tasks
- memory-heavy processing
- long-running jobs

Interview explanation:

Uber mode reduces overhead for small jobs by running tasks inside the ApplicationMaster container.

## Spark On YARN

When Spark runs on YARN:

- YARN allocates resources.
- Spark driver coordinates the Spark application.
- Spark executors run tasks.

Spark application:

```text
Driver
  |
  +-- Executor 1
  +-- Executor 2
  +-- Executor 3
```

YARN view:

```text
ResourceManager
  |
  +-- ApplicationMaster / Driver container
  +-- Executor container
  +-- Executor container
  +-- Executor container
```

## Driver

Every Spark application has one driver.

Driver:

- runs the main program
- creates SparkSession/SparkContext
- builds logical and physical plans
- creates jobs, stages, tasks
- talks to cluster manager
- schedules tasks on executors
- collects metadata/results

If the driver dies, the Spark application usually fails.

## Executors

Executors are worker-side processes.

They:

- run Spark tasks
- process data partitions
- store cached data
- write shuffle data
- report status to driver

Executor resources are defined using:

- executor memory
- executor cores
- number of executors

## Spark On YARN Modes

Spark can run on YARN in two main deploy modes:

1. Client mode
2. Cluster mode

## Client Mode

In client mode, the driver runs on the client/gateway node.

Diagram:

```text
Gateway Node
   |
   +-- Driver
          |
          v
YARN Cluster
   |
   +-- Executors
```

Used for:

- notebooks
- PySpark shell
- interactive debugging
- development

Problem:

If gateway node disconnects or crashes, the driver is gone and the job can fail.

## Cluster Mode

In cluster mode, the driver runs inside the YARN cluster.

Diagram:

```text
Gateway Node
   |
   +-- submits job
          |
          v
YARN Cluster
   |
   +-- Driver
   +-- Executors
```

Used for:

- production jobs
- scheduled pipelines
- long-running batch jobs
- `spark-submit`

Advantage:

Even if I log out from gateway node, the job can continue running because the driver is inside the cluster.

## Interactive Mode Vs Submit Mode

Interactive mode:

- Jupyter notebook
- PySpark shell
- Spark shell
- driver usually on gateway/client
- good for development

Submit mode:

```bash
spark-submit --master yarn --deploy-mode cluster app.py
```

- packaged code submitted to cluster
- driver can run inside cluster
- good for production

## ResourceManager UI

YARN ResourceManager UI shows:

- running applications
- completed applications
- application state
- memory usage
- vCore usage
- queue information
- logs links

Example details I may see:

```text
Total Memory
Total VCores
Used Memory
Used VCores
Application ID
Application State
Queue
```

## vCores

vCore means virtual core.

It is YARN's CPU resource unit.

Example:

```text
Cluster total = 90 vCores
Cluster memory = 151 GB
```

If one executor asks for:

```text
2 vCores
4 GB memory
```

YARN checks whether enough resources are available in the target queue.

## Scheduler And Queues

YARN can divide resources between teams using scheduler queues.

Example:

```text
Total cluster resources = 100%

sales queue     = 60%
marketing queue = 40%
```

This prevents one team from taking the whole cluster.

Common scheduler:

```text
Capacity Scheduler
```

It guarantees capacity to queues while allowing resource sharing if configured.

## Minimum And Maximum Container Size

YARN may have min/max allocation rules.

Example:

```text
Minimum allocation: <memory:1024, vCores:1>
Maximum allocation: <memory:8192, vCores:4>
```

If I request less than minimum, YARN rounds up.

If I request more than maximum, allocation may fail or be capped depending on configuration.

## Spark UI Vs ResourceManager UI

| UI | Main Use |
|---|---|
| ResourceManager UI | cluster/application resource tracking |
| Spark UI | Spark jobs, stages, tasks, SQL plans, storage/cache |

Use ResourceManager UI to answer:

- Is my app running?
- How many resources did it get?
- Which queue is it using?
- Did it finish or fail?

Use Spark UI to answer:

- Which stage is slow?
- How much shuffle happened?
- Are tasks skewed?
- Is data cached?
- Which SQL query ran?

## Production Perspective

In production, Spark on YARN jobs are usually submitted through:

- Airflow
- Oozie
- shell scripts
- enterprise schedulers
- ADF/Databricks jobs in cloud equivalents

Production jobs should use cluster deploy mode where possible.

Why?

- less dependency on gateway session
- better reliability
- logs are managed by cluster
- suitable for scheduled execution

## Common Mistakes

- Thinking YARN stores data. HDFS stores data; YARN manages resources.
- Confusing ResourceManager with NameNode.
- Running production jobs in client mode.
- Not checking ResourceManager UI when application is stuck.
- Requesting more executor memory/cores than YARN maximum allows.
- Forgetting that driver failure usually kills the Spark application.

## Best Practices

- Use client mode for development.
- Use cluster mode for production.
- Check ResourceManager UI for resource allocation.
- Check Spark UI for job execution details.
- Use queues properly in shared clusters.
- Size executors based on cluster limits.
- Use meaningful Spark application names.

## Interview Questions

### Beginner Questions

- What is YARN?
- What is ResourceManager?
- What is NodeManager?
- What is a container?
- What is the Spark driver?

### Intermediate Questions

- Explain what happens when a Spark job is submitted on YARN.
- What is ApplicationMaster?
- What is data locality?
- What is the difference between client mode and cluster mode?
- Why is cluster mode preferred for production?

### Senior Data Engineer Questions

- How would you troubleshoot a Spark job stuck in ACCEPTED state on YARN?
- How do YARN queues affect Spark applications?
- How do driver placement and deploy mode affect reliability?
- How would you size executors when YARN has max container limits?
- How does dynamic allocation interact with YARN?

## Scenario-Based Questions

### Scenario 1: Gateway Node Crashes

Your Spark job was running from a notebook and the gateway node crashed.

Likely result:

The job fails because driver was running on gateway node in client mode.

Production fix:

Use `spark-submit` with cluster deploy mode.

### Scenario 2: Job Stuck In ACCEPTED State

Possible reasons:

- no resources available
- queue capacity full
- requested executor size too large
- user queue limit reached
- cluster unhealthy

Debug:

- check ResourceManager UI
- check queue usage
- check requested memory/vCores
- check application logs

## Quick Revision

- HDFS stores data.
- YARN manages resources.
- ResourceManager is YARN master.
- NodeManager runs on worker nodes.
- Container = memory + vCores.
- Spark driver coordinates application.
- Executors run tasks.
- Client mode driver runs on gateway.
- Cluster mode driver runs inside cluster.
- Cluster mode is better for production.
