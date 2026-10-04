# Big Data Engineer Notes

This repository contains my personal Big Data Engineering study notes.

The notes are written for:

- learning concepts from basics to advanced
- quick revision before interviews
- Senior Data Engineer interview preparation
- practical PySpark, Hadoop, HDFS, Spark SQL, and data pipeline understanding

The structure is numbered by topic so it is easy to follow in order.

## Contents

## 01_Big_Data_Fundamentals

- [01_Big_Data_And_Hadoop_Overview.md](01_Big_Data_Fundamentals/01_Big_Data_And_Hadoop_Overview.md)

Covers Big Data fundamentals, the 5 Vs, monolithic vs distributed systems, Hadoop overview, HDFS/MapReduce/YARN, cloud advantages, Spark introduction, data lakes, data warehouses, serving layers, and the role of a Data Engineer.

## 02_Distributed_Storage_Data_Lake

- [01_HDFS_Deep_Dive.md](02_Distributed_Storage_Data_Lake/01_HDFS_Deep_Dive.md)
- [02_Linux_Commands_For_Data_Engineers.md](02_Distributed_Storage_Data_Lake/02_Linux_Commands_For_Data_Engineers.md)
- [03_HDFS_Commands.md](02_Distributed_Storage_Data_Lake/03_HDFS_Commands.md)
- [04_MapReduce_Distributed_Computing.md](02_Distributed_Storage_Data_Lake/04_MapReduce_Distributed_Computing.md)
- [05_Assignment_Walkthrough.md](02_Distributed_Storage_Data_Lake/05_Assignment_Walkthrough.md)

Covers HDFS architecture, NameNode/DataNode, block size, replication, rack awareness, HDFS vs cloud object storage, Linux commands, HDFS commands, MapReduce basics, and assignment-style file movement workflows.

## 03_Distributed_Processing_Spark

- [01_MapReduce_Deep_Dive.md](03_Distributed_Processing_Spark/01_MapReduce_Deep_Dive.md)
- [02_Apache_Spark_RDDs_And_PySpark.md](03_Distributed_Processing_Spark/02_Apache_Spark_RDDs_And_PySpark.md)
- [03_Lab_And_Assignment_Guide.md](03_Distributed_Processing_Spark/03_Lab_And_Assignment_Guide.md)

Covers MapReduce internals, mapper/reducer logic, shuffle, sort, partitioners, combiners, Spark introduction, RDDs, transformations vs actions, lazy evaluation, DAG, SparkSession basics, PySpark word count, and lab/assignment practice.

## 04_Apache_Spark_Core_RDD

- [01_RDD_Transformations_And_Actions.md](04_Apache_Spark_Core_RDD/01_RDD_Transformations_And_Actions.md)
- [02_Spark_Performance_Optimization.md](04_Apache_Spark_Core_RDD/02_Spark_Performance_Optimization.md)
- [03_Joins_Broadcast_And_Practical_Patterns.md](04_Apache_Spark_Core_RDD/03_Joins_Broadcast_And_Practical_Patterns.md)
- [04_Assignment_Solutions_RDD.md](04_Apache_Spark_Core_RDD/04_Assignment_Solutions_RDD.md)

Covers Spark Core RDD operations, lambda functions, higher-order functions, `map`, `flatMap`, `filter`, `reduce`, `reduceByKey`, `groupByKey`, narrow vs wide transformations, Spark jobs/stages/tasks, joins, broadcast joins, repartition vs coalesce, caching, and RDD assignment solutions.

## 05_DataFrames_Spark_SQL

- [01_DataFrames_And_Spark_SQL_Fundamentals.md](05_DataFrames_Spark_SQL/01_DataFrames_And_Spark_SQL_Fundamentals.md)
- [02_Spark_SQL_Managed_And_External_Tables.md](05_DataFrames_Spark_SQL/02_Spark_SQL_Managed_And_External_Tables.md)
- [03_DataFrame_And_SQL_Use_Cases.md](05_DataFrames_Spark_SQL/03_DataFrame_And_SQL_Use_Cases.md)
- [04_Optimization_And_Assignment.md](05_DataFrames_Spark_SQL/04_Optimization_And_Assignment.md)

Covers DataFrames, Spark SQL, SparkSession, DataFrame readers, CSV/JSON/Parquet/ORC, temp views, managed tables, external tables, DataFrame API vs SQL API, use cases on retail datasets, assignment solutions, and executor/resource optimization basics.

## 06_PySpark_DataFrames_Advanced

- [01_Schema_Management_Read_Modes_And_Dates.md](06_PySpark_DataFrames_Advanced/01_Schema_Management_Read_Modes_And_Dates.md)
- [02_DataFrame_Creation_Patterns.md](06_PySpark_DataFrames_Advanced/02_DataFrame_Creation_Patterns.md)
- [03_DataFrame_Transformations_And_Deduplication.md](06_PySpark_DataFrames_Advanced/03_DataFrame_Transformations_And_Deduplication.md)
- [04_SparkSession_Architecture_And_Assignment.md](06_PySpark_DataFrames_Advanced/04_SparkSession_Architecture_And_Assignment.md)

Covers schema inference vs schema enforcement, DDL schemas, `StructType`, nested schemas, `ArrayType`, date parsing, read modes, DataFrame creation patterns, RDD to DataFrame conversion, `withColumn`, `drop`, `select`, `selectExpr`, duplicate handling, SparkSession architecture, client vs cluster mode, and assignment solutions.

## 07_Spark_Caching_And_Persist

- [01_Cache_And_Persist_Fundamentals.md](07_Spark_Caching_And_Persist/01_Cache_And_Persist_Fundamentals.md)
- [02_Spark_UI_Table_Cache_And_Catalog.md](07_Spark_Caching_And_Persist/02_Spark_UI_Table_Cache_And_Catalog.md)
- [03_Persist_Storage_Levels.md](07_Spark_Caching_And_Persist/03_Persist_Storage_Levels.md)
- [04_Assignment_Caching_Strategies.md](07_Spark_Caching_And_Persist/04_Assignment_Caching_Strategies.md)

Covers Spark cache and persist, lazy cache materialization, Spark UI Storage tab, History Server vs Spark UI, table caching, cache invalidation, `refresh table`, `spark.catalog` cache APIs, persist storage levels, serialized vs deserialized storage, and caching strategy assignments.

## 08_Spark_Architecture_Aggregations_Windows

- [01_Spark_On_YARN_Architecture.md](08_Spark_Architecture_Aggregations_Windows/01_Spark_On_YARN_Architecture.md)
- [02_Columns_And_Aggregations.md](08_Spark_Architecture_Aggregations_Windows/02_Columns_And_Aggregations.md)
- [03_Window_Functions_Rank_Lead_Lag.md](08_Spark_Architecture_Aggregations_Windows/03_Window_Functions_Rank_Lead_Lag.md)
- [04_Log_Analysis_Pivot_And_Nulls.md](08_Spark_Architecture_Aggregations_Windows/04_Log_Analysis_Pivot_And_Nulls.md)

Covers Spark on YARN architecture, ResourceManager, NodeManager, ApplicationMaster, client vs cluster mode, column access methods in PySpark, simple and grouping aggregations, window functions, rank/dense rank/row number, lead/lag, log file analysis, pivot tables, and null handling.

## 09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit

- [01_DataFrame_Writer_Partitioning_And_Bucketing.md](09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit/01_DataFrame_Writer_Partitioning_And_Bucketing.md)
- [02_Spark_Internals_Partitions_And_Parallelism.md](09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit/02_Spark_Internals_Partitions_And_Parallelism.md)
- [03_Spark_Submit_And_Resource_Configuration.md](09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit/03_Spark_Submit_And_Resource_Configuration.md)
- [04_Assignment_Solutions_And_Practical_Guide.md](09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit/04_Assignment_Solutions_And_Practical_Guide.md)

Covers DataFrame Writer API, write modes, file formats, partitionBy, partition pruning, bucketing, Spark internals, jobs/stages/tasks, initial partitions, shuffle partitions, small files, splittable vs non-splittable files, spark-submit, resource configuration, client/cluster deploy mode, and assignment solutions.

## 10_Spark_Joins_AQE_And_Skew_Optimization

- [01_GroupBy_Shuffle_And_AQE.md](10_Spark_Joins_AQE_And_Skew_Optimization/01_GroupBy_Shuffle_And_AQE.md)
- [02_Join_Types_And_Strategies.md](10_Spark_Joins_AQE_And_Skew_Optimization/02_Join_Types_And_Strategies.md)
- [03_Partition_Skew_And_Large_Table_Join_Optimization.md](10_Spark_Joins_AQE_And_Skew_Optimization/03_Partition_Skew_And_Large_Table_Join_Optimization.md)
- [04_Assignment_Joins_AQE_And_Practical_Guide.md](10_Spark_Joins_AQE_And_Skew_Optimization/04_Assignment_Joins_AQE_And_Practical_Guide.md)

Covers how `groupBy` works internally, shuffle partitions, Adaptive Query Execution, broadcast joins, shuffle sort merge joins, shuffle hash joins, join types, partition skew, salting, bucketing for large joins, and practical assignment-style join optimization experiments.

## 11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution

- [01_Spark_Memory_Management.md](11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution/01_Spark_Memory_Management.md)
- [02_Sort_Aggregate_Hash_Aggregate_And_Plans.md](11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution/02_Sort_Aggregate_Hash_Aggregate_And_Plans.md)
- [03_File_Formats_And_Compression.md](11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution/03_File_Formats_And_Compression.md)
- [04_Schema_Evolution.md](11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution/04_Schema_Evolution.md)
- [05_Assignment_Performance_And_Storage_Guide.md](11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution/05_Assignment_Performance_And_Storage_Guide.md)

Covers Spark executor memory management, heap and overhead memory, storage vs execution memory, off-heap and PySpark memory, Hash Aggregate vs Sort Aggregate, parsed/analyzed/optimized/physical plans, Catalyst Optimizer, file formats, compression techniques, Parquet internals, predicate pushdown, column pruning, schema evolution, and assignment-style performance experiments.

## 12_Spark_Project_LendingClub_Data_Cleaning

- [01_Project_Framing_And_Interview_Story.md](12_Spark_Project_LendingClub_Data_Cleaning/01_Project_Framing_And_Interview_Story.md)
- [02_Agile_CICD_Testing_And_Production_Process.md](12_Spark_Project_LendingClub_Data_Cleaning/02_Agile_CICD_Testing_And_Production_Process.md)
- [03_LendingClub_Project_Architecture_And_Dataset_Preparation.md](12_Spark_Project_LendingClub_Data_Cleaning/03_LendingClub_Project_Architecture_And_Dataset_Preparation.md)
- [04_Data_Cleaning_Customers_Loans_Repayments_Defaulters.md](12_Spark_Project_LendingClub_Data_Cleaning/04_Data_Cleaning_Customers_Loans_Repayments_Defaulters.md)
- [05_Project_Interview_QA_And_Production_Checklist.md](12_Spark_Project_LendingClub_Data_Cleaning/05_Project_Interview_QA_And_Production_Checklist.md)

Covers realistic Spark project explanation, domain project ideas, Agile delivery, Scrum ceremonies, CI/CD, unit testing, logging, SCD concepts, LendingClub project architecture, borrower/loan/repayment/defaulter dataset preparation, data cleaning rules, PySpark implementation, production improvements, and project interview Q&A.

## 13_PySpark_Project_Productionization_Testing_And_Logging

- [01_LendingClub_External_Tables_Views_And_Loan_Score.md](13_PySpark_Project_Productionization_Testing_And_Logging/01_LendingClub_External_Tables_Views_And_Loan_Score.md)
- [02_Project_Structure_Config_And_Parameterization.md](13_PySpark_Project_Productionization_Testing_And_Logging/02_Project_Structure_Config_And_Parameterization.md)
- [03_Local_Setup_Pipenv_Pyenv_And_IDE.md](13_PySpark_Project_Productionization_Testing_And_Logging/03_Local_Setup_Pipenv_Pyenv_And_IDE.md)
- [04_Unit_Testing_With_Pytest.md](13_PySpark_Project_Productionization_Testing_And_Logging/04_Unit_Testing_With_Pytest.md)
- [05_Logging_With_Log4j_In_PySpark.md](13_PySpark_Project_Productionization_Testing_And_Logging/05_Logging_With_Log4j_In_PySpark.md)

Covers LendingClub external tables, consolidated views, precomputed tables, loan score calculation, bad member ID handling, PySpark project structure, config-driven development, local Java/Python/PySpark setup, pipenv, pyenv, pytest fixtures, parameterized tests, markers, and Log4j logging in PySpark applications.

## How To Use These Notes

Suggested revision order:

1. Start with `01_Big_Data_Fundamentals`.
2. Learn storage concepts in `02_Distributed_Storage_Data_Lake`.
3. Understand distributed processing in `03_Distributed_Processing_Spark`.
4. Practice Spark Core RDD concepts in `04_Apache_Spark_Core_RDD`.
5. Move to higher-level APIs in `05_DataFrames_Spark_SQL`.
6. Deepen PySpark DataFrame skills in `06_PySpark_DataFrames_Advanced`.
7. Study Spark caching and persistence in `07_Spark_Caching_And_Persist`.
8. Learn Spark architecture, aggregations, windows, pivots, and null handling in `08_Spark_Architecture_Aggregations_Windows`.
9. Study DataFrame writing, partitioning, bucketing, Spark internals, and spark-submit in `09_DataFrame_Writer_Partitioning_Bucketing_SparkSubmit`.
10. Learn Spark joins, AQE, skew handling, and large-table join optimization in `10_Spark_Joins_AQE_And_Skew_Optimization`.
11. Study Spark memory management, execution plans, file formats, compression, and schema evolution in `11_Spark_Memory_Plans_FileFormats_And_Schema_Evolution`.
12. Practice end-to-end Spark project explanation and LendingClub data cleaning in `12_Spark_Project_LendingClub_Data_Cleaning`.
13. Productionize PySpark projects with tables, views, scoring logic, config management, local setup, unit testing, and logging in `13_PySpark_Project_Productionization_Testing_And_Logging`.

Each topic includes explanations, code examples, production notes, best practices, common mistakes, performance tips, and interview questions.
