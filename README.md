# PySpark

- PySpark is the Python API for Apache Spark. It enables you to perform real-time, large-scale data processing in a distributed environment using Python. 
- It also provides a PySpark shell for interactively analyzing your data.

#### PySpark supports all of Spark’s features such as Spark SQL, DataFrames, Structured Streaming, Machine Learning (MLlib) and Spark Core.

![image](https://github.com/imkushwaha/PySpark/assets/72372136/04fab381-e479-414d-b22f-28bc8489ac40)


## Apache Spark

- Apache Spark is a unified analytics engine for large-scale data processing. 
- It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general execution graphs. 
- It also supports a rich set of higher-level tools:
  - Spark SQL for SQL and structured data processing, 
  - pandas API on Spark for pandas workloads, 
  - MLlib for machine learning, 
  - GraphX for graph processing, and 
  - Structured Streaming for incremental computation and stream processing.
 
![image](https://github.com/imkushwaha/PySpark/assets/72372136/023cefce-11c2-4bee-8d4c-f711cb11cfde)


- Speed: Spark performs up to 100 times faster than MapReduce for processing large amounts of data. It is also able to divide the data into chunks in a controlled way.
- Powerful Caching: Powerful caching and disk persistence capabilities are offered by a simple programming layer.
- Deployment: Mesos, Hadoop via YARN, or Spark’s own cluster manager can all be used to deploy it.
- Real-Time: Because of its in-memory processing, it offers real-time computation and low latency.
- Polyglot: In addition to Java, Scala, Python, and R, Spark also supports all four of these languages. You can write Spark code in any one of these languages. Spark also provides a command-line interface in Scala and Python.


### Two Main Abstractions of Apache Spark

- **Resilient Distributed Datasets (RDD):** It is a key tool for data computation. It enables you to recheck data in the event of a failure, and it acts as an interface for immutable data. It helps in recomputing data in case of failures, and it is a data structure. There are two methods for modifying RDDs: transformations and actions.

- **Directed Acyclic Graph (DAG):** The driver converts the program into a DAG for each job. The Apache Spark Eco-system includes various components such as the API core, Spark SQL, Streaming and real-time processing, MLIB, and Graph X. A sequence of connection between nodes is referred to as a driver. As a result, you can read volumes of data using the Spark shell. You can also use the Spark context -cancel, run a job, task (work), and job (computation) to stop a job.

### Spark Architecture

![image](https://github.com/imkushwaha/PySpark/assets/72372136/45149990-32ad-412b-9618-ad6ad992c16d)


- When the Driver Program in the Apache Spark architecture executes, it calls the real program of an application and creates a SparkContext. 
- SparkContext contains all of the basic functions. 
- The Spark Driver includes several other components, including a DAG Scheduler, Task Scheduler, Backend Scheduler, and Block Manager, all of which are responsible for translating user-written code into jobs that are actually executed on the cluster.

- The Cluster Manager manages the execution of various jobs in the cluster. 
- Spark Driver works in conjunction with the Cluster Manager to control the execution of various other jobs. 
- The cluster Manager does the task of allocating resources for the job. Once the job has been broken down into smaller jobs, which are then distributed to worker nodes, SparkDriver will control the execution.
- Many worker nodes can be used to process an RDD created in the SparkContext, and the results can also be cached.
- The Spark Context receives task information from the Cluster Manager and enqueues it on worker nodes.
- The executor is in charge of carrying out these duties. 
- The lifespan of executors is the same as that of the Spark Application. We can increase the number of workers if we want to improve the performance of the system. In this way, we can divide jobs into more coherent parts.


### Spark Architecture Applications

#### Driver Program

- The Driver Program is a process that runs the main() function of the application and creates the SparkContext object. The purpose of SparkContext is to coordinate the spark applications, running as independent sets of processes on a cluster.
- To run on a cluster, the SparkContext connects to a different type of cluster managers and then perform the following tasks: -
  - It acquires executors on nodes in the cluster.
  - Then, it sends your application code to the executors. Here, the application code can be defined by JAR or Python files passed to the SparkContext.
  - At last, the SparkContext sends tasks to the executors to run.


#### Cluster Manager

- The role of the cluster manager is to allocate resources across applications. The Spark is capable enough of running on a large number of clusters.
- It consists of various types of cluster managers such as Hadoop YARN, Apache Mesos and Standalone Scheduler.
- Here, the Standalone Scheduler is a standalone spark cluster manager that facilitates to install Spark on an empty set of machines.

#### Worker Node

- The worker node is a slave node.
- Its role is to run the application code in the cluster.

#### Executor

- An executor is a process launched for an application on a worker node.
- It runs tasks and keeps data in memory or disk storage across them.
- It read and write data to the external sources.
- Every application contains its executor.

#### Task

- A unit of work that will be sent to one executor.


### Modes of Execution

- **Cluster mode:** Cluster mode is the most frequent way of running Spark Applications. In cluster mode, a user delivers a pre-compiled JAR, Python script, or R script to a cluster manager. Once the cluster manager receives the pre-compiled JAR, Python script, or R script, the driver process is launched on a worker node inside the cluster, in addition to the executor processes. This means that the cluster manager is in charge of all Spark application-related processes.

- **Client mode:** In contrast to cluster mode, where the Spark driver remains on the client machine that submitted the application, the Spark driver is removed in client mode and is therefore responsible for maintaining the Spark driver process on the client machine. These machines, usually referred to as gateway machines or edge nodes, are maintained on the client machine.

- **Local mode:** Local mode runs the entire Spark Application on a single machine, as opposed to the previous two modes, which parallelized the Spark Application through threads on that machine. As a result, the local mode uses threads instead of parallelized threads. This is a common way to experiment with Spark, try out your applications, or experiment iteratively without having to make any changes on Spark’s end.


### Cluster Manager Types

- Standalone
- Apache Mesos
- Hadoop YARN


