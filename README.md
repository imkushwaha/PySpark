## PySpark

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

- Resilient Distributed Datasets (RDD): It is a key tool for data computation. It enables you to recheck data in the event of a failure, and it acts as an interface for immutable data. It helps in recomputing data in case of failures, and it is a data structure. There are two methods for modifying RDDs: transformations and actions.

- Directed Acyclic Graph (DAG): The driver converts the program into a DAG for each job. The Apache Spark Eco-system includes various components such as the API core, Spark SQL, Streaming and real-time processing, MLIB, and Graph X. A sequence of connection between nodes is referred to as a driver. As a result, you can read volumes of data using the Spark shell. You can also use the Spark context -cancel, run a job, task (work), and job (computation) to stop a job.

### Spark Architecture

![image](https://github.com/imkushwaha/PySpark/assets/72372136/45149990-32ad-412b-9618-ad6ad992c16d)


More coming soon......
