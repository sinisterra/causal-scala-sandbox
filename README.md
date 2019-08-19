## Causal Scala Examples

[TOC]

### Bayesian networks with Spark + Scala

Some examples with bayesian networks are included inside the folder `bayesian-networks`.

#### Instructions

Check `Examples.scala` to run different scenarios. Each object in the class has a `run` function that can be called to run each model.

Running Spark-based examples such as `BayesFromData` requires to use spark-submit. A fat JAR needs to be built, then executed with `spark-submit`.

The following commands consider that `SPARK_HOME` belongs to your path. If the `SparkSession` is configured in local mode (host is `local[*]`) then you might not need to do this.

```
start-master.sh
start-slave.sh spark://<your_host_name>:7077
```

Then build the fat JAR and run it. INFO messages have been suppressed.

```
sbt assembly
spark-submit target/scala-2.11/bayesian-networks-assembly-0.1.0-SNAPSHOT.jar
```
