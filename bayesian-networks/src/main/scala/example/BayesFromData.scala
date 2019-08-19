package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import dk.bayes.dsl.variable.Categorical
import dk.bayes.dsl.infer

object BayesFromData {

  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.format("csv").option("header", "true").load("files/lucas.csv")

  def generateCombinations[T](x: List[List[T]]): List[List[T]] = {
    x match {
      case Nil    => List(Nil)
      case h :: _ => h.flatMap(i => generateCombinations(x.tail).map(i :: _))
    }
  }

  def buildCpt(target: String, parents: List[String]) = {

    val s =
      df.groupBy(target, parents: _*).count().withColumn("prob", col("count") / df.count).persist()

    s.show

    val columns = parents :+ target match {
      case l :: rest if l == target => List(l)
      case c                        => c
    }
    val eventCombinations = generateCombinations(List.fill(columns.length)(List(0, 1)))

    eventCombinations.foldLeft(Vector[Double]())((acc, elem) => {

      val zipped = columns.zip(elem)

      val q = s.filter(
        zipped
          .map(tup => {
            val (column, value) = tup
            s"${column}==${value}"
          })
          .mkString(" AND ")
      )

      val prob: Double = q.select(col("prob")).collect() match {
        case Array(row) =>
          row.get(0) match {
            case value: Double => value
            case _             => 0
          }
        case _ => 0
      }

      acc :+ prob
    })

  }

  def singleCpt(variable: String) = {
    buildCpt(variable, List(variable))
  }

  def run() = {
    // Exogenous variables
    val anxiety = Categorical(singleCpt("Anxiety"))
    val pp = Categorical(singleCpt("Peer_pressure"))
    val allergy = Categorical(singleCpt("Allergy"))
    val genetics = Categorical(singleCpt("Genetics"))

    // Endogenous variables
    val smoking = Categorical(anxiety, pp, buildCpt("Smoking", List("Anxiety", "Peer_pressure")))
    val yellowFingers = Categorical(smoking, buildCpt("Yellow_Fingers", List("Smoking")))
    val lungCancer =
      Categorical(smoking, genetics, buildCpt("Lung_cancer", List("Smoking", "Genetics")))
    val coughing =
      Categorical(allergy, lungCancer, buildCpt("Coughing", List("Allergy", "Lung_cancer")))
    val fatigue =
      Categorical(coughing, lungCancer, buildCpt("Fatigue", List("Coughing", "Lung_cancer")))
    val attentionDisorder =
      Categorical(genetics, buildCpt("Attention_Disorder", List("Genetics")))
    val carAccident = Categorical(
      fatigue,
      attentionDisorder,
      buildCpt("Car_Accident", List("Fatigue", "Attention_Disorder"))
    )

    println("Base")
    println(infer(smoking).cpd)

    println("With value 0")
    println(infer(smoking).cpd)

    println("With value 1")
    println(infer(smoking).cpd)

  }
}
