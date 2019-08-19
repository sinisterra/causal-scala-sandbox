package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import probability_monad._
import probability_monad.Distribution._

object BayesPGM {

  def run() {

    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load("files/lucas.csv")

    df.cache()

    def getValueForBooleanDistribution(column: String): Double = {

      val t = df
        .groupBy(column)
        .count()
        .where(col(column) === 1)
        .withColumn("prob", col("count") / df.count)
        .select("prob")

      t.cache()

      t.collect()(0)(0) match {
        case v: Double => v
        case _         => 0
      }
    }

    val map_cache: scala.collection.mutable.Map[(String, List[String]), DataFrame] =
      scala.collection.mutable.Map()

    val cp_cache: scala.collection.mutable.Map[(String, List[String]), Double] =
      scala.collection.mutable.Map()

    def buildConditionalProbabilityTable(target: String, parents: List[String]): DataFrame = {
      val cached_df = map_cache.get((target, parents))

      cached_df match {
        case Some(fr: DataFrame) => fr
        case None => {
          val first :: rest = parents
          val frame = df
            .where(col(target) === 1)
            .groupBy(first, rest: _*)
            .count()
            .withColumn("prob", col("count") / df.count)

          map_cache += ((target, parents) -> frame)

          frame.cache()

          frame
        }
      }

    }

    // exogenous variables
    val anxiety: Distribution[Boolean] = tf(0.6427)
    val peerPressure: Distribution[Boolean] = tf(0.3299)
    val genetics: Distribution[Boolean] = tf(getValueForBooleanDistribution("Genetics"))
    val allergy: Distribution[Boolean] = tf(getValueForBooleanDistribution("Allergy"))
    val bornAnEvenDay: Distribution[Boolean] = tf(
      getValueForBooleanDistribution("Born_an_Even_Day")
    )

    def smoking(anxiety: Boolean, peerPressure: Boolean) = {

      val cpt: DataFrame =
        buildConditionalProbabilityTable("Smoking", List("Anxiety", "Peer_Pressure"))

      // tf(
      //   cpt
      //     .where(col("Anxiety") === anxiety)
      //     .where(col("Peer_pressure") === peerPressure)
      //     .select(col("prob"))
      //     .collect()(0)(0) match {
      //     case d: Double => d
      //     case _         => 0
      //   }
      // )
      (anxiety, peerPressure) match {
        case (true, true)   => tf(0.91)
        case (true, false)  => tf(0.8686)
        case (false, true)  => tf(0.7459)
        case (false, false) => tf(0.4311)
      }
    }

    def yellowFingers(smoking: Boolean) = {
      smoking match {
        case true  => tf(0.2)
        case false => tf(0.2)
      }
    }

    def lungCancer(smoking: Boolean, genetics: Boolean) = {
      (smoking, genetics) match {
        case (true, _)  => tf(0.2)
        case (false, _) => tf(0.2)
      }
    }

    def coughing(lungCancer: Boolean, allergy: Boolean) = {
      (lungCancer, allergy) match {
        case (true, _)  => tf(0.2)
        case (false, _) => tf(0.2)
      }
    }

    def fatigue(lungCancer: Boolean, coughing: Boolean) = {
      (lungCancer, coughing) match {
        case (true, _)  => tf(0.2)
        case (false, _) => tf(0.2)
      }
    }

    def carAccident(fatigue: Boolean, attentionDisorder: Boolean) = {
      (fatigue, attentionDisorder) match {
        case (true, _)  => tf(0.2)
        case (false, _) => tf(0.2)
      }
    }

    def attentionDisorder(genetics: Boolean) = {
      genetics match {
        case true  => tf(0.2)
        case false => tf(0.2)
      }
    }

    case class SmokingRisk(
      anxiety: Boolean,
      peerPressure: Boolean,
      smoking: Boolean,
      yellowFingers: Boolean,
      lungCancer: Boolean,
      genetics: Boolean,
      allergy: Boolean,
      coughing: Boolean,
      fatigue: Boolean,
      carAccident: Boolean,
      attentionDisorder: Boolean,
      bornAnEvenDay: Boolean
    )

    val smokingRisk: Distribution[SmokingRisk] = for {
      // exogenous variables
      a  <- anxiety
      p  <- peerPressure
      g  <- genetics
      al <- allergy
      b  <- bornAnEvenDay
      // endogenous variables
      s  <- smoking(a, p)
      y  <- yellowFingers(s)
      l  <- lungCancer(s, g)
      c  <- coughing(l, al)
      f  <- fatigue(l, c)
      ad <- attentionDisorder(g)
      ca <- carAccident(f, ad)
    } yield
      SmokingRisk(
        a,
        p,
        s,
        y,
        l,
        g,
        al,
        c,
        f,
        ca,
        ad,
        b
      )

    // average treatment effect
    // a1: !anxiety
    // a0: anxiety
    val ate = smokingRisk.pr(_.smoking) - smokingRisk.pr(!_.smoking)

    val att = smokingRisk.given(_.anxiety).pr(_.smoking) - smokingRisk
      .given(_.anxiety)
      .pr(!_.smoking)

    val atu = smokingRisk.given(!_.anxiety).pr(_.smoking) - smokingRisk
      .given(!_.anxiety)
      .pr(!_.smoking)

    println(("ATE", ate), ("ATT", att), ("ATU", atu))
  }
}
