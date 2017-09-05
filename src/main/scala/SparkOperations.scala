import java.io.{File, PrintWriter}
import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by knoldus on 9/4/17.
  */
object SparkOperations {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Assignment")

    val sparkContext = new SparkContext(sparkConf)

    val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()

    val pageCount = sparkSession.sparkContext.textFile("src/main/resources/pagecounts-20151201-220000")

    val pageCountWithEn = pageCount.filter(pageCount => pageCount.split(" ")(0).equals("en"))

    val maxRequestedPges = pageCount.map { pageCount =>
      val wordsArray = pageCount.split(" ")
      (wordsArray(1),wordsArray(2).toInt)
    }.reduceByKey(_ + _)
      .filter(request => request._2 > 200000).collect()

    println("PAGE COUNT IS : " + pageCount.count())

    println("PAGE COUNT WITH EN IS : " + pageCountWithEn.count())

    println("PAGE COUNT REQUESTED MORE THAN 200000 IS : " + maxRequestedPges.toList)

    val topTen = pageCount.take(10).toList

    val writer = new PrintWriter(new File("src/main/resources/topTen.txt"))

    writer.write(topTen.mkString("\n"))

    writer.close()
  }
}
