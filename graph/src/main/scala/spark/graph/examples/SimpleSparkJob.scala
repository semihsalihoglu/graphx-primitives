package spark.graph.examples

import spark.SparkContext
import SparkContext._

object SimpleGraphJob {
  def main(args: Array[String]) {
    val logFile = "/Users/semihsalihoglu/Desktop/TODO.rtf" // Should be some file on your system
    val sc = new SparkContext("local", "Simple Job")
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}