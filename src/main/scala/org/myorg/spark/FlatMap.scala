package org.myorg.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/*
* Sometimes we want to produce multiple output elements for each input element.
* The operation to do this is called flatMap().
* The function flatMap() is called individually for each element in our input RDD.
* Instead of returning a single element, we return an iterator with our return values.
* Rather than producing an RDD of iterators, we get back an RDD that consists of the elements from all of the iterators.
* A simple usage of flatMap() is splitting up an input string into words
*/

object FlatMap {
  def main(args: Array[String]) {

  //Define the Spark Configuration
  val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local")

  //Define the Spark Context
  val sc = new SparkContext(conf)

  //Define the SQL Context
  val sqlContext = new SQLContext(sc)

  //Load data from file
  val input = sc.textFile("/Users/gangadharkadam/myapps/MySparkApps/src/main/scala/org/myorg/spark/BasicMap.scala")

  //Split the line into words
  val words = input.flatMap(line => line.split(" "))
    .map(word => (word, 1))

  //reduce each key
  val counts = words.reduceByKey((x, y) => x + y)

  //write output data to disk
   counts.saveAsTextFile("out.txt")

  //stop the context
  sc.stop()
}


}
