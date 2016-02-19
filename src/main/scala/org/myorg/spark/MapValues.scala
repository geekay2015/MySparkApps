package org.myorg.spark

import org.apache.spark.{SparkContext, SparkConf}


/*
* When we use map() with a Pair RDD, we get access to both Key & value.
* There are times we might only be interested in accessing the value(& not key).
* In those case, we can use mapValues() instead of map().
*
* In this example we use mapValues() along with reduceByKey() to calculate average for each subject
*
*/

object MapValues extends App{
  //Define the Spark Configuration
  val conf = new SparkConf().setAppName("BasicMap").setMaster("local")

  //Define the Spark Context
  val sc = new SparkContext(conf)

  val inputRDD = sc.parallelize(Seq(
    ("maths", 50),
    ("maths", 60),
    ("english", 65),
    ("english", 75)
  )
  )

  val mapped = inputRDD.mapValues(mark => (mark, 1))

  val reduced = mapped.reduceByKey((x,y) => (x._1 +  y._1, x._2 + y._2))

  val average = reduced.map{ x =>
    val tmp = x._2
    val total = tmp._1
    val count = tmp._2
    (x._1, total/count)
}
  println(average.collect().mkString(", "))

  //Stop the Spark Context
  sc.stop()

}
