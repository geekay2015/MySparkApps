package org.myorg.spark

import org.apache.spark.{SparkContext, SparkConf}

/*
** The map() transformation takes in a function and applies it to each element in the RDD
** with the result of the function being the new value of each element in the resulting RDD.
** It is useful to note that map()’ s return type does not have to be the same as its input type,
** so if we had an RDD String and our map() function were to parse the strings and return a Double,
** our input RDD type would be RDD[ String] and the resulting RDD type would be
*/

object Map extends App{

  //Define the Spark Configuration
  val conf = new SparkConf().setAppName("BasicMap").setMaster("local")

  //Define the Spark Context
  val sc = new SparkContext(conf)

  //Define a collection
  val data = Array(1, 2, 3, 4, 5)

  //Parallelize Collections
  val input = sc.parallelize(data)

  //transform input RDD
  val result = input.map(x => x*x)

  //Call an action
  println(result.collect().mkString(","))
  println(result.count())

  //Stop the Spark Context
  sc.stop()
}
