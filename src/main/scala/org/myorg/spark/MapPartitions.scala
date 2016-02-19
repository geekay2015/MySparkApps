package org.myorg.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.ContentExchange


/*
* mapPartitions() can be used as an alternative to map() & foreach().
* mapPartitions() is called once for each Partition unlike map() & foreach()
* which is called for each element in the RDD.
*
* The main advantage being that, we can do initialization on
* Per-Partition basis instead of per-element basis(as done by map() & foreach())
*
* Consider the case of Initializing a database.
* If we are using map() or foreach(), the number of times we would need to
* initialize will be equal to the no of elements in RDD.
* Whereas if we use mapPartitions(), the no of times we would need to initialize would be equal to number of Partitions
*
* We get Iterator as an argument for mapPartition, through which we can iterate through all the elements in a Partition.
* */

object MapPartitions extends App{

  //Define the Spark Configuration
  val conf = new SparkConf().setAppName("BasicMap").setMaster("local")

  //Define the Spark Context
  val sc = new SparkContext(conf)

  //Define a Collection
  val inputData = List("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB")

  //Parallelize the collection
  val input = sc.parallelize(inputData)

  //transform using the mapPartitions
  val result = input.mapPartitions{
    signs =>
      val client = new HttpClient()
      client.start()
      signs.map {sign =>
        val exchange = new ContentExchange(true)
        exchange.setURL(s"http://qrzcq.com/call/${sign}")
        client.send(exchange)
        exchange
      }.map { exchange =>
        exchange.waitForDone();
        exchange.getResponseContent
      }
  }
  println(result.collect().mkString(","))

  //Stop the Spark Context
  sc.stop()
}
