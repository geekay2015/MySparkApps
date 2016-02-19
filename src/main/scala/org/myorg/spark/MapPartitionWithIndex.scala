package org.myorg.spark



import org.apache.spark.{SparkContext, SparkConf}

/*
* mapPartitionsWithIndex(),
* which apart from similar to mapPartitions() also provides an index to track the Partition No
*
* def mapPartitionsWithIndex[U](
* f: (Int, Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)
* (implicit arg0: ClassTag[U]): RDD[U]
*
* Return a new RDD by applying a function to each partition of this RDD,
* while tracking the index of the original partition.
*
* preservesPartitioning indicates whether the input function preserves the partitioner,
* which should be false unless this is a pair RDD and the input function doesn't modify the keys.
*
*/

object MapPartitionWithIndex extends App {
  //Define the Spark Configuration
  val conf = new SparkConf().setAppName("BasicMap").setMaster("local")

  //Define the Spark Context
  val sc = new SparkContext(conf)

  val rdd1 = sc.parallelize(List("yellow", "red", "blue", "cyan", "black"), 3)

  val mapped = rdd1.mapPartitionsWithIndex{
    //index represents the partitions no
    //iterator to itreate though all the elements in the partitions
    (index, iterator) => {
      println("Called in the Partition " + index)
      val myList = iterator.toList
      //initialization
      myList.map(x => x + " ->" + index).iterator
    }
  }
  println(mapped.collect().mkString(","))


  //Stop the Spark Context
  sc.stop()
}
