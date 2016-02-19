package org.myorg.spark

import org.apache.spark.{SparkConf, SparkContext}


/*
* def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
* Aggregate the elements of each partition, and
* then the results for all the partitions, using given combine functions and a neutral "zero value".
* This function can return a different result type, U, than the type of this RDD, T.
* Thus, we need one operation for merging a T into an U and one operation for merging two U's, as in scala.TraversableOnce.
* Both of these functions are allowed to modify and return their first argument instead of creating a new U to avoid memory allocation.
*
* Compared to reduce() & fold(), the aggregate() function has the advantage,
* it can return different Type vis-a-vis the RDD Element Type(ie Input Element type)
*/

object Aggreagte {
  def main(args: Array[String]) {

    //Define the Spark Configuration
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

   //define a List
    val list = List(("maths",21), ("english", 22), ("science", 31))


    val inputRDD = sc.parallelize(list, 3)
    println(inputRDD.partitions.length)

    val result = inputRDD.aggregate(3) (
      /// This is a seqOp for merging T into a U that is  (String, Int) into Int
      // (we take (String, Int) in 'value' & return Int
      //Arguments :
      //acc   :  Reprsents the accumulated result
      //value :  Represents the element in 'inputRDD'
      (acc, value) => acc + value._2,
      //combine operation for merging 2 U's that is 2 Integers
      (acc1, acc2) => acc1 + acc2


      )

    println(result.toInt)


    //stop the context
    sc.stop()
  }


}
