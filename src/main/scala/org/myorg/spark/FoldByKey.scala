package org.myorg.spark

import org.apache.spark.{SparkConf, SparkContext}

/*
* In Map/Reduce key plays a role of grouping values.
* We can use foldByKey operation to aggregate values based on keys.
* */

object FoldByKey {
  def main(args: Array[String]) {

    //Define the Spark Configuration
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)





    //TODO: Enter your code here

    //stop the context
    sc.stop()
  }


}
