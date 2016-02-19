package org.myorg.spark

import org.apache.spark.{SparkConf, SparkContext}

/*
* Fold is a very powerful operation in spark which allows
* you to calculate many important values in O(n) time.
*
* Syntax:
* def fold[T](acc:T)((acc,value) => acc)
* def fold(zeroValue: T)(op: (T, T) ⇒ T): T
*
* T          :is the data type of RDD
* acc        :is accumulator of type T which will be return value of the fold operation
* A function : , which will be called for each element in rdd with previous accumulator.
*
* Aggregate the elements of each partition, and
* then the results for all the partitions, using a given associative function and a neutral "zero value".
* The function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation;
* however, it should not modify t2.
* */

object Fold {
  def main(args: Array[String]) {

    //Define the Spark Configuration
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

    /*Example1*/

    //Build a RDD
    val employeeData = List(("Jack", 100000.0),("Bob", 200000.0),("Chris", 300000.0))
    val employeeRDD = sc.makeRDD(employeeData)

    //Lets find out an employee with maximum Salary
    //To use folld we need a start value
    val dummyEmployee = ("dummy", 0.0)

    //now find the maximum Salary using fold
    val maxSalaryEmployee = employeeRDD.fold(dummyEmployee)((acc,employee) =>{
      if (acc._2 < employee._2) employee else acc
    }
    )

    val minSalaryEmployee = employeeRDD.fold(dummyEmployee)((acc,employee) =>{
      if (acc._2 == employee._2) employee else acc
    }
    )

    //print employee with max salary
    println("MAXIMUM VALUE USING FOLD: " + maxSalaryEmployee)
    println("MINIMUM VALUE USING FOLD: " + minSalaryEmployee)


    /*Example2*/

    val rdd1 = sc.parallelize(List(
      ("Maths", 95),
      ("English",80),
      ("Science",90)
    ))

    println(rdd1.partitions.length)

    val additionalMarks = ("extra", 4)

    val sum = rdd1.fold(additionalMarks) { (acc, marks) =>
      val sum = acc._2 + marks._2
      ("Total", sum )
    }

    println("SUM USING FOLD: " + sum)

    /*Example3*/
    val input = sc.parallelize(List(1,2,3,4))
    val output = input.fold(0) ((x, y) =>
      x + y )

    print("Simple Sum using Fold: " + output)


    //stop the context
    sc.stop()
  }


}
