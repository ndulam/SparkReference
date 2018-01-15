package com.naresh.org
import org.apache.spark.{SparkConf, SparkContext}
object IterableExample {

  def main(args:Array[String]) = {

    val sparkConf = new SparkConf().setAppName("process")
    val sc = new SparkContext(sparkConf)
    val pair = sc.parallelize(List(("A", 5), ("B", 6), ("A", 7), ("C", 8), ("B", 9)))
    val gpd = pair.groupByKey()

    val r = gpd.map(t => {
      val l = t._2
      val b = l.toList
      for (e <- b) {
        println(e)
      }
    })
  }

}
