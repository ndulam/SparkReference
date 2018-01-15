package com.naresh.org

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyExample
{

  def main(args:Array[String]) = {

    val sparkConf = new SparkConf().setAppName("process")
    val sc = new SparkContext(sparkConf)


  }



}
