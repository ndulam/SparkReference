package com.naresh.org

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyExample {

  def main(args:Array[String]) =
  {

    val sparkConf = new SparkConf().setAppName("process")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("/tx/airline_stg/data_dt=2006")
    val header =file.first

    val data = file.filter(line => line!=header)
    val edata = data.map(x=>((x.split(",")(9),(x.split(",")(16),x.split(",")(17),x.split(",")(18).toInt ))))
    val result = edata.combineByKey( (pair)=>(pair._3,1) , (acc:(Int,Int),v) => (acc._1+v._3,acc._2+1),(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    sc.stop()
  }

}
