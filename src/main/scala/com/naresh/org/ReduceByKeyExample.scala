package com.naresh.org
import org.apache.spark.{SparkConf, SparkContext}
object ReduceByKeyExample
{


  def main(args:Array[String]) = {

    val sparkConf = new SparkConf().setAppName("process")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("/tx/airline_stg/data_dt=2006")
    val header = file.first

    val data = file.filter(line => line != header)
    val edata = data.map(x=>((x.split(",")(9),(x.split(",")(18).toInt ))))
    val result = edata.reduceByKey(_+_)
    sc.stop()
  }

}
