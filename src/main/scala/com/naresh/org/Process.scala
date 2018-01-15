package com.naresh.org

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object Process
{

  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf().setAppName("process")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("/tx/airline_stg/data_dt=2008/2008.csv")
    //println(file.count)
    val schema =  StructType(Array(StructField("year",IntegerType,false),StructField("flightnum",IntegerType,false), StructField("origin",StringType,false),StructField("dest",StringType,false),StructField("distance",IntegerType,false),StructField("Cancelled",StringType,false),StructField("Canclecode",StringType,false)   ))
    val splitData = data.map(r=>r.split(","))
    val rdd = splitData.map(data=>org.apache.spark.sql.Row(data(0).toInt,data(9).toInt,data(16),data(17),data(18).toInt,data(21),data(22)  ))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schemaRDD = sqlContext.createDataFrame(rdd,schema)
    schemaRDD.registerTempTable("temp")
    sqlContext.sql("select min(distance),max(distance) from temp").first

    sc.stop()
  }


}
