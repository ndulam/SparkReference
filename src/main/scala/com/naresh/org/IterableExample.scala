package com.naresh.org

object IterableExample {

  val pair = sc.parallelize(List(("A",5),("B",6),("A",7),("C",8),("B",9)  ))
  val gpd = pair.groupByKey()

  val r = gpd.map(t=>{
    val l = t._2
    val b = l.toList
    for(e<-b){
      println(e)
    }
  })


  val file = sc.textFile("/tx/airline_stg/data_dt=2006")
  val header =file.first

  val data = file.filter(line => line!=header)
  val edata = data.map(x=>((x.split(",")(9),(x.split(",")(16),x.split(",")(17),x.split(",")(18).toInt ))))
  val result = edata.combineByKey( (pair)=>(pair._3,1) , (acc:(Int,Int),v) => (acc._1+v._3,acc._2+1),(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

}
