package lab

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.WrappedArray
//import spark.implicits._
import org.apache.spark.sql.functions._

object Ques1sql
{
  def main(args: Array[String])
  {

    val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
    import spark.implicits._
    val lines = spark.sparkContext.textFile(args(0))
    val rdd = lines.map(parse).filter(line => line._1 != "-1")
    val rddPair = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) {
      ((x._1, y), x._2)
    } else {
      ((y, x._1), x._2)
    }))
    val reduceRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val reduce_rddPair = reduceRdd.map(r => (r._1, r._2._2)).filter(l=>l._2!=List())
    val output1=reduce_rddPair.map(r => (r._1, r._2.size)).sortBy(_._1, true)
    val output2=output1.map(r => (r._1._1,r._1._2,r._2)).toDF("FriendA","FriendB", "Mutual")
    output2.createOrReplaceTempView("Table1")
    val sqlDF = spark.sql("SELECT FriendA,FriendB,Mutual FROM Table1").collect()
    val finalRDD=spark.sparkContext.parallelize(sqlDF).map(col=>col(0)+","+col(1)+"\t"+col(2))
    finalRDD.saveAsTextFile(args(1))
  }

    def parse(line: String): (String, (Long, List[String])) = {
      val fields = line.split("\t")
      if (fields.length == 2)
      {
        val user = fields(0)
        val friendsList = fields(1).split(",").toList
        return (user, (1L, friendsList))
      }
      else
      {
        return ("-1", (-1L, List()))
      }
    }
}