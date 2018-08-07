package lab

import org.apache.spark._

object Ques1
{

  def main(args: Array[String])
  {

    val sc = new SparkContext("local[*]", "Ques1")
    val lines = sc.textFile(args(0))
    val rdd = lines.map(parse).filter(line => line._1 != "-1")
    val rddPair = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) { ((x._1, y), x._2) } else { ((y, x._1), x._2) }))
    val reduceRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val reduce_rddPair = reduceRdd.map(r => (r._1, r._2._2)).filter(l=>l._2!=List())
    val output=reduce_rddPair.map(r => (r._1, r._2)).sortBy(_._1, true)
    val outputTabSpace = output.map(tup => tup._1._1.toString() + "," + tup._1._2.toString() + "\t" + tup._2.toString())
    outputTabSpace.saveAsTextFile(args(1))
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