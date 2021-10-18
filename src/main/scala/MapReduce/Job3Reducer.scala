package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import math.Ordering.Implicits.infixOrderingOps

class Job3Reducer extends Reducer[Text,Text,Text,IntWritable] {

  val COMMA = ",,,,,"

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val split = values.map(t => (t.toString.split(COMMA)(0), t.toString.split(COMMA)(1)))
    val maxSplit = split.reduceLeft((t1, t2) =>
      if (t1._2 > t2._2) {
        t1
      } else {
        t2
      }
    )
    context.write(key, IntWritable(maxSplit._1.length))
  }
}