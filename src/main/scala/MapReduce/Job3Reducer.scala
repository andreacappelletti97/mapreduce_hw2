package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import math.Ordering.Implicits.infixOrderingOps

class Job3Reducer extends Reducer[Text,Text,Text,IntWritable] {
  val logger = CreateLogger(classOf[Job3Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  val separator = config.getString("config.logMessageSeparator")

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val split = values.map(t => (t.toString.split(separator)(0), t.toString.split(separator)(1)))
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