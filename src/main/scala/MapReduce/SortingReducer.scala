package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class SortingReducer extends Reducer[LongWritable,IntWritable,IntWritable,IntWritable] {
  val logger = CreateLogger(classOf[Job3Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  override def reduce(key: LongWritable, values: java.lang.Iterable[IntWritable], context:Reducer[LongWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
    logger.info("Sorting reducer has started...")
    val kmyKey = key.toString
    values.foreach { token =>
      val value = token.toString
      context.write(new IntWritable(Integer.parseInt(value)), new IntWritable(Integer.parseInt(kmyKey)))
    }
    logger.info("Sorting reducer has ended...")
  }

}
