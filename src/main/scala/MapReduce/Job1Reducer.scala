package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Job1Reducer extends Reducer[LongWritable,IntWritable,IntWritable,LongWritable] {
  val logger = CreateLogger(classOf[Job3Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  override def reduce(key: LongWritable, values: java.lang.Iterable[IntWritable], context:Reducer[LongWritable, IntWritable, IntWritable, LongWritable]#Context): Unit = {
    logger.info("Job1 reducer has started...")
    val sum = values.foldLeft(0) { (m,x) => m + x.get }
    context.write(new IntWritable(sum), key)
    logger.info("Job1 reducer has ended...")
  }

}
