package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Job2Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  val logger = CreateLogger(classOf[Job2Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Job2 mapper is running...")
    val sum = values.foldLeft(0) { (m,x) => m + x.get }
    context.write(key, new IntWritable(sum))
    logger.info("Job2 reducer has ended...")
  }

}
