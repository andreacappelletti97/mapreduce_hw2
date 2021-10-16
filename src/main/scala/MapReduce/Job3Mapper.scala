package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class Job3Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[Job3Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val pattern = Pattern.compile(config.getString("config.job2.pattern"))
  private final val one = new IntWritable(1)
  private val logLevel = new Text()

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    
  }

}
