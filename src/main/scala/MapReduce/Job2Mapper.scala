package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import Jobs.TypeFrequency.config
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class Job2Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val pattern = Pattern.compile(config.getString("config.job2.pattern"))
  private final val one = new IntWritable(1)
  private val logLevel = new Text()

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val line: String = value.toString
    line.split(" ").foreach { token =>
      val matcher = pattern.matcher(token)
      if(matcher.matches()){
        logLevel.set(token)
        context.write(logLevel, one)
      }
    }
  
  }

}
