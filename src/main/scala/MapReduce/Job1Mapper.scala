package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class Job1Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val one = new IntWritable(1)
  private val timeInterval = new Text()
  private final val pattern = Pattern.compile(config.getString("config.job1.pattern"))

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val line: String = value.toString
    val splittedLine = line.split(" ")
    val interval = splittedLine(0)
    //Match the ERROR message type
    splittedLine.foreach { token =>
      val matcher = pattern.matcher(token)
      if (matcher.matches()) {
        //Match the RE pattern
        timeInterval.set(interval)
        context.write(timeInterval, one)
      }
    }
  }

}
