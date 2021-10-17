package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class Job1Mapper extends Mapper[LongWritable, Text, LongWritable, IntWritable] {
  val logger = CreateLogger(classOf[Job1Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val one = new IntWritable(1)
  private final val zero = new IntWritable(0)
  private val timeInterval = new LongWritable()
  private final val patternType = Pattern.compile(config.getString("config.job1.pattern"))
  private final val patternLogMessage = Pattern.compile(config.getString("config.logMessagePattern"))

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, LongWritable, IntWritable]#Context): Unit = {
    val line: String = value.toString
    val splittedLineBySpace = line.split(" ")
    val splittedLineByComma = line.split(",")
    val interval = splittedLineByComma(0)
    //Match the ERROR message type
    splittedLineBySpace.foreach { token =>
      val matcherType = patternType.matcher(token)
      val matcherLogMessage = patternLogMessage.matcher(splittedLineBySpace.last)
      if (matcherType.matches()) {
        timeInterval.set(interval.toLong)
        //Match the RE pattern
        if(matcherLogMessage.matches()){
          context.write(timeInterval, one)
        } else {
          context.write(timeInterval, zero)
        }
      }
    }
  }

}
