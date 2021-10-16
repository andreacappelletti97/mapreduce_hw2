package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern
import java.io.IOException
import java.util.Iterator

class Job0Mapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val one = new IntWritable(1)
  private val logLevel = new Text()
  private final val patternLogType = Pattern.compile(config.getString("config.job0.pattern"))
  private final val patternLogMessage = Pattern.compile(config.getString("config.logMessagePattern"))

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line: String = value.toString
    val splittedBySpace = line.split(" ")
    val timeInterval = (line.split(","))(0)
    line.split(" ").foreach { token =>
      val matchLogType = patternLogType.matcher(token)
      val matchLogMessage = patternLogMessage.matcher(splittedBySpace.last)
      if(matchLogType.matches()){
        logLevel.set(token)
        //TODO: fix match ... 
        if(matchLogMessage.matches()){
          System.out.println("writing1 ")
          context.write(new Text(timeInterval + "," + token), new Text("1,1"))
        } else {
          System.out.println("writing2 ")
          context.write(new Text(timeInterval + "," + token), new Text("1,0"))
        }
      }
    }

  }
}
