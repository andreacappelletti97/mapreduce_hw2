package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern
import java.io.IOException
import java.util.Iterator

class Job0Mapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = CreateLogger(classOf[Job0Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //Utility vals
  private val logLevel = new Text()
  private val myText = new Text()
  //Get the regular expression from the config
  private final val patternLogType = Pattern.compile(config.getString("config.job0.pattern"))
  private final val patternLogMessage = Pattern.compile(config.getString("config.logMessagePattern"))

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Job0 mapper has started...")
    val line: String = value.toString
    //Split the string by space
    val splittedBySpace = line.split(config.getString("config.splitBySpace"))
    //Get the timeInterval
    val timeInterval = (line.split(config.getString("config.splitByComma")))(0)
    line.split(config.getString("config.splitBySpace")).foreach { token =>
      val matchLogType = patternLogType.matcher(token)
      val matchLogMessage = patternLogMessage.matcher(splittedBySpace.last)
      //Detect the type of log message
      if(matchLogType.matches()){
        //Store the log type with the time interval
        logLevel.set(timeInterval + config.getString("config.splitByComma") + token)
        if(matchLogMessage.find()){
          logger.debug("Regex found into the log message...")
          myText.set(config.getString("config.job0.contained"))
          context.write(logLevel, myText)
        } else {
          logger.debug("Regex not found into the log message...")
          myText.set(config.getString("config.job0.notContained"))
          context.write(logLevel, myText)
        }
      }
    }
    logger.info("Job0 mapper has ended...")
  }
}
