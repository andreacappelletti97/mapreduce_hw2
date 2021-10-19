package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import java.util.regex.Pattern


class Job3Mapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = CreateLogger(classOf[Job3Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private val logLevel = new Text()
  private final val patternType = Pattern.compile(config.getString("config.job3.pattern"))
  private final val patternLogMessage = config.getString("config.logMessagePattern").r
  

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Job3 mapper has started...")
    val line = value.toString
    val logMessage = line.split(config.getString("config.splitBySpace")).last
    line.split(config.getString("config.splitBySpace")).foreach(token =>
      //Match the type --> INFO, DEBUG, ERROR ...
      if (UtilityFunctions.matchType(token, patternType)) {
        //Match the RE into the message
        val listOfPatternMatch = patternLogMessage.findAllIn(logMessage).toList
        if(!UtilityFunctions.isEmpty(listOfPatternMatch)){
          val lengthList = listOfPatternMatch.map(t => t.length)
          lengthList.foreach(l =>
            context.write(Text(token), Text(logMessage + config.getString("config.logMessageSeparator") + l.toString))
          )
        }
      }
    )
    logger.info("Job3 mapper has ended...")
  }

}
