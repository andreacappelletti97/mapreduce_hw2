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
  private final val patternLogMessage = config.getString("config.testMessagePattern").r

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line = value.toString
    val logMessage = line.split(" ").last
    line.split(" ").foreach(token =>
      val matcherType = patternType.matcher(token)
      //Match the type --> INFO, DEBUG, ERROR ...
      if (matcherType.matches()) {
        //Match the RE into the message
        val listOfPatternMatch = patternLogMessage.findAllIn(logMessage).toList
        if(!listOfPatternMatch.isEmpty){
          val lengthList = listOfPatternMatch.map(t => t.length)
          lengthList.foreach(l =>
            context.write(Text(token), Text(logMessage + "," + l.toString))
          )
        }
      }
    )
  }

}
