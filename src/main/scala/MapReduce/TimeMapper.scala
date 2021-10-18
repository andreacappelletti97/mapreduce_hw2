package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import MapReduce.TimeDriver.logger
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.text.SimpleDateFormat
import java.util.TimeZone


class TimeMapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = CreateLogger(classOf[TimeMapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("TimeInterval mapper has started...")
    //Get the configuration starTime param
    val conf = context.getConfiguration
    val startTime = conf.get("startTime")
    //Get the split interval defined into the config
    val splitInterval: Int = Integer.parseInt(conf.get("splitInterval"))
    //Get the current line
    val line: String = value.toString
    //Split the line by space
    val splittedString = line.split(config.getString("config.splitBySpace"))
    //Get the time of the log message
    val currentTime = splittedString(0)
    //Init dummy dateFormat to execute operations with date and time
    val dateFormat = new SimpleDateFormat(config.getString("config.timeIntervalJob.dateFormat"));
    //Set the timezone of the dummy dateFormat
    dateFormat.setTimeZone(TimeZone.getTimeZone(config.getString("config.timeIntervalJob.dateZone")));
    //Init the dummy dateFormats
    val currentDate = dateFormat.parse(config.getString("config.timeIntervalJob.standardDate") + currentTime);
    val startDate = dateFormat.parse(config.getString("config.timeIntervalJob.standardDate") + startTime);
    //Compute the time interval
    val dif = ((currentDate.getTime() - startDate.getTime()) / splitInterval).round
    //Return the timeInterval as a key and the log message as value
    context.write(new Text(dif.toString), value)
    logger.info("TimeInterval mapper has ended...")
  }

}
