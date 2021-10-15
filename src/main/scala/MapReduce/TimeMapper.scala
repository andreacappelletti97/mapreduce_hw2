package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.text.SimpleDateFormat
import java.util.TimeZone


class TimeMapper extends Mapper[LongWritable, Text, Text, Text] {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val conf = context.getConfiguration
    val startTime = conf.get("startTime")
    val splitInterval: Int = Integer.parseInt(conf.get("splitInterval"))
    val line: String = value.toString
    val splittedString = line.split(" ")
    val currentTime = splittedString(0)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    val currentDate = dateFormat.parse(config.getString("config.timeIntervalJob.standardDate") + currentTime);
    val startDate = dateFormat.parse(config.getString("config.timeIntervalJob.standardDate") + startTime);
    val dif = ((currentDate.getTime() - startDate.getTime()) / splitInterval).round
    context.write(new Text(dif.toString), value)

  }

}
