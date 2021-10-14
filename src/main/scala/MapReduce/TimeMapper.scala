package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.text.SimpleDateFormat
import java.util.TimeZone


class TimeMapper extends Mapper[LongWritable, Text, Text, Text] {

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val conf = context.getConfiguration
    val startTime = conf.get("startTime")
    val splitInterval: Int = Integer.parseInt(conf.get("splitInterval"))
    val line: String = value.toString
    val splittedString = line.split(" ")
    val currentTime = splittedString(0)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    val currentDate = dateFormat.parse("1970-01-01 " + currentTime);
    val startDate = dateFormat.parse("1970-01-01 " + startTime);

    System.out.println("time1:" + currentDate.getTime)
    System.out.println("time2:" + startDate.getTime)
    val dif = ((currentDate.getTime() - startDate.getTime()) / splitInterval).round
    context.write(new Text(dif.toString), value)

  }

}
