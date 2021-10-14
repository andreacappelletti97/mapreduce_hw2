package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import java.io.{BufferedReader, InputStreamReader}
import java.util.regex.Pattern

class TimeDriver

object TimeDriver {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run(args: Array[String]) = {
    val jobName0 = "job0"
    val timePattern =  Pattern.compile(config.getString("config.timePattern"))

    val conf: Configuration = new Configuration()
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))


    val newPath = new Path(args(0), config.getString("config.logFileName"))
    val fs = FileSystem.get(conf)
    val input = fs.open(newPath)
    val bf = new BufferedReader(new InputStreamReader(input))
    val firstLine = bf.readLine()
    input.close();
    bf.close();
    fs.close();

    val time = firstLine.split(" ")
    val matcher = timePattern.matcher(time(0))
    if(matcher.matches()){
      val startTimeInterval = matcher.group()
      System.out.println("MATCH!!!!")
      System.out.println(startTimeInterval)
      conf.set("startTime", startTimeInterval)
    } else {
      System.out.println("NOT MATCH ")
    }
    conf.set("splitInterval", config.getString("config.timeWindow"))

    val job0 = Job.getInstance(conf, jobName0)
    job0.setJarByClass(classOf[TimeMapper])
    job0.setMapperClass(classOf[TimeMapper])
    job0.setReducerClass(classOf[TimeReducer])

    job0.setInputFormatClass(classOf[TextInputFormat])
    job0.setOutputKeyClass(classOf[Text])
    job0.setOutputValueClass(classOf[Text])
    job0.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.addInputPath(job0, new Path(args(0)))
    FileOutputFormat.setOutputPath(job0, new Path(args(1)))

    job0.waitForCompletion(true)

  }
}
