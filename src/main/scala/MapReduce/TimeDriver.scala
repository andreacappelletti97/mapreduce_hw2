package MapReduce

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

  def Run(args: Array[String]) = {
    val jobName0 = "job0"
    val timePattern =  Pattern.compile("([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(:|\\.)\\d{3}")

    val conf: Configuration = new Configuration()
    conf.set("mapred.textoutputformat.separator", ",")


    val newPath = new Path(args(0), "input.txt")
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
    conf.set("splitInterval", "5000")
    
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
