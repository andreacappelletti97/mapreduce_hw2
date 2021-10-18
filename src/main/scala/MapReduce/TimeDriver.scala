package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import java.io.{BufferedReader, InputStreamReader}
import java.util.regex.Pattern

class TimeDriver
/*
This job divides and create time intervals
*/
object TimeDriver {
  val logger = CreateLogger(classOf[TimeDriver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.timeIntervalJob.name") 
    //Get the pattern of the allowed log time format to detect the start time
    val timePattern =  Pattern.compile(config.getString("config.timeIntervalJob.timePattern"))
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Retrieve the first line of the input log file
    val newPath = new Path(config.getString("config.timeIntervalJob.inputDir"), config.getString("config.timeIntervalJob.logFileName"))
    val fs = FileSystem.get(conf)
    val input = fs.open(newPath)
    val bf = new BufferedReader(new InputStreamReader(input))
    //Store the first line of the logs
    val firstLine = bf.readLine()
    input.close();
    bf.close();
    fs.close();
    //Split the first line with spaces
    val time = firstLine.split(config.getString("config.splitBySpace"))
    //Get the first time of the logs
    val matcher = timePattern.matcher(time(0))
    //Check if the format is correct and save it to the configuration to pass it to the Mappers and split the log intervals
    if(matcher.matches()){
      val startTimeInterval = matcher.group()
      logger.info("Setting the first log start time to split the intervals on the mappers...")
      conf.set("startTime", startTimeInterval)
    } else {
      logger.error("First interval not match, wrong input format!")
      System.exit(0)
    }
    conf.set("splitInterval", config.getString("config.timeIntervalJob.timeWindow"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Setup the right scala classes
    job.setJarByClass(classOf[TimeMapper])
    job.setMapperClass(classOf[TimeMapper])
    job.setReducerClass(classOf[TimeReducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.timeIntervalJob.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.timeIntervalJob.outputDir")))
    //Start the job
    logger.info("Running the time interval job...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("Time interval job is completed...")
  }
}
