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

class Job0Driver
/*
This job finds out the frequency of the logMessagePattern for each log type in each time interval
*/
object Job0Driver {
  val logger = CreateLogger(classOf[Job0Driver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.job0.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Setup the right scala classes
    job.setJarByClass(classOf[Job0Mapper])
    job.setMapperClass(classOf[Job0Mapper])
    job.setReducerClass(classOf[Job0Reducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.job0.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.job0.outputDir")))
    //Start the job
    logger.info("Running job0 job...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("job0 is completed...")
  }
}
