package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

class Job3Driver
/*
This job finds out the logMessagePattern longest match per type and printout the entire message length
*/
object Job3Driver {
  val logger = CreateLogger(classOf[Job3Driver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.job3.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Setup the right scala classes
    job.setJarByClass(classOf[Job3Mapper])
    job.setMapperClass(classOf[Job3Mapper])
    job.setReducerClass(classOf[Job3Reducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.job3.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.job3.outputDir")))
    //Start the job
    logger.info("Running job3...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("Job3 is completed...")
  }
}
