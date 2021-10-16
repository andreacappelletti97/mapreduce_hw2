package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

class Job2Driver

object Job2Driver {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run(args: Array[String]) = {
    val jobName = config.getString("config.job2.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Setup the right scala classes
    job.setJarByClass(classOf[Job2Mapper])
    job.setMapperClass(classOf[Job2Mapper])
    job.setReducerClass(classOf[Job2Reducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.job2.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.job2.outputDir")))
    //Start the job
    logger.info("Running the time interval job...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("Time interval job is completed...")
  }
}
