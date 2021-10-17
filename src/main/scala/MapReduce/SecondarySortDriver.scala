package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.io.LongWritable
class SecondarySortDriver

object SecondarySortDriver {
  val logger = CreateLogger(classOf[SecondarySortDriver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.secondarySortJob.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Setup the right scala classes
    job.setJarByClass(classOf[SecondarySortMapper])
    job.setMapperClass(classOf[SecondarySortMapper])
    job.setReducerClass(classOf[SecondarySortReducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[IntWritable, IntWritable]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.secondarySortJob.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.secondarySortJob.outputDir")))
    
    //Start the job
    logger.info("Running the time interval job...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("Time interval job is completed...")
  }
}
