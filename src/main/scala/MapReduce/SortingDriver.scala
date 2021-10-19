package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

class SortingDriver
/*
This job finds out the logMessagePattern longest match per type and printout the entire message length
*/
object SortingDriver {
  val logger = CreateLogger(classOf[SortingDriver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.sorting.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    job.setSortComparatorClass(classOf[LongWritable.DecreasingComparator])
    //Setup the right scala classes
    job.setJarByClass(classOf[SortingMapper])
    job.setMapperClass(classOf[SortingMapper])
    job.setReducerClass(classOf[SortingReducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[LongWritable, IntWritable]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.sorting.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.sorting.outputDir")))
    //Start the job
    logger.info("Running Sorting...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("Sorting is completed...")
  }
}
