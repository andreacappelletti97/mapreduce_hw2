package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.io.LongWritable
import java.io.{BufferedReader, InputStreamReader}
import java.util.regex.Pattern

class Job1Driver
/*
This job finds out the frequency in desc order for each time interval of logMessagePattern in the ERROR log type
*/
object Job1Driver {
  val logger = CreateLogger(classOf[Job1Driver])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def Run() = {
    val jobName = config.getString("config.job1.name")
    //Instanciate the configuration
    val conf: Configuration = new Configuration()
    //Set the output to CSV format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"))
    //Set the config for the job
    val job = Job.getInstance(conf, jobName)
    //Order the output in a DESC order by key
    //job.setSortComparatorClass(classOf[LongWritable.DecreasingComparator])
    //Setup the right scala classes
    job.setJarByClass(classOf[Job1Mapper])
    job.setMapperClass(classOf[Job1Mapper])
    job.setReducerClass(classOf[Job1Reducer])
    //Setup Mappers and Reducers i/o format
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[LongWritable, IntWritable]])
    //Setup input and output directories
    FileInputFormat.addInputPath(job, new Path(config.getString("config.job1.inputDir")))
    FileOutputFormat.setOutputPath(job, new Path(config.getString("config.job1.outputDir")))
    //Start the job
    logger.info("Running job1...")
    job.waitForCompletion(config.getBoolean("config.verbose"))
    logger.info("job1 is completed...")
  }

}

