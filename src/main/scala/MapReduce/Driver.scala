package MapReduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import MapReduce.MyMapper
import MapReduce.MyReducer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

class Driver

object Driver {

  def Run(args: Array[String]) = {
    val jobName0 = "job0"
    val conf: Configuration = new Configuration()
    conf.set("mapred.textoutputformat.separator", ",")
    conf.set("even", "even")
    conf.set("odd", "odd")
    val job0 = Job.getInstance(conf, jobName0)
    job0.setJarByClass(classOf[MyMapper])
    job0.setMapperClass(classOf[MyMapper])
    job0.setReducerClass(classOf[MyReducer])

    job0.setInputFormatClass(classOf[TextInputFormat])
    job0.setOutputKeyClass(classOf[Text])
    job0.setOutputValueClass(classOf[IntWritable])
    job0.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.addInputPath(job0, new Path(args(0)))
    FileOutputFormat.setOutputPath(job0, new Path(args(1)))

    job0.waitForCompletion(true)


  }





}
