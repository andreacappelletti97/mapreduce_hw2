package Jobs

import HelperUtils.CreateLogger
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import java.io.IOException
import java.util.Iterator
import java.util.regex.Pattern

class TypeFrequency

object TypeFrequency {
  val logger = CreateLogger(classOf[TypeFrequency])

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    private final val pattern = Pattern.compile("(TRACE)|(DEBUG)|(INFO)|(WARN)|(ERROR)|(FATAL)")
    private final val one = new IntWritable(1)
    private val logLevel = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter) = {
      val line: String = value.toString
      line.split("[\\[\\]]").foreach { token =>
      val matcher = pattern.matcher(token)
      if(matcher.matches()){
        logLevel.set(token)
        output.collect(logLevel, one)
      }
      }
    }
  }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    @throws[IOException]
    def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter) = {
      val sum = values.toList.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
    }
  }


  @throws[Exception]
  def main(args: Array[String]) = {
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("TypeFrequency")
    logger.info("STARTING THE EVEN ODD SUM")


    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))
    JobClient.runJob(conf)
    logger.info("FINISH")

  }
}
