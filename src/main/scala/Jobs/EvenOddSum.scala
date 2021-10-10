package Jobs

import HelperUtils.CreateLogger
import Jobs.WordCount.{Map, Reduce}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import java.io.IOException
import java.util.Iterator

class EvenOddSum

object EvenOddSum {
  val logger = CreateLogger(classOf[EvenOddSum])

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
    private final val even = Text("even")
    private final val odd = Text("odd")

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter) = {
      logger.info("MAPPER")
      val number  = Integer.parseInt(value.toString)
      val n = IntWritable(number)
      //Even
      if(number % 2 == 0){
        System.out.println("NUMBER IS EVEN")
        System.out.println(number)
        output.collect(even, n)
      } else {
        System.out.println("NUMBER IS ODD")
        System.out.println(number)
        output.collect(odd, n)
      }

    }
  }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    logger.info("REDUCER")
    @throws[IOException]
    def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter) = {
      val sum = values.toList.reduce((valueOne, valueTwo) =>
        System.out.println("value 1")
        System.out.println(valueOne.get())
        System.out.println("value 2")
        System.out.println(valueTwo.get())
        new IntWritable(valueOne.get() + valueTwo.get())

      )

      output.collect(key, new IntWritable(sum.get()))
    }
  }


  @throws[Exception]
  def main(args: Array[String]) = {
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("EvenOddSum")
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
