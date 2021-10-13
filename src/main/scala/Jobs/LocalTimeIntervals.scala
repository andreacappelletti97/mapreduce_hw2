package Jobs

import HelperUtils.CreateLogger
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.conf.Configuration
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.Iterator
import org.apache.hadoop.fs.FileSystem
class LocalTimeIntervals
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import scala.jdk.javaapi.CollectionConverters.asScala



object LocalTimeIntervals {
  val logger = CreateLogger(classOf[EvenOddSum])

  class Map extends Mapper[Object, Text, Text, IntWritable] {
    private final val even = Text("even")
    private final val odd = Text("odd")
    @throws[IOException]
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val number  = Integer.parseInt(value.toString)
      val n = IntWritable(number)
      System.out.println("enter here!!!")
      val conf = context.getConfiguration
      val test = conf.get("test")
      System.out.println("test" + test)
      if(number % 2 == 0){
        System.out.println("NUMBER IS EVEN")
        System.out.println(number)
        context.write(even, n)
      } else {
        System.out.println("NUMBER IS ODD")
        System.out.println(number)
        context.write(odd, n)
      }
    }
  }

  class Reduce  extends Reducer[Text, Text, Text, IntWritable] {
    @throws[IOException]
    override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text,Text, Text, IntWritable]#Context): Unit = {
      System.out.println("enter 333!!!")
      context.write(key, new IntWritable(1))
    }
  }


  @throws[Exception]
  def Start(args: Array[String]) = {
    val conf: Configuration = new Configuration()
    System.out.println("enter 222!!!")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.set("test", "myTst")
    val job0 = Job.getInstance(conf, "job0")

    job0.setMapperClass(classOf[LocalTimeIntervals.Map])
    //job0.setCombinerClass(classOf[LocalTimeIntervals.Reduce])
    job0.setReducerClass(classOf[LocalTimeIntervals.Reduce])

    job0.setInputFormatClass(classOf[TextInputFormat])
    job0.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(job0, new Path(args(0)))
    FileOutputFormat.setOutputPath(job0, new Path(args(1)))



    job0.waitForCompletion(true)
    logger.info("FINISH")

  }

}
