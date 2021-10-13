package Jobs
import HelperUtils.{CreateLogger, ObtainConfigReference}
import jdk.jpackage.internal.Arguments.CLIOptions.context
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.jets3t.service.utils.TimeFormatter

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Iterator, TimeZone}
import java.util.regex.Pattern
import java.time.*
import java.time.format.*
import java.time.temporal.*


class TimeIntervals

object TimeIntervals{
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  private val timePattern =  Pattern.compile("([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(:|\\.)\\d{3}")
  private var startTimeInterval = ""
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] {
    private final val pattern = Pattern.compile(config.getString("config.job0.pattern"))
    private val logInterval = new Text()
    private var myInterval = ""

    override def configure(job: JobConf): Unit = myInterval = job.get("startTime")

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter) = {
      val line: String = value.toString
      val splittedString = line.split(" ")
      val time = splittedString(0)

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      val date = dateFormat.parse("1970-01-01 " + time);
      val startDate = dateFormat.parse("1970-01-01 " + myInterval);
      val dif = ((date.getSeconds() - startDate.getSeconds()) / 5).round
      System.out.println("Time difference is " + dif)
      logInterval.set(dif.toString)
      output.collect(logInterval, value)

    }
  }


  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    @throws[IOException]
    def reduce(key: Text, values: Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter) = {
      System.out.println("key " + key)
      System.out.println("Values" )
      values.forEachRemaining(
        token => {
          System.out.println(token)
          output.collect(key, token)
        }
      )

    }
  }

  @throws[Exception]
  def Start(args: Array[String]) = {
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("TimeIntervalsBuilder")
    logger.info("Starting the timeintervalsBuilder job")
    //CSV output format
    conf.set("mapred.textoutputformat.separator", config.getString("config.outputFormat"));

    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])

    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])

    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    val newPath = new Path(args(0), "input.txt")
    val fs = FileSystem.get(conf)
    val input = fs.open(newPath)
    val bf = new BufferedReader(new InputStreamReader(input))
    val firstLine = bf.readLine()
    input.close();
    bf.close();
    fs.close();

    val time = firstLine.split(" ")
    val matcher = timePattern.matcher(time(0))
    if(matcher.matches()){
      startTimeInterval = matcher.group()
      System.out.println("MATCH!!!!")
      System.out.println(startTimeInterval)
    } else {
      System.out.println("NOT MATCH ")
    }
    conf.set("startTime", startTimeInterval)

    JobClient.runJob(conf)
    logger.info("Finished timeintervalsBuilder job")
  }
}
