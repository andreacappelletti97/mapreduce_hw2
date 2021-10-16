import HelperUtils.{CreateLogger, ObtainConfigReference}
import MapReduce.{Job0Driver, Job1Driver, Job2Driver, TimeDriver}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

object MainDriver:
  val logger = CreateLogger(classOf[MainDriver])

  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def main(args: Array[String])  = {
    logger.info("Running mapreduce jobs")
    TimeDriver.Run()
    Job0Driver.Run()
    Job1Driver.Run()
    Job2Driver.Run()
    logger.info("Finished mapreduce jobs...")
  }

class MainDriver