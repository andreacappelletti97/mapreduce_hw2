import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.{LocalTimeIntervals, TimeIntervals, TypeFrequency}
import MapReduce.{Driver, TimeDriver}
import Simulations.BasicCloudSimPlusExample
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

object Simulation:
  val logger = CreateLogger(classOf[Simulation])

  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  def main(args: Array[String])  = {
    logger.info("Running mapreduce job0")
    TimeDriver.Run(args)
    //TypeFrequency.Start(args)
    logger.info("Finished mapreduce job0...")
  }

class Simulation