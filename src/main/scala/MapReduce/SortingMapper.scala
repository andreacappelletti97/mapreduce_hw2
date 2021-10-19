package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class SortingMapper extends Mapper[LongWritable, Text, LongWritable, IntWritable] {
  val logger = CreateLogger(classOf[Job1Mapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }


  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, LongWritable, IntWritable]#Context): Unit = {
    logger.info("Sorting mapper has started...")
    val line: String = value.toString
    val splittedLineByComma = line.split(config.getString("config.splitByComma"))
    val key = splittedLineByComma(0)
    val myValue = splittedLineByComma(1)
    //Flip key and value
    context.write(LongWritable(Integer.parseInt(myValue)), IntWritable(Integer.parseInt(key)))
  }

}
