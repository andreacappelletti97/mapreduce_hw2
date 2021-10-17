package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.util.regex.Pattern

class SecondarySortMapper extends Mapper[LongWritable, Text, IntWritable, IntWritable] {
  val logger = CreateLogger(classOf[SecondarySortMapper])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  private final val pattern = Pattern.compile(config.getString("config.job2.pattern"))
  private final val one = new IntWritable(1)
  private val logLevel = new Text()

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, IntWritable, IntWritable]#Context): Unit = {
    val line = value.toString
    val index = line.split(",")(0)
    val myValue = line.split(",")(1)
    context.write(IntWritable(Integer.parseInt(myValue)), IntWritable(Integer.parseInt(index)))
  }

}
