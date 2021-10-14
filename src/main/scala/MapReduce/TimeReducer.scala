package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Jobs.TypeFrequency
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class TimeReducer extends Reducer[Text,Text,Text,Text] {
  val logger = CreateLogger(classOf[TypeFrequency])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {
    values.foreach(token =>
    context.write(key, token)
    )
  }

}
