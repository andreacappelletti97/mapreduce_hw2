package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Job2Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.foldLeft(0) { (m,x) => m + x.get }
    context.write(key, new IntWritable(sum))
  }

}
