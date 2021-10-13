package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class MyReducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    System.out.println("enter reducer")
    val sum = values.foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }

}
