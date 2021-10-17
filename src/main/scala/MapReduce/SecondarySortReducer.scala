package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class SecondarySortReducer extends Reducer[IntWritable,IntWritable,IntWritable,IntWritable] {

  private def handleTuple(key: IntWritable, value: IntWritable, context:Reducer[IntWritable,IntWritable, IntWritable, IntWritable]#Context): Unit = {
    context.write(value, key)
  }

  override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable], context:Reducer[IntWritable, IntWritable, IntWritable, IntWritable]#Context): Unit = {
    //Now handle each tuple individually, flip key and value again
    System.out.println("enter keeeey " + key)
    values.forEach{v =>
      System.out.println("my value is: " + v)
    context.write(v, key)
  }
  }

}
