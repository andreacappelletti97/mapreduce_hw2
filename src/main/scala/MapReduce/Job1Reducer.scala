package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Job1Reducer extends Reducer[LongWritable,IntWritable,IntWritable,LongWritable] {

  override def reduce(key: LongWritable, values: java.lang.Iterable[IntWritable], context:Reducer[LongWritable, IntWritable, IntWritable, LongWritable]#Context): Unit = {
    val sum = values.foldLeft(0) { (m,x) => m + x.get }
    context.write(new IntWritable(sum), key)
  }

}
