package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class MyReducer extends Reducer[Text,IntWritable,Text,IntWritable] {

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    System.out.println("enter reducer")
  }

}
