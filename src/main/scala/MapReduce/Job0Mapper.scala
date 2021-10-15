package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class Job0Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    System.out.println("enter mapper")
  }

}
