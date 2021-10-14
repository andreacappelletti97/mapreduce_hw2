package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper


class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val conf = context.getConfiguration
    val evenString = conf.get("even")
    val oddString = conf.get("odd")
    val even = new Text(evenString)
    val odd = new Text(oddString)
    
    val number  = Integer.parseInt(value.toString)
    val n = IntWritable(number)

    if(number % 2 == 0){
      System.out.println("NUMBER IS EVEN")
      System.out.println(number)
      context.write(even, n)
    } else {
      System.out.println("NUMBER IS ODD")
      System.out.println(number)
      context.write(odd, n)
    }
  }

}
