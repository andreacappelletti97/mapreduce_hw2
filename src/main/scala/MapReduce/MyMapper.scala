package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper


class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  private final val even = Text("even")
  private final val odd = Text("odd")
  override def map(key: LongWritable, value:Text, context:Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val conf = context.getConfiguration
    val test = conf.get("myConfig")
  System.out.println(test)
    System.out.println(value)
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
