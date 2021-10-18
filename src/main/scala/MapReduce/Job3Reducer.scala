package MapReduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import math.Ordering.Implicits.infixOrderingOps

class Job3Reducer extends Reducer[Text,Text,Text,IntWritable] {

  val COMMA = ",,,,,"

  override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    System.out.println("key is " + key)
    /* key = INFO, values --> adsjkdas,1 */

    /* ERROR, A8ibg2M9ice0O5hbf0  , 18 */
    val split = values.map(t => (t.toString.split(COMMA)(0), t.toString.split(COMMA)(1)))
    val maxSplit = split.reduceLeft((t1, t2) =>
      if (t1._2 > t2._2) {
        t1
      } else {
        t2
      }
    )
    //System.out.println("maxString: " + maxSplit._1)
    //System.out.println("maxValue: " + maxSplit._2)
    context.write(key, IntWritable(maxSplit._1.length))

      /*
      val splitted = t.toString.split(",")
      val dropped = splitted.drop(1)
      System.out.println("dropped is " + dropped.toString)
      dropped.foreach(z => System.out.println("z is" + z))
      val dropMap = dropped.map(_.toInt)
      System.out.println("final is " + dropMap)
    //val valuesF = values.map(c => Integer.parseInt(c.toString))
    val valueSeq = dropMap.toSeq
    val maxValue = valueSeq.reduceLeft(_ max _)
    System.out.println("max value is " + maxValue)
    //context.write(key, IntWritable(maxValue))
    }
      */
  }
}