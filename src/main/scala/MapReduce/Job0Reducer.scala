package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.JavaConverters._

class Job0Reducer extends Reducer[Text,Text,Text,Text] {

  def sumOperation(values: Iterable[Text], frequency: Boolean): Int = {
    val sum = values.foldLeft(0) { (m,x) =>
      val value = x.toString
      if(frequency) {
        val numberOfTypes = Integer.parseInt(value.split(",")(0))
        numberOfTypes + m
      } else {
        val numberOfTypes = Integer.parseInt(value.split(",")(1))
        numberOfTypes + m
      }
    }
    return sum
  }
  
  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {
    val valuesF = values.map(c => Text(c.toString()))
    val valuesSeq = valuesF.toSeq
    val it1 = valuesSeq.toIterable
    val it2 = valuesSeq.toIterable
    val sum = sumOperation(it1, true)
    val secondSum = sumOperation(it2, false)
    context.write(key, new Text(sum + "," + secondSum))
  }

}
