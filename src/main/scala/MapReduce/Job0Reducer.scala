package MapReduce

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class Job0Reducer extends Reducer[Text,Text,Text,Text] {

  def sumOperation(values: Iterable[Text], frequency: Boolean): Int = {
    val sum = values.foldLeft(0) { (m,x) =>
      System.out.println("fold m " + m)
      System.out.println("fold x " + x)
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

  def createList(myValueList : List[Int], iteration : Integer): Unit ={
    if(iteration > 0) return myValueList

  }

  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {
    System.out.println("enter with key " + key)
    val copy1 = values
    val copy2 = values
    val sum = sumOperation(copy1, false)
    val secondSum = sumOperation(copy2, true)
    context.write(key, new Text(sum + "," + secondSum))
  }

}
