package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.JavaConverters.*

class Job0Reducer extends Reducer[Text,Text,Text,Text] {

  val logger = CreateLogger(classOf[TimeReducer])
  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  //Function to sum both the frequency and the regex matches number with foldLeft
  def sumOperation(values: Iterable[Text], frequency: Boolean): Int = {
    val sum = values.foldLeft(0) { (m,x) =>
      val value = x.toString
      if(frequency) {
        val numberOfTypes = Integer.parseInt(value.split(config.getString("config.splitByComma"))(0))
        numberOfTypes + m
      } else {
        val numberOfTypes = Integer.parseInt(value.split(config.getString("config.splitByComma"))(1))
        numberOfTypes + m
      }
    }
    return sum
  }
  
  override def reduce(key: Text, values: java.lang.Iterable[Text], context:Reducer[Text, Text, Text, Text]#Context): Unit = {
    logger.info("Job0 reducer has started...")
    //Bug found into the method Text.toSeq of hadoop library
    //Copy by reference and not by value, this makes the method not working.
    //In order to fix it, map the values received
    val valuesCopy = values.map(c => Text(c.toString()))
    val valuesSeq = valuesCopy.toSeq
    //Create two iterable to perform the frequency sum and the regex match sum
    val it1 = valuesSeq.toIterable
    val it2 = valuesSeq.toIterable
    val sum = sumOperation(it1, true)
    val secondSum = sumOperation(it2, false)
    context.write(key, new Text(sum + config.getString("config.splitByComma") + secondSum))
    logger.info("Job0 reducer has ended...")
  }

}
