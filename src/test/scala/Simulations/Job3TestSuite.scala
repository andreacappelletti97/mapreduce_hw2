package Simulations

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import MapReduce.UtilityFunctions

import java.util.regex.Pattern

class Job3TestSuite  extends AnyFlatSpec with Matchers with PrivateMethodTester {

  val logger = CreateLogger(classOf[Job3TestSuite])

  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  //this test check the behavior of the pattern matching function used in Job3Mapper
  behavior of "UtilityFunction in Job3Mapper"

  "matchType" should "match the right log message type from the config regex" in {

    val myPattern = Pattern.compile(config.getString("config.job3.pattern"))
    val info = "INFO"
    val warn = "WARN"
    val debug = "DEBUG"
    val error = "ERROR"
    val fatal = "FATAL"
    val trace = "TRACE"
    assert(UtilityFunctions.matchType(info,myPattern ), true)
    assert(UtilityFunctions.matchType(warn, myPattern), true)
    assert(UtilityFunctions.matchType(debug, myPattern), true)
    assert(UtilityFunctions.matchType(error, myPattern), true)
    assert(UtilityFunctions.matchType(fatal, myPattern), true)
    assert(UtilityFunctions.matchType(trace, myPattern), true)

  }

}
