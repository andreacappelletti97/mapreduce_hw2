package Simulations

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import MapReduce.UtilityFunctions

import java.util.regex.Pattern

class UtilityFunctionsTestSuite  extends AnyFlatSpec with Matchers with PrivateMethodTester {

  val logger = CreateLogger(classOf[Job3TestSuite])

  val config = ObtainConfigReference("config") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }
  
  //This test checks the behavior of the isEmpty utility function used in Job3Mapper

  behavior of "UtilityFunction"

  "isEmpty" should "return true if the list of Strings is empty" in {
    val myList = List[String]()
    assert(UtilityFunctions.isEmpty(myList), false)
  }

}
