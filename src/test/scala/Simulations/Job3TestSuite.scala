package Simulations

import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import MapReduce.UtilityFunctions

import java.util.regex.Pattern

class Job3TestSuite  extends AnyFlatSpec with Matchers with PrivateMethodTester {
  behavior of "UtilityFunction"

  "is valid test" should "match the regex" in {

    val myPattern = Pattern.compile("INFO")
    val info = "INFO"
    assert(UtilityFunctions.matchType(info,myPattern ), true)


  }

}
