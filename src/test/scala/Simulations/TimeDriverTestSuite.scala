package Simulations

import MapReduce.TimeDriver.config
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimeDriverTestSuite extends AnyFlatSpec with Matchers {
  behavior of "configuration parameters module"

  //This test checks if the right configuration is loaded
  
  it should "obtain the right type matching pattern" in {
    config.getString("config.job0.pattern") shouldBe "(TRACE)|(DEBUG)|(INFO)|(WARN)|(ERROR)|(FATAL)"
  }

  it should "Set the right output format CSV" in {
    config.getString("config.outputFormat") shouldBe ","
  }
  
  it should  "Set the right type of logMessagePattern" in {
    config.getString("config.logMessagePattern") shouldBe "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
  }


}
