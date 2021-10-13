package Simulations

import Simulations.BasicCloudSimPlusExample.config
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BasicCloudSimPlusExampleTestSuite extends AnyFlatSpec with Matchers {
  behavior of "configuration parameters module"

  it should "obtain the right type matching pattern" in {
    config.getString("config.job0.pattern") shouldBe "(TRACE)|(DEBUG)|(INFO)|(WARN)|(ERROR)|(FATAL)"
  }

  it should "Set the right output format CSV" in {
    config.getString("config.outputFormat") shouldBe ","
  }
}
