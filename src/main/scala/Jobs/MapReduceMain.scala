package Jobs

import HelperUtils.{CreateLogger, ObtainConfigReference}
import Simulations.BasicCloudSimPlusExample
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory


class MapReduceMain

object MapReduceMain:
  val logger = CreateLogger(classOf[MapReduceMain])
  @main def runSimulation =
    logger.info("Running the map reduce jobs")
    
    logger.info("Finished run")
