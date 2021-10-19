package MapReduce

import java.util.regex.Pattern

object UtilityFunctions {
  /*Utility function to match the right log type */
  def matchType(token: String, patternType: Pattern): Boolean ={
    val matcherType = patternType.matcher(token)
    if(matcherType.matches()) return true
    return false
  }
}
