package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rectPoints = queryRectangle.split(",")
    var firstX = rectPoints(0).trim().toDouble
    var firstY = rectPoints(1).trim().toDouble
    var secondX = rectPoints(2).trim().toDouble
    var secondY = rectPoints(3).trim().toDouble

    var point = pointString.split(",")
    var pointX = point(0).trim().toDouble
    var pointY = point(1).trim().toDouble

    var largeX = math.max(firstX, secondX)
    var largeY = math.max(firstY, secondY)
    var smallX = math.min(firstX, secondX)
    var smallY = math.min(firstY, secondY)

    if (pointX >= smallX && pointY >= smallY && pointX <= largeX && pointY <= largeY)
    {
      return true
    } 
    else
    {
      return false
    }
    // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
