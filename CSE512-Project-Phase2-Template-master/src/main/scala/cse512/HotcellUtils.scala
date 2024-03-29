package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def CalculateNeighbors(inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int =
  {
    var num_cells = 0     
    if (inputX == minX || inputX == maxX) 
    {
      num_cells += 1
    }
    if (inputY == minY || inputY == maxY) 
    {
        num_cells += 1
    }
    if (inputZ == minZ || inputZ == maxZ) 
    {
        num_cells += 1
    }
    if (num_cells == 1) 
    {
      return 17;
    } 
    else if (num_cells == 2) 
    {
      return 11;
    } 
    else if (num_cells == 3) 
    {
      return 7;
    }    
    return 26;
  }

  def calculateGScore(num_adjCells: Int, sum_adjCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, SD: Double): Double =
  {
    val numerator = (sum_adjCells.toDouble - (mean * num_adjCells.toDouble))
    val denominator = SD * math.sqrt((((numCells.toDouble * num_adjCells.toDouble) - (num_adjCells.toDouble * num_adjCells.toDouble)) / (numCells.toDouble - 1.0).toDouble).toDouble).toDouble    
    return (numerator / denominator).toDouble
  }
  
}
