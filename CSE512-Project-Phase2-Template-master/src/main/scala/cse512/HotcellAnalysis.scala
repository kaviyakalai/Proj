package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
  pickupInfo.createOrReplaceTempView("chosenCellvalues")

  pickupInfo = spark.sql("select x, y, z, count(*) as num_hotzone from chosenCellvalues group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("chosenHotzone")
  
  val sum_chosenCells = spark.sql("select sum(num_hotzone) as sum_hotzone from chosenHotzone")
  sum_chosenCells.createOrReplaceTempView("sum_chosenCells")
  
  val mean = (sum_chosenCells.first().getLong(0).toDouble / numCells.toDouble).toDouble
  
  spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))
  
  val squares_sum = spark.sql("select sum(squared(num_hotzone)) as squares_sum from chosenHotzone")
  squares_sum.createOrReplaceTempView("squares_sum")
  
  val SD = scala.math.sqrt(((squares_sum.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
  
  spark.udf.register("NeighborCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.CalculateNeighbors(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
  
  val NeighborCells = spark.sql("select NeighborCells(one.x, one.y, one.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as NeighborCount," + "one.x as x, one.y as y, one.z as z, " + "sum(two.num_hotzone) as sum_hotzone " + "from chosenHotzone as one, chosenHotzone as one " + "where (two.x = one.x+1 or two.x = one.x or two.x = one.x-1) " + "and (two.y = one.y+1 or two.y = one.y or two.y = one.y-1) " + "and (two.z = one.z+1 or two.z = one.z or two.z = one.z-1) " + "group by one.z, one.y, one.x " + "order by one.z, one.y, one.x")
  NeighborCells.createOrReplaceTempView("NeighborCells")
    
  spark.udf.register("GScore", (NeighborCount: Int, sum_hotzone: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, SD: Double) => ((HotcellUtils.calculateGScore(NeighborCount, sum_hotzone, numCells, x, y, z, mean, SD))))
    
  pickupInfo = spark.sql("select GScore(NeighborCount, sum_hotzone, "+ numCells + ", x, y, z," + mean + ", " + SD + ") as g_score, x, y, z from NeighborCells order by g_score desc");
  pickupInfo.createOrReplaceTempView("GScore")
    
  pickupInfo = spark.sql("select x, y, z from GScore")
  pickupInfo.createOrReplaceTempView("lastPickupInfo")

  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
