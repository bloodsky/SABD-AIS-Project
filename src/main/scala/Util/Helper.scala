package Util

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Locale

object Helper {

  var LAT_VALUE = BigDecimal(32.0) to BigDecimal(45.0)
  var LON_VALUE = BigDecimal(-6.0) to BigDecimal(37.0)
  var LAT_INDICES = ('A' to 'J')
  var LON_INDICES = (1 to 40)

  var SICILY_LINE_CHANNEL_LAT = 37.2
  var SICILY_LINE_CHANNEL_LON = 11.2

  var formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)

  // Occidentale
  def isMWest(LAT:Double, LON: Double) : Boolean = {
    if(LAT > SICILY_LINE_CHANNEL_LAT && LON < SICILY_LINE_CHANNEL_LON) true else false
  }

  // Orientale
  def isMEast(LAT:Double, LON: Double) : Boolean = {
    if(LAT < SICILY_LINE_CHANNEL_LAT && LON > SICILY_LINE_CHANNEL_LON) true else false
  }

  def getSector(LAT: Double, LON: Double) : String = {
    var latCounter = 0
    var lonCounter = 0
    for (i <- LAT_VALUE by 1.2) {
      if (LAT <= i) {
        for (j <- LON_VALUE by 1.1) {
          if (LON <= j) {
            if (latCounter == 10 && lonCounter == 40) return LAT_INDICES(9).toString+LON_INDICES(39).toString
            else if (latCounter == 10) return LAT_INDICES(9).toString+LON_INDICES(lonCounter).toString
            else if (lonCounter == 40) return LAT_INDICES(latCounter).toString+LON_INDICES(39).toString
            else return LAT_INDICES(latCounter).toString+LON_INDICES(lonCounter).toString
          }
          lonCounter = lonCounter + 1
        }
      }
      latCounter = latCounter + 1
    }
    "Error!"
  }

  def csvMaker() : Unit = {

    if (!Files.exists(Paths.get("src/dataset/ordered.txt"))) {
      println("Start making file ...")
      var format1 = new SimpleDateFormat("dd-MM-yy HH:mm")
      var format2 = new SimpleDateFormat("dd/MM/yy HH:mm")

      val file = new File("src/dataset/ordered.txt")
      val bw = new BufferedWriter(new FileWriter(file))

      val bufferedSource = io.Source.fromFile("src/dataset/prj2_dataset.csv")
      val map = bufferedSource.getLines().drop(1).map { line =>
        val elems = line.split(",")
        if (elems(7).contains("/")) (format2.parse(elems(7)),line)
        else (format1.parse(elems(7)),line)
      }.toSeq.sortBy(_._1)
        .map { e =>
          val elems = e._2.split(",")
          (elems(0),
            elems(1).toInt,
            elems(2).toDouble,
            elems(3).toDouble,
            elems(4).toDouble,
            elems(5).toInt,
            if (elems(6).isEmpty) 0
            else elems(6).toInt,
            e._1,
            elems(8),
            if (elems(9).isEmpty || elems(9).contains("""""")) 0
            else elems(9).toInt,
            elems(10))
        }
      map.foreach(row => bw.write(row.toString().replaceAll("[()]", "")+"\n"))
      bw.close()
      println("Closing buffer writer ... end.")
    }
  }
}
