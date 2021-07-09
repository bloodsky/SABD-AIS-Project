import Util.Helper
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.{Calendar, Date}
import scala.collection.mutable

object AIS {

  def main(args: Array[String]) {

    Helper.csvMaker()

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates AIS tuple
      .addSource(new SensorSource)

    val ws = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading](Duration.ofSeconds(60))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.TIMESTAMP.getTime
      })

    val Q1avgWeek: DataStreamSink[Q1ResultsReading] = sensorData
      .assignTimestampsAndWatermarks(ws)
      .filter{ t =>
        (t.LON >= -6.0 && t.LON <= 37.0) &&
          (t.LAT >= 32.0 && t.LAT <= 45.0) &&
          Helper.isMWest(t.LAT,t.LON)
      }
      .map(r => Q1ResultsReading("",r.SHIPTYPE,Helper.getSector(r.LAT,r.LON),"35",0.0,"60-69",0.0,"70-79",0.0,"Others",0.0))
      .keyBy(_.ID_CELLA)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .aggregate(new AvgWeek(), new ProcessWinWeek())
      //.writeAsCsv("Q1Week.csv")
      .addSink(new SocketSink("localhost", 9999))
      // set parallelism to 1 because only one thread can write to a socket
      .setParallelism(1)

    val Q1avgMonth: DataStreamSink[Q1ResultsReading] = sensorData
      .assignTimestampsAndWatermarks(ws)
      .filter{ t =>
        (t.LON >= -6.0 && t.LON <= 37.0) &&
          (t.LAT >= 32.0 && t.LAT <= 45.0) &&
          Helper.isMWest(t.LAT,t.LON)
      }
      .map(r => Q1ResultsReading("",r.SHIPTYPE,Helper.getSector(r.LAT,r.LON),"35",0.0,"60-69",0.0,"70-79",0.0,"Others",0.0))
      .keyBy(_.ID_CELLA)
      .window(TumblingEventTimeWindows.of(Time.days(31)))
      .aggregate(new AvgMonth(), new ProcessWinMonth())
      //.writeAsCsv("Q1Month.csv")
      .addSink(new SocketSink("localhost", 9998))
      // set parallelism to 1 because only one thread can write to a socket
      .setParallelism(1)


    val Q2rankWeek: DataStreamSink[Q2ResultReading] = sensorData
      .assignTimestampsAndWatermarks(ws)
      .filter{ t =>
        (t.LON >= -6.0 && t.LON <= 37.0) &&
        (t.LAT >= 32.0 && t.LAT <= 45.0)
      }
      .keyBy(k => Helper.getSector(k.LAT,k.LON))
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .aggregate(new RankWeek, new ProcessWinRank)
      .writeAsCsv("Q2Week.csv")
      //.addSink(new SocketSink("localhost", 9998))
      // set parallelism to 1 because only one thread can write to a socket
      //.setParallelism(1)

    val Q2rankMonth: DataStreamSink[Q2ResultReading] = sensorData
      .assignTimestampsAndWatermarks(ws)
      .filter{ t =>
        (t.LON >= -6.0 && t.LON <= 37.0) &&
          (t.LAT >= 32.0 && t.LAT <= 45.0)
      }
      .keyBy(k => Helper.getSector(k.LAT,k.LON))
      .window(TumblingEventTimeWindows.of(Time.days(31)))
      .aggregate(new RankWeek, new ProcessWinRank)
      .writeAsCsv("Q2Month.csv")
      //.addSink(new SocketSink("localhost", 9998))
      // set parallelism to 1 because only one thread can write to a socket
      //.setParallelism(1)

    env.execute("AIS processing data")
  }
}

case class IntermediateResult(ID: String, AM: String, PM: String, AM_NUM: Int, PM_NUM: Int, LAT: Double, LON: Double, TS: Date)

class ProcessWinRank extends ProcessWindowFunction[IntermediateResult,Q2ResultReading,String, TimeWindow] {

  def getWinDate(context: Context): String = {
    val date = new Date()
    date.setTime(context.window.getStart)
    val nd = date.toString.split(" ")
    nd(0)+" "+nd(1)+" "+nd(2)
  }

  val mmam = new mutable.HashMap[String, mutable.Set[Int]] with mutable.MultiMap[String, Int]
  val mmpm = new mutable.HashMap[String, mutable.Set[Int]] with mutable.MultiMap[String, Int]
  var time = 0L
  var qLocal: Q2ResultReading = Q2ResultReading("","","","","","")

  override def process(key: String, context: Context, elements: Iterable[IntermediateResult], out: Collector[Q2ResultReading]): Unit = {
    val NumShip = elements.iterator.toSeq.take(1).toString().split(",")

    if (time == 0L) {
      time = context.window.getEnd
      mmam.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(3).toInt)
      mmpm.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(4).toInt)
    } else {
      // finestra finisce e scatta al prossimo set di valori
      if (context.window.getEnd != time) {

        mmam.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(3).toInt)
        mmpm.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(4).toInt)

        val l = mmam.map(x => (x._1,x._2.sum)).toList.sortBy(x => x._2)(Ordering[Int].reverse).take(3).map(x => x._1).toArray
        val r = mmpm.map(x => (x._1,x._2.sum)).toList.sortBy(x => x._2)(Ordering[Int].reverse).take(3).map(x => x._1).toArray

        val sea = if(Helper.isMWest(elements.iterator.next().LAT,elements.iterator.next().LON)) "Occidentale"
        else "Orientale"

        val qOut = Q2ResultReading(getWinDate(context),sea,"00:00-11:59","{"+l.mkString("Array(", ", ", ")")
          .replace("Array(","").replace(")","")+"}",
          "12:00-23:59","{"+r.mkString("Array(", ", ", ")")
            .replace("Array(","").replace(")","")+"}")
        if (!qOut.equals(qLocal)) {
          qLocal = qOut
          out.collect(qOut)
        }
      } else {
        mmam.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(3).toInt)
        mmpm.addBinding(Helper.getSector(NumShip(5).toDouble,NumShip(6).replace(")","").toDouble),NumShip(4).toInt)
      }
    }
  }
}

class RankWeek extends AggregateFunction[SensorReading, IntermediateResult, IntermediateResult] {

  val cal = Calendar.getInstance()

  override def createAccumulator() = {
    IntermediateResult("","00:00-11:59","12:00-23:59",0,0,0,0,new Date)
  }

  override def add(acc: SensorReading, a: IntermediateResult) = {
    val d = cal.setTime(acc.TIMESTAMP)
    if (cal.get(Calendar.HOUR_OF_DAY) < 12) IntermediateResult(acc.SHIP_ID,"00:00-11:59","12:00-23:59",a.AM_NUM+1,a.PM_NUM,acc.LAT,acc.LON,acc.TIMESTAMP)
    else IntermediateResult(acc.SHIP_ID,"00:00-11:59","12:00-23:59",a.AM_NUM,a.PM_NUM+1,acc.LAT,acc.LON,acc.TIMESTAMP)
  }

  override def getResult(acc: IntermediateResult) = {
    IntermediateResult(acc.ID,"00:00-11:59","12:00-23:59",acc.AM_NUM,acc.PM_NUM,acc.LAT,acc.LON,acc.TS)
  }

  override def merge(a: IntermediateResult, b: IntermediateResult) = {
    IntermediateResult(a.ID,"00:00-11:59","12:00-23:59",a.AM_NUM+b.AM_NUM,a.PM_NUM+b.PM_NUM,a.LAT,a.LON,a.TS)
  }
}

class ProcessWinWeek extends ProcessWindowFunction[Q1ResultsReading,Q1ResultsReading,String, TimeWindow] {

  def getWinDate(context: Context): String = {
    val date = new Date()
    date.setTime(context.window.getStart)
    val nd = date.toString.split(" ")
    nd(0)+" "+nd(1)+" "+nd(2)
  }

  override def process(key: String, context: Context, elements: Iterable[Q1ResultsReading], out: Collector[Q1ResultsReading]): Unit = {

    val res = elements.iterator.next
    out.collect(Q1ResultsReading(getWinDate(context),res.STYPE,res.ID_CELLA,res.STYPE_35,res.AVG_35,res.STYPE_6069,res.AVG_6069,
      res.STYPE_7079,res.AVG_7079,res.STYPE_OTHERS,res.AVG_OTHERS))
  }
}
                                                                      // 35  6069 7079  OTH
class AvgWeek extends AggregateFunction[Q1ResultsReading, (Q1ResultsReading, Int, Int, Int, Int), Q1ResultsReading] {

  override def createAccumulator() =
    (Q1ResultsReading("",0,"","35",0.0,"60-69",0.0,"70-79",0.0,"Others",0.0),0,0,0,0)

  override def add(acc: Q1ResultsReading, a: (Q1ResultsReading, Int, Int, Int, Int)) = {
    val q = Q1ResultsReading("",acc.STYPE,acc.ID_CELLA,"35",acc.AVG_35,"60-69",acc.AVG_6069,"70-79",acc.AVG_7079,"Others",acc.AVG_OTHERS)
    if (acc.STYPE == 35) {
      (q,a._2+1,a._3,a._4,a._5)
    } else if (acc.STYPE >= 60 && acc.STYPE <= 69) {
      (q,a._2,a._3+1,a._4,a._5)
    } else if (acc.STYPE >= 70 && acc.STYPE <= 79) {
      (q,a._2,a._3,a._4+1,a._5)
    } else {
      (q,a._2,a._3,a._4,a._5+1)
    }
  }

  override def getResult(acc: (Q1ResultsReading, Int, Int, Int, Int)) = {
    Q1ResultsReading("",acc._1.STYPE,acc._1.ID_CELLA,"35",acc._2/7,"60-69",acc._3/7,"70-79",acc._4/7,"Others",acc._5/7)
  }

  override def merge(a: (Q1ResultsReading, Int, Int, Int, Int), b: (Q1ResultsReading, Int, Int, Int, Int)) =
    (a._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5)
}

class ProcessWinMonth extends ProcessWindowFunction[Q1ResultsReading,Q1ResultsReading,String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Q1ResultsReading], out: Collector[Q1ResultsReading]): Unit = {
    val date = new Date()
    date.setTime(context.window.getStart)
    val nd = date.toString.split(" ")
    val nnd = nd(0)+" "+nd(1)+" "+nd(2)
    val res = elements.iterator.next
    out.collect(Q1ResultsReading(nnd,res.STYPE,res.ID_CELLA,res.STYPE_35,res.AVG_35,res.STYPE_6069,res.AVG_6069,
      res.STYPE_7079,res.AVG_7079,res.STYPE_OTHERS,res.AVG_OTHERS))
  }
}

class AvgMonth extends AggregateFunction[Q1ResultsReading, (Q1ResultsReading, Int, Int, Int, Int), Q1ResultsReading] {
  override def createAccumulator() = (Q1ResultsReading("",0,"","35",0.0,"60-69",0.0,"70-79",0.0,"Others",0.0),0,0,0,0)

  override def add(acc: Q1ResultsReading, a: (Q1ResultsReading, Int, Int, Int, Int)) = {
    val q = Q1ResultsReading("",acc.STYPE,acc.ID_CELLA,"35",acc.AVG_35,"60-69",acc.AVG_6069,"70-79",acc.AVG_7079,"Others",acc.AVG_OTHERS)
    if (acc.STYPE == 35) {
      (q,a._2+1,a._3,a._4,a._5)
    } else if (acc.STYPE >= 60 && acc.STYPE <= 69) {
      (q,a._2,a._3+1,a._4,a._5)
    } else if (acc.STYPE >= 70 && acc.STYPE <= 79) {
      (q,a._2,a._3,a._4+1,a._5)
    } else {
      (q,a._2,a._3,a._4,a._5+1)
    }
  }

  override def getResult(acc: (Q1ResultsReading, Int, Int, Int, Int)) = {
    Q1ResultsReading("",acc._1.STYPE,acc._1.ID_CELLA,"35",acc._2/31,"60-69",acc._3/31,"70-79",acc._4/31,"Others",acc._5/31)
  }

  override def merge(a: (Q1ResultsReading, Int, Int, Int, Int), b: (Q1ResultsReading, Int, Int, Int, Int)) =
    (a._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5)
}