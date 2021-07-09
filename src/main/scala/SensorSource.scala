import Util.Helper
import org.apache.flink.streaming.api.functions.source.SourceFunction

class SensorSource extends SourceFunction[SensorReading] {

  var replayTime = 10

  def myCollect(ctx: SourceFunction.SourceContext[SensorReading], elems: Array[String]): Unit = {
    ctx.collect(SensorReading(elems(0),
      elems(1).toInt,
      elems(2).toDouble,
      elems(3).toDouble,
      elems(4).toDouble,
      elems(5).toInt,
      elems(6).toInt,
      Helper.formatter.parse(elems(7)),
      elems(8),
      elems(9).toInt,
      elems(10)))
    Thread.sleep(replayTime)
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val bufferedSource = io.Source.fromFile("src/dataset/ordered.txt")

    bufferedSource.getLines().foreach { line =>
      val elems = line.split(",")
      myCollect(ctx,elems)
    }
    /*val rand = new Random()

    // initialize sensor ids and temperatures
    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (1 * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // emit data until being canceled
    while (true) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach( t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }*/
  }

  override def cancel(): Unit = {
  }
}
