import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration

class MetricsWizard extends RichMapFunction[String,String] {
  @transient private var valueToExpose = 0

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext
      .getMetricGroup
      .gauge[Int, ScalaGauge[Int]]("GaugeLatency", ScalaGauge[Int]( () => valueToExpose ) )
  }

  override def map(value: String): String = {
    valueToExpose += 1
    value
  }
}
