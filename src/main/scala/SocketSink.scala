import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.io.PrintStream
import java.net.{InetAddress, Socket}

class SocketSink(val host: String, val port: Int) extends RichSinkFunction[Q1ResultsReading] {
  var socket: Socket = _
  var writer: PrintStream = _

  override def open(config: Configuration): Unit = {
    // open socket and writer
    socket = new Socket(InetAddress.getByName(host), port)
    writer = new PrintStream(socket.getOutputStream)
  }

  override def invoke(value: Q1ResultsReading): Unit = {
    // write sensor reading to socket
    writer.println(value.toString)
    writer.flush()
  }

  override def close(): Unit = {
    // close writer and socket
    writer.close()
    socket.close()
  }
}
