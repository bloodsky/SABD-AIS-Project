import java.sql.Timestamp
import java.util.Date

/** Case class to hold the SensorReading data. */
case class SensorReading(SHIP_ID: String,
                         SHIPTYPE: Int,
                         SPEED: Double,
                         LON: Double,
                         LAT: Double,
                         COURSE: Int,
                         HEADING: Int,
                         TIMESTAMP: Date,
                         DEPARTURE_PORT_NAME: String,
                         REPORTED_DRAUGHT: Int,
                         TRIP_ID: String)

//case class SensorReading(id: String, timestamp: Long, temperature: Double)