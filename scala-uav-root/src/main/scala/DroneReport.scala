
import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.immutable.HashMap

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit


object DroneReport {

  case class Identity(
                       firstname: String,
                       lastname: String,
                       adress: String,
                       peacescore: Int
                     )

  case class Drone(
                    id : String,
                    drone_time : String,
                    lat_location : Double,
                    long_location : Double,
                    words : List[String],
                    surround : Identity
                  )

}
