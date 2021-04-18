import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.FileRecords.open
import net.liftweb.json.{DefaultFormats, parse}

import java.util.Properties
import java.util.Collections
import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import com.github.tototoshi.csv._

import java.io.{BufferedWriter, File, FileWriter}


object ConsumerToCSV extends App {
  def test(): Unit = {

    val topic = "testtopicfile"

    val props_con = new Properties()

    props_con.put("bootstrap.servers", "localhost:9092")
    props_con.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props_con.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
    props_con.put("auto.offset.reset", "earliest")
    props_con.put("group.id", "MessagesListDrone")


    val consumer = new KafkaConsumer[String, Int](props_con)
    consumer.subscribe(Collections.singletonList("testtopicfile"))
    consumer.seekToBeginning(consumer.assignment())

    val time = System.currentTimeMillis()

    print("VERIF_0")

    val records = consumer.poll(10000).asScala
    print("VERIF_1")
    print(records)
    records.foreach(record => {
      println("VERIF_2")
      println(record)
      val writer = new BufferedWriter(new FileWriter("testdd.csv", true))
      if (scala.io.Source.fromFile("testdd.csv").getLines.size == 0) {
        writer.write("id,drone_time,lat_location,long_location,words,lastname,firstname,address,peacescore")
      }
      writer.newLine()
      implicit val formats = DefaultFormats
      val jvalue = parse(record.key())
      println(jvalue)
      val rec = jvalue.extract[DroneReport.Drone]
      val fd = rec.surround.lastname + "-" + rec.surround.firstname + "-" + rec.surround.adress + "-" + rec.surround.peacescore
      val reted = rec.words.mkString(";")

      val formreport = rec.id + "," + rec.drone_time + "," + rec.lat_location + "," + rec.long_location + "," + reted + "," + rec.surround.lastname + "," + rec.surround.firstname + "," + rec.surround.adress + "," + rec.surround.peacescore //rec.surround.map(x => x.lastname + "," + x.firstname + "," + x.adress + "," + x.peacescore)
      writer.write(formreport)


      writer.close()
    })
    print("VERIF_3")
    /*for(record <- records.iterator){
      if(record.value() > 50){

        println("Problem " + record.key() + record.value())
      }
      else {
        println("Ok " + record.value())
      }
    }*/

    print("Time " + time)
  }


}

