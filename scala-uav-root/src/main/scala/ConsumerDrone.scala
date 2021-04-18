import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.Properties
import java.util.Collections
import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter




object ConsumerDrone extends App{
  def test(): Unit = {

    val topic = "testtopic"

    val props_con = new Properties()

    props_con.put("bootstrap.servers", "localhost:9092")
    props_con.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props_con.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
    props_con.put("auto.offset.reset", "earliest")
    props_con.put("group.id", "MessagesListDrone")


    val consumer= new KafkaConsumer[String, Int](props_con)
    consumer.subscribe(Collections.singletonList("testtopic"))
    consumer.seekToBeginning(consumer.assignment())

    val time = System.currentTimeMillis()

    while(System.currentTimeMillis() - time < 15000){
      val records = consumer.poll(1000).asScala

      records.foreach(record => {
        if(record.value()>50){ println("Problem" + record.key() + " " + record.value())}
        else{println("No problem")}
      })


    }
    print("Time " + time)
  }




}
