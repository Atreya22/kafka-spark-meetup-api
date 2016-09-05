import java.net.URL
import java.util.Properties

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import kafka.tools.ConsoleProducer.ProducerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.concurrent.Future


object Producer {
  def main(args: Array[String]): Unit = {

    /* Meetup API JSON Generator */
    val url = new URL("http://stream.meetup.com/2/rsvps")
    val conn = url.openConnection()
    conn.addRequestProperty("User-Agent",
      "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)")
    val jsonFactory = new JsonFactory(new ObjectMapper)
    val parser = jsonFactory.createParser(conn.getInputStream)



    /* Producer Properties */
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("broker.list", "localhost:9092")
    props.put("group.id", "None")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")


    val kafkaProducer = new KafkaProducer[String,String](props)
    while (parser.nextToken()!=null)
      {
        val record = parser.readValueAsTree().toString()
        println(record)
        val producerRecord = new ProducerRecord[String, String]("test", record)
        kafkaProducer.send(producerRecord)
      }

    kafkaProducer.close()
//      for( i <- 1 to 100){
//        val metaF: Future[RecordMetadata] = kafkaProducer.send(new ProducerRecord[String, String]("test", Integer.toString(i), Integer.toString(i)))
//        val meta = metaF.get() // blocking!
//        val msgLog =
//        s"""
//           |offset    = ${meta.offset()}
//           |partition = ${meta.partition()}
//           |topic     = ${meta.topic()}
//     """.stripMargin
//        println(msgLog)
//      }



  }

}
