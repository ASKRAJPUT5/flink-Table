
import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment

object RealTimeAlert extends App {

  /**
   * @param ID
   * @param locationID
   * @param temp
   *             case class for the table columns made (event Table)
   */
  case class Event(ID: Int, locationID: String, temp: Double)

  /**
   * @param ID
   * @param locationIDPat
   * @param temperature
   *                    case class for the table columns made (Temp)
   */
  case class Temp(ID: Int, locationIDPat: String, temperature: Double)


  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  /**
   * This temperature val will create a Kafka Stream for flink to consume with topic -broadcast, and is read as simple String schema
   */
  val temperature = new FlinkKafkaConsumer010("broadcast", new SimpleStringSchema, properties)

  /**
   * Environment Settings, Stream Execution Environment and Stream Table environment are made
   */
  val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tenv = StreamTableEnvironment.create(env, fsSettings)

  /**
   *Data Stream for the Event data namely stream(data on which patterns are to be matched) is made
   */
  val stream: DataStream[Event] = env.
    addSource(temperature)
    .map { v =>
      val t = v.split(",")
      Event(t.head.trim.toInt, t(1), t(2).trim.toDouble)
    }

  /**
   *stream DataStream is converted into a table by registering it with the name event
   */
  val tbl = tenv.registerDataStream("event", stream, 'ID, 'locationID, 'temp)

  /**
   * This pattern val will create a Kafka Stream for flink to consume with topic -pattern, and is read as simple String schema
   */
  val pattern = new FlinkKafkaConsumer010("pattern", new SimpleStringSchema(), properties)

  /**
   *Data Stream for the pattern data namely streamPat(data which will create alert if got matched with any Event (stream)) is made
   */
  val streamPat: DataStream[Temp] = env.
    addSource(pattern)
    .map {
      v =>
        val tp = v.split(",")
        Temp(tp.head.trim.toInt, tp(1), tp(2).trim.toDouble)
    }

  /**
   * streamPat DataStream is converted into a table by registering it with the name patt
   */
  val tbl1 = tenv.registerDataStream("patt", streamPat, 'ID, 'locationIDPat, 'temperature)

  /**
   *Join is performed on the two table namely -event and pat , the continuous query made will check the ID and
   * if the ID's are same it'll check whether the temp of the location of particular ID has temperature more than or
   * equal to the predefined temperatures stated for all the particular locations
   */
  val res: Table = tenv.sqlQuery(
    """
      |select event.ID,event.locationID, event.temp
      |from event
      |JOIN patt
      |ON event.ID = patt.ID
      |AND event.temp >= patt.temperature
      |""".stripMargin
  )

  res.toAppendStream[Event].print("Alert for these location")

  /**
   * Used to execute the environment , so that the code could be run
   */
  env.execute()


}

