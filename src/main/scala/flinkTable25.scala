import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment

object flinkTable25 extends App {

   val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val temperature = new FlinkKafkaConsumer010("broadcast", new SimpleStringSchema, properties)

    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(env, fsSettings)

    val stream: DataStream[Event] = env.
      addSource(temperature)
      .map { v =>
        val t = v.split(",")
        Event(t.head.trim.toInt, t(1), t(2).trim.toDouble)
      }
//      .map(data => (data(0).trim.toInt, data(1), data(2).trim.toDouble))
 /* val streamInfo = new RowTypeInfo(
    Types.INT,
    Types.STRING,
    Types.DOUBLE
  )

  val parsedStreamData: DataStream[Row] = stream.map(x => x.split(","))
    .map(data => (data(0).toInt, data(1), data(2).toDouble))(streamInfo)


        print(stream.getClass)*/
    val tbl = tenv.registerDataStream("event", stream, 'ID, 'locationID, 'temp)

    val pattern = new FlinkKafkaConsumer010("pattern", new SimpleStringSchema(), properties)
    val streamPat: DataStream[Temp] = env.
      addSource(pattern)
      .map {
        v =>
          val tp = v.split(",")
          Temp(tp.head.trim.toInt, tp(1), tp(2).trim.toDouble)
      }

//      .map(patt => (patt(0).trim.toInt, patt(1), patt(2).trim.toDouble))

    val tbl1 = tenv.registerDataStream("patt", streamPat, 'ID, 'locationIDPat, 'temperature)

//    val res = tenv.sqlQuery(
//      """
//        |select *
//        |FROM kafka AS k,
//        |flink AS f
//        |where k.kID = f.fID
//        |""".stripMargin
//    )
val res: Table = tenv.sqlQuery(
 """
   |select event.ID,event.locationID, event.temp
   |from event
   |JOIN patt
   |ON event.ID = patt.ID
   |AND event.temp >= patt.temperature
   |""".stripMargin
)

//  println(res.getClass)

    res.toAppendStream[Event].print("Alert for these location")

    env.execute()

      case class Event(ID: Int, locationID: String, temp: Double)
      case class Temp(ID: Int, locationIDPat: String, temperature: Double)

}