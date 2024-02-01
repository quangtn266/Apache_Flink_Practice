package com.imooc.flink.cdc

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema
import io.debezium.data.Envelope
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord

class PKDeserializationSchema extends DebeziumDeserializationSchema[String]{
  override def deserialize(sourceRecord: SourceRecord, collector: Collector[String]): Unit = {

    val result: JSONObject = new JSONObject()

    val topic: String = sourceRecord.topic()
    val splits: Array[String] = topic.split("\\.")
    result.put("db", splits(1))
    result.put("table", splits(2))

    // before
    val value: Struct = sourceRecord.value().asInstanceOf[Struct]
    val before: Struct = value.getStruct("before")
    val beforeJSON: JSONObject = new JSONObject()
    if(before != null) {
      val schema: Schema = before.schema()
      val fields: util.List[Field] = schema.fields()

      import scala.collection.JavaConverters._
      for (field <- fields.asScala) {
        beforeJSON.put(field.name(), before.get(field))
      }
    }
    result.put("before", beforeJSON)

    // after
    val after: Struct = value.getStruct("after")
    val afterJSON: JSONObject = new JSONObject()
    if(after != null) {
      val schema: Schema = after.schema()
      val fields: util.List[Field] = schema.fields()

      import scala.collection.JavaConverters._
      for(field <- fields.asScala) {
        afterJSON.put(field.name(), after.get(field))
      }
    }
    result.put("after", afterJSON)

    // op
    val operation: Envelope.Operation = Envelope.operationFor(sourceRecord)
    result.put("op", operation)

    collector.collect(result.toString)
  }

  override def getProducedType: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
}
