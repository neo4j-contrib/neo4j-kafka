package streams.kafka.connect.utils

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.driver.Record
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship
import streams.extensions.asStreamsMap
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.utils.JSONUtils
import streams.service.StreamsSinkEntity
import java.time.temporal.TemporalAccessor

fun SinkRecord.toStreamsSinkEntity(): StreamsSinkEntity = StreamsSinkEntity(
        convertData(this.key(),true),
        convertData(this.value()))

private val converter = Neo4jValueConverter()

private fun convertData(data: Any?, stringWhenFailure :Boolean = false) = when (data) {
    is Struct -> converter.convert(data)
    null -> null
    else -> JSONUtils.readValue<Any>(data, stringWhenFailure)
}

fun Record.asJsonString(): String = JSONUtils.writeValueAsString(this.asMap())

fun Record.schema(asMap: Map<String, Any> = this.asMap()): Schema {
    val structBuilder = SchemaBuilder.struct()
    asMap.forEach { structBuilder.field(it.key, neo4jValueSchema(it.value)) }
    return structBuilder.build()
}

fun Record.asStruct(): Struct {
    val asMap = this.asMap()
    val schema = schema(asMap)
    val struct = Struct(schema)
    schema.fields().forEach {
        struct.put(it, neo4jToKafka(it.schema(), asMap[it.name()]))
    }
    struct
    return struct
}

private fun neo4jToKafka(schema: Schema, value: Any?): Any? = when (schema.type()) {
    Schema.Type.ARRAY -> when (value) {
        is Collection<*> -> value.map { neo4jToKafka(neo4jValueSchema(it), it) }
        is Array<*> -> value.map { neo4jToKafka(neo4jValueSchema(it), it) }.toTypedArray()
        else -> throw IllegalArgumentException("For Schema.Type.ARRAY we support only Collection and Array")
    }
    Schema.Type.MAP -> when (value) {
        is Map<*, *> -> value.mapValues { neo4jToKafka(neo4jValueSchema(it), it) }
        else -> throw IllegalArgumentException("For Schema.Type.MAP we support only Map")
    }
    Schema.Type.STRUCT -> when (value) {
        is Map<*, *> -> {
            val struct = Struct(schema)
            schema.fields().forEach {
                struct.put(it, neo4jToKafka(it.schema(), value[it.name()]))
            }
            struct
        }
        is Point -> {
            val map = JSONUtils.readValue<Map<String, Any>>(value)
            neo4jToKafka(schema, map)
        }
        is Node -> {
            val map = value.asStreamsMap()
            neo4jToKafka(schema, map)
        }
        is Relationship -> {
            val map = value.asStreamsMap()
            neo4jToKafka(schema, map)
        }
        else -> throw IllegalArgumentException("For Schema.Type.STRUCT we support only Map and Point")
    }
    else -> when (value) {
        null -> null
        is TemporalAccessor -> {
            val temporalValue = JSONUtils.readValue<String>(value)
            neo4jToKafka(schema, temporalValue)
        }
        else -> when {
            Schema.Type.STRING == schema.type() && value !is String -> value.toString()
            else -> value
        }
    }
}

private fun neo4jValueSchema(value: Any?): Schema = when (value) {
    is Long -> Schema.OPTIONAL_INT64_SCHEMA
    is Double -> Schema.OPTIONAL_FLOAT64_SCHEMA
    is Boolean -> Schema.OPTIONAL_BOOLEAN_SCHEMA
    is Collection<*> -> {
        (value.firstOrNull()?.let {
            SchemaBuilder.array(neo4jValueSchema(it))
        } ?: SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)).build()
    }
    is Array<*> -> {
        (value.firstOrNull()?.let {
            SchemaBuilder.array(neo4jValueSchema(it))
        } ?: SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)).build()
    }
    is Map<*, *> -> {
        if (value.isEmpty()) {
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build()
        } else {
            val valueTypes = value.values.mapNotNull { elem -> elem?.let{ it::class.java.simpleName } }
                    .toSet()
            if (valueTypes.size == 1) {
                SchemaBuilder.map(Schema.STRING_SCHEMA, neo4jValueSchema(value.values.first()))
            } else {
                val structMap = SchemaBuilder.struct()
                value.forEach { structMap.field(it.key.toString(), neo4jValueSchema(it.value)) }
                structMap.build()
            }
        }
    }
    is Point -> neo4jValueSchema(JSONUtils.readValue<Map<String, Any>>(value))
    is Node -> neo4jValueSchema(value.asStreamsMap())
    is Relationship -> neo4jValueSchema(value.asStreamsMap())
    else -> Schema.OPTIONAL_STRING_SCHEMA
}
