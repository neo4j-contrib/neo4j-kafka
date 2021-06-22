package streams.kafka.connect.source

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.neo4j.driver.Record
import streams.kafka.connect.utils.asJsonString
import streams.kafka.connect.utils.asStruct
import kotlin.properties.Delegates

class SourceRecordBuilder {

    private lateinit var topic: String

    private lateinit var streamingProperty: String

    private var timestamp by Delegates.notNull<Long>()

    private lateinit var sourcePartition: Map<String, Any>

    private lateinit var record: Record

    private var enforceSchema: Boolean = false

    fun withTopic(topic: String): SourceRecordBuilder {
        this.topic = topic
        return this
    }

    fun withStreamingProperty(streamingProperty: String): SourceRecordBuilder {
        this.streamingProperty = streamingProperty
        return this
    }

    fun withTimestamp(timestamp: Long): SourceRecordBuilder {
        this.timestamp = timestamp
        return this
    }

    fun withSourcePartition(sourcePartition: Map<String, Any>): SourceRecordBuilder {
        this.sourcePartition = sourcePartition
        return this
    }

    fun withRecord(record: Record): SourceRecordBuilder {
        this.record = record
        return this
    }

    fun withEnforceSchema(enforceSchema: Boolean): SourceRecordBuilder {
        this.enforceSchema = enforceSchema
        return this
    }

    fun build(): SourceRecord {
        val sourceOffset = mapOf("property" to streamingProperty.ifBlank { "undefined" },
                "value" to timestamp)
        val (struct, schema) = when (enforceSchema) {
            true -> {
                val st = record.asStruct()
                val sc = st.schema()
                st to sc
            }
            else -> record.asJsonString() to Schema.STRING_SCHEMA
        }
        return SourceRecord(sourcePartition, sourceOffset, topic, schema, struct, schema, struct)
    }
}
