package streams.kafka.connect.source

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import streams.kafka.connect.common.ConnectorType
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.kafka.connect.utils.PropertiesUtil

enum class SourceType {
    QUERY, LABELS, RELATIONSHIP
}

enum class StreamingFrom {
    ALL, NOW, LAST_COMMITTED;

    fun value() = when (this) {
        ALL -> -1
        else -> System.currentTimeMillis()
    }
}

class Neo4jSourceConnectorConfig(originals: Map<*, *>): Neo4jConnectorConfig(config(), originals, ConnectorType.SOURCE) {

    val topic: String = getString(TOPIC)

    val labels: Array<String>
    val relationship: String
    val query: String

    val partitions: List<Int> = (1 .. getInt(PARTITIONS)).toList()

    val streamingFrom: StreamingFrom = StreamingFrom.valueOf(getString(STREAMING_FROM))
    val streamingProperty: String = getString(STREAMING_PROPERTY)

    val sourceType: SourceType = SourceType.valueOf(getString(SOURCE_TYPE))

    val pollInterval: Int = getInt(STREAMING_POLL_INTERVAL)

    val enforceSchema: Boolean = getBoolean(ENFORCE_SCHEMA)

    init {
        when (sourceType) {
            SourceType.QUERY -> {
                query = getString(SOURCE_TYPE_QUERY)
                if (query.isNullOrBlank()) {
                    throw ConfigException("You need to define: $SOURCE_TYPE_QUERY")
                }
                labels = emptyArray()
                relationship = ""
            }
            else -> {
                throw ConfigException("Supported source query types are: ${SourceType.QUERY}")
            }
        }
    }

    fun sourcePartition() = when (sourceType) {
        SourceType.QUERY -> mapOf("database" to this.database,
                "type" to "query", "query" to query, "partition" to 1)
        else -> throw UnsupportedOperationException("Supported source query types are: ${SourceType.QUERY}")
    }

    companion object {
        const val PARTITIONS = "partitions"
        const val TOPIC = "topic"
        const val STREAMING_FROM = "neo4j.streaming.from"
        const val ENFORCE_SCHEMA = "neo4j.enforce.schema"
        const val STREAMING_PROPERTY = "neo4j.streaming.property"
        const val STREAMING_POLL_INTERVAL = "neo4j.streaming.poll.interval.msecs"
        const val SOURCE_TYPE = "neo4j.source.type"
        const val SOURCE_TYPE_QUERY = "neo4j.source.query"
        const val SOURCE_TYPE_LABELS = "neo4j.source.labels"
        const val SOURCE_TYPE_RELATIONSHIP = "neo4j.source.relationship"

        fun config(): ConfigDef = Neo4jConnectorConfig.config()
                .define(ConfigKeyBuilder.of(ENFORCE_SCHEMA, ConfigDef.Type.BOOLEAN)
                        .documentation(PropertiesUtil.getProperty(ENFORCE_SCHEMA)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue(false)
                        .validator(ConfigDef.NonNullValidator())
                        .build())
                .define(ConfigKeyBuilder.of(STREAMING_POLL_INTERVAL, ConfigDef.Type.INT)
                        .documentation(PropertiesUtil.getProperty(STREAMING_POLL_INTERVAL)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue(10000)
                        .validator(ConfigDef.Range.atLeast(1))
                        .build())
                .define(ConfigKeyBuilder.of(STREAMING_PROPERTY, ConfigDef.Type.STRING)
                        .documentation(PropertiesUtil.getProperty(STREAMING_PROPERTY)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue("")
//                        .validator(ConfigDef.NonEmptyString())
                        .build())
                .define(ConfigKeyBuilder.of(TOPIC, ConfigDef.Type.STRING)
                        .documentation(PropertiesUtil.getProperty(TOPIC)).importance(ConfigDef.Importance.HIGH)
                        .validator(ConfigDef.NonEmptyString())
                        .build())
                .define(ConfigKeyBuilder.of(PARTITIONS, ConfigDef.Type.INT)
                        .documentation(PropertiesUtil.getProperty(PARTITIONS)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue(1)
                        .validator(ConfigDef.Range.atLeast(1))
                        .build())
                .define(ConfigKeyBuilder.of(STREAMING_FROM, ConfigDef.Type.STRING)
                        .documentation(PropertiesUtil.getProperty(STREAMING_FROM)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue(StreamingFrom.NOW.toString())
                        .validator(ValidEnum.of(StreamingFrom::class.java))
                        .build())
                .define(ConfigKeyBuilder.of(SOURCE_TYPE, ConfigDef.Type.STRING)
                        .documentation(PropertiesUtil.getProperty(SOURCE_TYPE)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue(SourceType.QUERY.toString())
                        .validator(ValidEnum.of(SourceType::class.java))
                        .build())
                .define(ConfigKeyBuilder.of(SOURCE_TYPE_QUERY, ConfigDef.Type.STRING)
                        .documentation(PropertiesUtil.getProperty(SOURCE_TYPE_QUERY)).importance(ConfigDef.Importance.HIGH)
                        .defaultValue("")
                        .build())
    }
}