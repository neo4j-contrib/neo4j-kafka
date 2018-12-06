package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.configuration.Config


private object StreamsSinkConfigurationConstants {
    const val STREAMS_CONFIG_PREFIX: String = "streams."
    const val STREAMS_SINK_TOPIC_CYPHER_PREFIX: String = "sink.topic.cypher."
    const val ENABLED = "sink.enabled"
    const val PROCEDURES_ENABLED = "procedures.enabled"
}

data class StreamsSinkConfiguration(val enabled: Boolean = true,
                                    val proceduresEnabled: Boolean = true,
                                    val sinkPollingInterval: Long = Long.MAX_VALUE,
                                    val topics: Map<String, String> = emptyMap()) {

    companion object {
        fun from(cfg: Config): StreamsSinkConfiguration {
            return from(cfg.raw)
        }

        fun from(cfg: Map<String, String>): StreamsSinkConfiguration {
            val default = StreamsSinkConfiguration()
            val config = cfg
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX) }
                    .mapKeys { it.key.substring(StreamsSinkConfigurationConstants.STREAMS_CONFIG_PREFIX.length) }
            val topics = config
                    .filterKeys { it.startsWith(StreamsSinkConfigurationConstants.STREAMS_SINK_TOPIC_CYPHER_PREFIX) }
                    .mapKeys { it.key.replace(StreamsSinkConfigurationConstants.STREAMS_SINK_TOPIC_CYPHER_PREFIX, StringUtils.EMPTY) }
            return default.copy(enabled = config.getOrDefault(StreamsSinkConfigurationConstants.ENABLED, default.enabled).toString().toBoolean(),
                    proceduresEnabled = config.getOrDefault(StreamsSinkConfigurationConstants.PROCEDURES_ENABLED, default.proceduresEnabled)
                            .toString().toBoolean(),
                    sinkPollingInterval = config.getOrDefault("sink.polling.interval", default.sinkPollingInterval).toString().toLong(),
                    topics = topics)
        }
    }

}