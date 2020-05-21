package streams

import org.junit.Test
import org.neo4j.kernel.configuration.Config
import streams.service.TopicType
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StreamsSinkConfigurationTest {

    @Test
    fun shouldReturnDefaultConfiguration() {
        val default = StreamsSinkConfiguration()
        testDefaultConf(default)
    }

    @Test
    fun shouldReturnConfigurationFromMap() {
        val prefixes = StreamsSinkConfigurationConstants
        val pollingInterval = "10"
        val topic = "topic-neo"
        val cdctopic = "cdctopic"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val customLabel = "CustomLabel"
        val customId = "customId"
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", cdctopic)
                .withSetting("streams.sink.topic.cdc.sourceId.labelName", customLabel)
                .withSetting("streams.sink.topic.cdc.sourceId.idName", customId)
                .build()
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, pollingInterval, topic, topicValue)
        assertFalse { streamsConfig.enabled }
        assertEquals(setOf(cdctopic), streamsConfig.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsConfig.sourceIdStrategyConfig.labelName)
        assertEquals(customId, streamsConfig.sourceIdStrategyConfig.idName)
        assertEquals(prefixes.EVENT_PREFIX_TIMESTAMP_DEFAULT, streamsConfig.eventPrefixTimestamp)
        assertEquals(prefixes.EVENT_PREFIX_HEADERS_DEFAULT, streamsConfig.eventPrefixHeaders)
        assertEquals(prefixes.EVENT_PREFIX_KEY_DEFAULT, streamsConfig.eventPrefixKey)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedTopics() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.topic.pattern.node.nodePatternTopic", "User{!userId,name,surname,address.city}")
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", topic)
                .build()
        StreamsSinkConfiguration.from(config)
    }

    @Test(expected = RuntimeException::class)
    fun shouldFailWithCrossDefinedCDCTopics() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", topic)
                .withSetting("streams.sink.topic.cdc.schema", topic)
                .build()
        StreamsSinkConfiguration.from(config)
    }

    @Test
    fun shouldReturnConfigurationFromMapWithExtendedFields() {
        val prefixes = StreamsSinkConfigurationConstants
        val customTimestamp = "__fecha__"
        val customHeader = "__cabecera__"
        val customKey = "__clave__"
        val pollingInterval = "10"
        val topic = "topic-neo"
        val cdctopic = "cdctopic"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val customLabel = "CustomLabel"
        val customId = "customId"
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("streams.sink.enabled", "false")
                .withSetting("streams.sink.topic.cdc.sourceId", cdctopic)
                .withSetting("streams.sink.topic.cdc.sourceId.labelName", customLabel)
                .withSetting("streams.sink.topic.cdc.sourceId.idName", customId)
                .withSetting(prefixes.STREAMS_CONFIG_PREFIX + prefixes.EVENT_PREFIX_TIMESTAMP, customTimestamp)
                .withSetting(prefixes.STREAMS_CONFIG_PREFIX + prefixes.EVENT_PREFIX_HEADERS, customHeader)
                .withSetting(prefixes.STREAMS_CONFIG_PREFIX + prefixes.EVENT_PREFIX_KEY, customKey)
                .build()
        val streamsConfig = StreamsSinkConfiguration.from(config)
        testFromConf(streamsConfig, pollingInterval, topic, topicValue)
        assertFalse { streamsConfig.enabled }
        assertEquals(setOf(cdctopic), streamsConfig.topics.asMap()[TopicType.CDC_SOURCE_ID])
        assertEquals(customLabel, streamsConfig.sourceIdStrategyConfig.labelName)
        assertEquals(customId, streamsConfig.sourceIdStrategyConfig.idName)
        assertEquals(customTimestamp, streamsConfig.eventPrefixTimestamp)
        assertEquals(customHeader, streamsConfig.eventPrefixHeaders)
        assertEquals(customKey, streamsConfig.eventPrefixKey)
    }

    companion object {
        fun testDefaultConf(default: StreamsSinkConfiguration) {
            assertEquals(emptyMap(), default.topics.cypherTopics)
            assertEquals(10000, default.sinkPollingInterval)
        }
        fun testFromConf(streamsConfig: StreamsSinkConfiguration, pollingInterval: String, topic: String, topicValue: String) {
            assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
            assertEquals(1, streamsConfig.topics.cypherTopics.size)
            assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
        }
    }

}