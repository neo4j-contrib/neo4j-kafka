package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.VersionUtil
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.extensions.asProperties
import streams.service.errors.ErrorData
import streams.service.errors.ErrorService
import streams.service.errors.KafkaErrorService
import streams.utils.StreamsUtils


class Neo4jSinkTask : SinkTask() {
    private val log: Logger = LoggerFactory.getLogger(Neo4jSinkTask::class.java)
    private lateinit var config: Neo4jSinkConnectorConfig
    private lateinit var neo4jSinkService: Neo4jSinkService
    private lateinit var errorService: ErrorService

    override fun version(): String {
        return VersionUtil.version(this.javaClass as Class<*>)
    }

    override fun start(map: Map<String, String>) {
        this.config = Neo4jSinkConnectorConfig(map)
        this.neo4jSinkService = Neo4jSinkService(this.config)
        this.errorService = KafkaErrorService(this.config.kafkaBrokerProperties.asProperties(),
                ErrorService.ErrorConfig.from(map.asProperties()),
                log::error)
    }

    override fun put(collection: Collection<SinkRecord>) {
        if (collection.isEmpty()) {
            return
        }
        try {
            val data = EventBuilder()
                    .withBatchSize(config.batchSize)
                    .withSinkRecords(collection)
                    .build()

            neo4jSinkService.writeData(data)
        } catch(e:Exception) {
            errorService.report(collection.map {
                ErrorData(it.topic(), it.timestamp(), it.key(), it.value(), it.kafkaPartition(), it.kafkaOffset(), this::class.java, this.config.database, e)
            })
        }
    }

    override fun stop() {
        log.info("Stop() - Neo4j Sink Service")
        StreamsUtils.ignoreExceptions({ neo4jSinkService.close() }, UninitializedPropertyAccessException::class.java)
    }
}