package streams.kafka.connect.source

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.neo4j.driver.Record
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.utils.StreamsUtils
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference


class Neo4jSourceService(private val config: Neo4jSourceConnectorConfig, offsetStorageReader: OffsetStorageReader): AutoCloseable {

    private val log: Logger = LoggerFactory.getLogger(Neo4jSourceService::class.java)

    private val driver = config.createDriver()

    private val queue: BlockingQueue<SourceRecord> = LinkedBlockingQueue()
    private val error: AtomicReference<Throwable> = AtomicReference(null)

    private val sourcePartition = config.sourcePartition()

    private val lastCheck: AtomicLong by lazy {
        val offset = offsetStorageReader.offset(sourcePartition) ?: emptyMap()
        // if the user wants to recover from LAST_COMMITTED
        val startValue = if (config.streamingFrom == StreamingFrom.LAST_COMMITTED
                && offset["value"] != null && offset["property"] == config.streamingProperty) {
            log.info("Resuming offset $offset, the ${Neo4jSourceConnectorConfig.STREAMING_FROM} value is ignored")
            offset["value"] as Long
        } else {
            if (config.streamingFrom == StreamingFrom.LAST_COMMITTED) {
                log.info("You provided ${Neo4jSourceConnectorConfig.STREAMING_FROM}: ${config.streamingFrom} but no offset has been found, we'll start to consume from NOW")
            } else {
                log.info("No offset to resume, we'll the provided value of ${Neo4jSourceConnectorConfig.STREAMING_FROM}: ${config.streamingFrom}")
            }
            config.streamingFrom.value()
        }
        AtomicLong(startValue)
    }

    private val sessionConfig = config.createSessionConfig()
    private val transactionConfig = config.createTransactionConfig()

    private val pollInterval = config.pollInterval.toLong()
    private val isStreamingPropertyDefined = config.streamingProperty.isNotBlank()
    private val streamingProperty = config.streamingProperty.ifBlank { "undefined" }

    private val job: Job = GlobalScope.launch(Dispatchers.IO) {
        var lastCheckHadResult = false
        while (isActive) {
            try {
                // if the user doesn't set the streaming property we fallback to an
                // internal mechanism
                if (!isStreamingPropertyDefined) {
                    // we update the lastCheck property only if the last loop round
                    // returned results otherwise we stick to the old value
                    if (lastCheckHadResult) {
                        lastCheck.set(System.currentTimeMillis() - pollInterval)
                    }
                }
                driver.session(sessionConfig).readTransaction({ tx ->
                    val result = tx.run(config.query, mapOf("lastCheck" to lastCheck.get()))
                    lastCheckHadResult = result.hasNext()
                    result.forEach { record ->
                        try {
                            val sourceRecord = toSourceRecord(record)
                            queue.put(sourceRecord)
                        } catch (e: Exception) {
                            setError(e)
                        }
                    }
                }, transactionConfig)
                delay(pollInterval)
            } catch (e: Exception) {
                setError(e)
            }
        }
    }

    private fun toSourceRecord(record: Record): SourceRecord {
        val thisValue = computeLastTimestamp(record)
        return SourceRecordBuilder()
            .withRecord(record)
            .withTopic(config.topic)
            .withSourcePartition(sourcePartition)
            .withStreamingProperty(streamingProperty)
            .withEnforceSchema(config.enforceSchema)
            .withTimestamp(thisValue)
            .build()
    }

    private fun computeLastTimestamp(record: Record) = if (isStreamingPropertyDefined) {
        val value = record.get(config.streamingProperty).asLong()
        lastCheck.getAndUpdate { oldValue ->
            if (oldValue >= value) {
                oldValue
            } else {
                value
            }
        }
        value
    } else {
        lastCheck.get()
    }

    private fun checkError() {
        val fatalError = error.getAndSet(null)
        if (fatalError != null) {
            throw ConnectException(fatalError)
        }
    }

    fun poll(): List<SourceRecord>? {
        checkError()
        // Block until at least one item is available or until the
        // courtesy timeout expires, giving the framework a chance
        // to pause the connector.
        val firstEvent = queue.poll(1, TimeUnit.SECONDS)
        if (firstEvent == null) {
            log.debug("Poll returns 0 results")
            return null // Looks weird, but caller expects it.
        }

        val events = mutableListOf<SourceRecord>()
        return try {
            events.add(firstEvent)
            queue.drainTo(events, config.batchSize - 1)
            log.info("Poll returns {} result(s)", events.size)
            events
        } catch (e: Exception) {
            setError(e)
            null
        }
    }

    private fun setError(e: Exception) {
        if (e !is CancellationException) {
            if (error.compareAndSet(null, e)) {
                log.error("Error:", e)
            }
        }
    }

    override fun close() {
        runBlocking { job.cancelAndJoin() }
        StreamsUtils.closeSafetely(driver) {
            log.info("Error while closing Driver instance:", it)
        }
        log.info("Neo4j Source Service closed successfully")
    }
}