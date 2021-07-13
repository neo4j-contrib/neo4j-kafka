package streams.kafka.connect.source

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.mockito.Mockito
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.function.ThrowingSupplier
import org.neo4j.harness.junit.rule.Neo4jRule
import streams.Assert
import streams.extensions.labelNames
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.kafka.connect.sink.AuthenticationType
import streams.utils.JSONUtils
import java.util.UUID
import java.util.concurrent.TimeUnit

class Neo4jSourceTaskTest {

    @Rule @JvmField val db = Neo4jRule()
            .withDisabledServer()
            .withConfig(GraphDatabaseSettings.auth_enabled, false)

    private lateinit var task: SourceTask

    @After
    fun after() {
        task.stop()
    }

    @Before
    fun before() {
        db.defaultDatabaseService().beginTx().use {
            it.execute("MATCH (n) DETACH DELETE n")
            it.commit()
        }
        task = Neo4jSourceTask()
        val sourceTaskContextMock = Mockito.mock(SourceTaskContext::class.java)
        val offsetStorageReader = Mockito.mock(OffsetStorageReader::class.java)
        Mockito.`when`(sourceTaskContextMock.offsetStorageReader())
                .thenReturn(offsetStorageReader)
        Mockito.`when`(offsetStorageReader.offset(Mockito.anyMap<String, Any>()))
                .thenReturn(emptyMap())
        task.initialize(sourceTaskContextMock)
    }

    private fun structToMap(struct: Struct): Map<String, Any?> = struct.schema().fields()
            .map {
                it.name() to when (val value = struct[it.name()]) {
                    is Struct -> structToMap(value)
                    else -> value
                }
            }
            .toMap()

    fun Struct.toMap() = structToMap(this)

    @Test
    fun `should source data from Neo4j with custom QUERY from NOW`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords, true)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            expected.containsAll(actualList)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from NOW with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            expected.containsAll(actualList)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from ALL`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_FROM] = "ALL"
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords, true)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            expected == actualList
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY from ALL with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_FROM] = "ALL"
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.STREAMING_PROPERTY] = "timestamp"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        val expected = insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            expected == actualList
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun insertRecords(totalRecords: Int, longToInt: Boolean = false) = db.defaultDatabaseService().beginTx().use { tx ->
        val elements = (1..totalRecords).map {
            val result = tx.execute("""
                                |CREATE (n:Test{
                                |   name: 'Name ' + $it,
                                |   timestamp: timestamp(),
                                |   point: point({longitude: 56.7, latitude: 12.78, height: 8}),
                                |   array: [1,2,3],
                                |   datetime: localdatetime(),
                                |   boolean: true
                                |})
                                |RETURN n.name AS name, n.timestamp AS timestamp,
                                |   n.point AS point,
                                |   n.array AS array,
                                |   n.datetime AS datetime,
                                |   n.boolean AS boolean,
                                |   n AS node
                            """.trimMargin())
            val map = result.next()
            map["array"] = (map["array"] as LongArray).toList()
                    .map { if (longToInt) it.toInt() else it }
            map["point"] = JSONUtils.readValue<Map<String, Any>>(map["point"]!!)
            map["datetime"] = JSONUtils.readValue<String>(map["datetime"]!!)
            val node = map["node"] as org.neo4j.graphdb.Node
            val nodeMap = node.allProperties
            nodeMap["<id>"] = if (longToInt) node.id.toInt() else node.id
            nodeMap["<labels>"] = node.labelNames()
            // are the same value as above
            nodeMap["array"] = map["array"]
            nodeMap["point"] = map["point"]
            nodeMap["datetime"] = map["datetime"]
            map["node"] = nodeMap
            map
        }
        tx.commit()
        elements
    }

    @Test
    fun `should source data from Neo4j with custom QUERY without streaming property`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
            actualList.size >= 2
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should source data from Neo4j with custom QUERY without streaming property with Schema`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.ENFORCE_SCHEMA] = "true"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = getSourceQuery()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        val list = mutableListOf<SourceRecord>()
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            task.poll()?.let { list.addAll(it) }
            val actualList = list.map { (it.value() as Struct).toMap() }
            actualList.size >= 2
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun getSourceQuery() = """
                |MATCH (n:Test)
                |WHERE n.timestamp > ${'$'}lastCheck
                |RETURN n.name AS name, n.timestamp AS timestamp,
                |   n.point AS point,
                |   n.array AS array,
                |   n.datetime AS datetime,
                |   n.boolean AS boolean,
                |   n AS node
            """.trimMargin()

    @Test(expected = ConnectException::class)
    fun `should throw exception`() {
        val props = mutableMapOf<String, String>()
        props[Neo4jConnectorConfig.SERVER_URI] = db.boltURI().toString()
        props[Neo4jSourceConnectorConfig.TOPIC] = UUID.randomUUID().toString()
        props[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL] = "10"
        props[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY] = "WRONG QUERY".trimMargin()
        props[Neo4jConnectorConfig.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

        task.start(props)
        val totalRecords = 10
        insertRecords(totalRecords)

        task.poll()
    }
}