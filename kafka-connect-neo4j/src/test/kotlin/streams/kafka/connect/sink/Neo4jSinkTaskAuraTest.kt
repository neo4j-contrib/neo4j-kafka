package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.*
import org.junit.Assume.assumeTrue
import org.mockito.Mockito.mock
import org.neo4j.driver.*
import org.neo4j.driver.exceptions.ClientException
import streams.events.*
import streams.kafka.connect.common.Neo4jConnectorConfig
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDOperations
import streams.utils.JSONUtils
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class Neo4jSinkTaskAuraTest {

    companion object {

        private val SIMPLE_SCHEMA = SchemaBuilder.struct().name("com.example.Person")
                .field("name", Schema.STRING_SCHEMA)
                .build()

        private val user: String? = System.getenv("AURA_USER")
        private val password: String? = System.getenv("AURA_PASSWORD")
        private val uri: String? = System.getenv("AURA_URI")
        private var driver: Driver? = null

        private const val NAME_TOPIC = "neotopic"
        private const val SHOW_CURRENT_USER = "SHOW CURRENT USER"
        private const val DBMS_LIST_CONFIG = "CALL dbms.listConfig"
        private const val NEO4J = "neo4j"
        private const val SYSTEM = "system"
        private const val ERROR_ADMIN_COMMAND = "Executing admin procedure is not allowed for user '$NEO4J' with roles [PUBLIC] overridden by READ restricted to ACCESS."
        private const val LABEL_SINK_AURA = "SinkAuraTest"
        private const val COUNT_NODES_SINK_AURA = "MATCH (s:$LABEL_SINK_AURA) RETURN count(s) as count"

        @BeforeClass
        @JvmStatic
        fun setUp() {
            assumeTrue(user != null)
            assumeTrue(password != null)
            driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
        }

        fun getMapSinkConnectorConfig() = mutableMapOf(
                Neo4jConnectorConfig.AUTHENTICATION_BASIC_USERNAME to user!!,
                Neo4jConnectorConfig.AUTHENTICATION_BASIC_PASSWORD to password!!,
                Neo4jConnectorConfig.SERVER_URI to uri!!,
                Neo4jConnectorConfig.AUTHENTICATION_TYPE to AuthenticationType.BASIC.toString(),
                Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID_LABEL_NAME to LABEL_SINK_AURA,
        )

        fun countEntitiesSinkAura(session: Session?, number: Int, query: String = COUNT_NODES_SINK_AURA) = session!!.run(query).let {
            assertTrue { it!!.hasNext() }
            assertEquals(number, it!!.next()["count"].asInt())
            assertFalse { it.hasNext() }
        }
    }

    @After
    fun clearNodesAura() {
        driver?.session()?.run("MATCH (n:$LABEL_SINK_AURA) DETACH DELETE n")
    }


    @Test
    fun `test with struct in Aura 4`() {
        driver?.session().use { countEntitiesSinkAura(it, 0) }
        val props = getMapSinkConnectorConfig()
        props["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}$NAME_TOPIC"] = " CREATE (b:$LABEL_SINK_AURA)"
        props[Neo4jConnectorConfig.BATCH_SIZE] = 2.toString()
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, SIMPLE_SCHEMA, Struct(SIMPLE_SCHEMA).put("name", "Baz"), 42))
        task.put(input)

        driver?.session().use { countEntitiesSinkAura(it, 1) }
    }

    @Test
    fun `should insert data into Neo4j from CDC events in Aura 4`() {

        val props = getMapSinkConnectorConfig()
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC
        props[Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Pippo"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Pluto"), labels = listOf("User Ext"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "HAS_REL"
                ),
                schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataEnd, 43),
                SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataRelationship, 44))
        task.put(input)

        driver?.session().use {
            countEntitiesSinkAura(it, 1, "MATCH (:$LABEL_SINK_AURA)-[r:HAS_REL]->(:$LABEL_SINK_AURA) RETURN COUNT(r) as count")
        }
    }

    @Test
    fun `should update data into Neo4j from CDC events in Aura 4`() {
        driver?.session()?.run("""
                CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Pippo', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:$LABEL_SINK_AURA{name:'Pluto', sourceId:'1'})
            """.trimIndent()
        )

        val props = getMapSinkConnectorConfig()
        props[Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Pippo"),
                                labels = listOf("User", "OldLabel")),
                        after = NodeChange(properties = mapOf("name" to "Pippo", "age" to 99),
                                labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.updated
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 1999, "foo" to "bar")),
                        before = RelationshipChange(properties = mapOf("since" to 2014)),
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42),
                SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataRelationship, 43))
        task.put(input)

        driver?.session().use {
            countEntitiesSinkAura(it, 1,
                    "MATCH (:User {age:99})-[r:`KNOWS WHO`{since:1999, sourceId:'2', foo:'bar'}]->(:`User Ext`) RETURN COUNT(r) as count")
        }
    }

    @Test
    fun `should delete data into Neo4j from CDC events in Aura 4`() {

        driver?.session().use {
            it?.run("CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})")
            it?.run("CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'1'})")
        }

        driver?.session().use { countEntitiesSinkAura(it, 2) }

        val props = getMapSinkConnectorConfig()
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC
        props[Neo4jSinkConnectorConfig.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "0",
                        before = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"),
                                labels = listOf("User", "OldLabel")),
                        after = null
                ),
                schema = Schema()
        )
        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42))
        task.put(input)

        driver?.session().use { countEntitiesSinkAura(it, 1) }
    }

    @Test
    fun `should work with node pattern topic in Aura 4`() {
        val props = getMapSinkConnectorConfig()
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX}$NAME_TOPIC"] = "$LABEL_SINK_AURA{!userId,name,surname,address.city}"
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

        val data = mapOf("userId" to 1, "name" to "Pippo", "surname" to "Pluto",
                "address" to mapOf("city" to "Cerignola", "CAP" to "12345"))

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, data, 42))
        task.put(input)

        driver?.session().use {
            countEntitiesSinkAura(it, 1,
                    "MATCH (n:$LABEL_SINK_AURA{name: 'Pippo', surname: 'Pluto', userId: 1, `address.city`: 'Cerignola'}) RETURN count(n) AS count")
        }
    }

    @Test
    fun `should work with relationship pattern topic in Aura 4`() {
        val props = getMapSinkConnectorConfig()
        props["${Neo4jSinkConnectorConfig.TOPIC_PATTERN_RELATIONSHIP_PREFIX}$NAME_TOPIC"] = "(:$LABEL_SINK_AURA{!sourceId,sourceName,sourceSurname})-[:HAS_REL]->(:$LABEL_SINK_AURA{!targetId,targetName,targetSurname})"
        props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

        val data = mapOf("sourceId" to 1, "sourceName" to "Pippo", "sourceSurname" to "Pluto",
                "targetId" to 1, "targetName" to "Foo", "targetSurname" to "Bar")

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, data, 42))
        task.put(input)
        driver?.session().use {
            countEntitiesSinkAura(it, 1, "MATCH (:$LABEL_SINK_AURA{sourceId: 1})-[r:HAS_REL]->(:$LABEL_SINK_AURA{targetId: 1}) RETURN COUNT(r) as count")
        }
    }

    @Test
    fun `should ingest node data from CUD Events in Aura 4`() {
        val mergeMarkers = listOf(2, 5, 7)
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val data = (1..10).map {
            val labels = if (it % 2 == 0) listOf(LABEL_SINK_AURA, "Bar") else listOf(LABEL_SINK_AURA, "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(cudNode), it.toLong())
        }

        val props = getMapSinkConnectorConfig()
        props[Neo4jSinkConnectorConfig.TOPIC_CUD] = topic
        props[SinkTask.TOPICS_CONFIG] = topic

        val task = Neo4jSinkTask()
        task.initialize(mock(SinkTaskContext::class.java))
        task.start(props)
        task.put(data)

        driver?.session().use {
            countEntitiesSinkAura(it, 5, "MATCH (n:$LABEL_SINK_AURA:Bar:Label) RETURN count(n) AS count")
            countEntitiesSinkAura(it, 10, "MATCH (n:$LABEL_SINK_AURA:Bar) RETURN count(n) AS count")
        }
    }

    @Test
    fun `neo4j user should not have the admin role in Aura 4`() {

        driver?.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session?.run(SHOW_CURRENT_USER).let {
                assertTrue { it!!.hasNext() }
                val roles = it!!.next().get("roles").asList()
                assertFalse { roles.contains("admin") }
                assertTrue { roles.contains("PUBLIC") }
                assertFalse { it.hasNext() }
            }
        }
    }

    @Test
    fun `should fail if I try to run SHOW CURRENT USER commands on neo4j database in Aura 4`() {

        assertFailsWith(ClientException::class,
                "This is an administration command and it should be executed against the system database: $SHOW_CURRENT_USER")
        {
            driver?.session(SessionConfig.forDatabase(NEO4J)).use {
                it?.run(SHOW_CURRENT_USER)
            }
        }
    }

    @Test
    fun `should fail if I try to run admin commands with neo4j user in Aura 4`() {

        assertFailsWith(ClientException::class, ERROR_ADMIN_COMMAND)
        {
            driver?.session(SessionConfig.forDatabase(SYSTEM)).use {
                it?.run(DBMS_LIST_CONFIG)
            }
        }

        assertFailsWith(ClientException::class, ERROR_ADMIN_COMMAND)
        {
            driver?.session(SessionConfig.forDatabase(NEO4J)).use {
                it?.run(DBMS_LIST_CONFIG)
            }
        }
    }

}
