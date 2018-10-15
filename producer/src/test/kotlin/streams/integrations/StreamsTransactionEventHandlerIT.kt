package streams.integrations

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import streams.events.NodeChange
import streams.events.OperationType
import streams.mocks.MockStreamsEventRouter
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


class StreamsTransactionEventHandlerIT {

    private var db: GraphDatabaseService? = null

    @Before
    fun setUp() {
        MockStreamsEventRouter.reset()
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("streams.router", "streams.mocks.MockStreamsEventRouter")
                .newGraphDatabase()
    }

    @After
    fun tearDown() {
        db?.shutdown()
    }

    @Test fun testSequence(){
        db!!.execute("CREATE (:Person {name:'Omar', age: 30}), (:Person {name:'Andrea', age: 31})")

        assertEquals(2,MockStreamsEventRouter.events.size)
        assertEquals(OperationType.created,MockStreamsEventRouter.events[0].meta.operation)
        assertEquals(OperationType.created,MockStreamsEventRouter.events[1].meta.operation)
        assertEquals(2,MockStreamsEventRouter.events[0].meta.txEventsCount)
        assertEquals(2,MockStreamsEventRouter.events[1].meta.txEventsCount)
        assertEquals(0,MockStreamsEventRouter.events[0].meta.txEventId)
        assertEquals(1,MockStreamsEventRouter.events[1].meta.txEventId)
        assertNotNull(MockStreamsEventRouter.events[0].meta.source["hostname"])
        assertNotNull(MockStreamsEventRouter.events[1].meta.source["hostname"])

        MockStreamsEventRouter.reset()

        db!!.execute("MATCH (o:Person {name:'Omar'}), (a:Person {name:'Andrea'}) " +
                "SET o:Test " +
                "REMOVE o:Person " +
                "SET o.age = 31 " +
                "SET o.surname = 'unknown' " +
                "REMOVE o.name " +
                "SET a:Marked ")

        assertEquals(2,MockStreamsEventRouter.events.size)
        assertEquals(OperationType.updated,MockStreamsEventRouter.events[0].meta.operation)
        assertEquals(OperationType.updated,MockStreamsEventRouter.events[1].meta.operation)
        assertEquals(2,MockStreamsEventRouter.events[0].meta.txEventsCount)
        assertEquals(2,MockStreamsEventRouter.events[1].meta.txEventsCount)
        assertEquals(0,MockStreamsEventRouter.events[0].meta.txEventId)
        assertEquals(1,MockStreamsEventRouter.events[1].meta.txEventId)


        MockStreamsEventRouter.reset()

        db!!.execute("MATCH (o:Marked) DELETE o ")

        assertEquals(1,MockStreamsEventRouter.events.size)
        assertEquals(OperationType.deleted,MockStreamsEventRouter.events[0].meta.operation)
        val before : NodeChange = MockStreamsEventRouter.events[0].payload.before as NodeChange
        assertEquals(listOf("Person","Marked") , before.labels)
        assertEquals(mapOf("name" to "Andrea", "age" to 31L) , before.properties)

        assertEquals(1,MockStreamsEventRouter.events[0].meta.txEventsCount)
        assertEquals(0,MockStreamsEventRouter.events[0].meta.txEventId)
    }

}
