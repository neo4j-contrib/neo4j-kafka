package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log

class StreamsEventSinkQueryExecution(private val streamsTopicService: StreamsTopicService, private val db: GraphDatabaseAPI, val log: Log) {

    private val UNWIND: String = "UNWIND {events} AS event"

    fun execute(topic: String, params: Collection<Any>) {
        val cypherQuery = streamsTopicService.get(topic)
        if (cypherQuery == null) {
            return
        }
        if(log.isDebugEnabled){

            log.debug("Processing ${params.size} events from Kafka")
        }
        db.execute("$UNWIND $cypherQuery", mapOf("events" to params)).close()
    }

    fun execute(map: Map<String, Collection<Any>>) {
        map.entries.forEach{ execute(it.key, it.value) }
    }

}

