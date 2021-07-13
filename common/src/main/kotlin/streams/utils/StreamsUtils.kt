package streams.utils

import kotlinx.coroutines.*
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import streams.config.StreamsConfig
import streams.extensions.getSystemDb

object StreamsUtils {

    @JvmStatic val UNWIND: String = "UNWIND \$events AS event"

    @JvmStatic val WITH_EVENT_FROM: String = "WITH event, from"

    @JvmStatic val STREAMS_CONFIG_PREFIX = "streams."

    @JvmStatic val STREAMS_SINK_TOPIC_PREFIX = "sink.topic.cypher."

    @JvmStatic val LEADER = "LEADER"

    @JvmStatic val SYSTEM_DATABASE_NAME = "system"

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            if (toIgnore.isEmpty()) {
                return null
            }
            return if (toIgnore.any { it.isInstance(e) }) {
                null
            } else {
                throw e
            }
        }
    }

    fun blockUntilFalseOrTimeout(timeout: Long, delay: Long = 1000, action: () -> Boolean): Boolean = runBlocking {
        val start = System.currentTimeMillis()
        var success = action()
        while (System.currentTimeMillis() - start < timeout && !success) {
            delay(delay)
            success = action()
        }
        success
    }

    fun executeWhenSystemDbIsAvailable(databaseManagementService: DatabaseManagementService,
                                       configuration: StreamsConfig,
                                       actionIfAvailable: () -> Unit,
                                       actionIfNotAvailable: (() -> Unit)?) {
        val systemDb = databaseManagementService.getSystemDb()
        val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
        GlobalScope.launch(Dispatchers.IO) {
            if (systemDb.isAvailable(systemDbWaitTimeout)) {
                actionIfAvailable()
            } else if (actionIfNotAvailable != null) {
                actionIfNotAvailable()
            }
        }
    }

    fun getName(db: GraphDatabaseService) = db.databaseName()

    fun closeSafetely(closeable: AutoCloseable, onError: (Throwable) -> Unit) = try {
        closeable.close()
    } catch (e: Throwable) {
        onError(e)
    }

}