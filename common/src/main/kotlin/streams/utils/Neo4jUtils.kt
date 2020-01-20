package streams.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import streams.extensions.execute
import java.lang.reflect.InvocationTargetException
import kotlin.streams.toList

object Neo4jUtils {
    @JvmStatic val LEADER = "LEADER"
    @JvmStatic val SYSTEM_DATABASE_NAME = "system"
    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean {
        try {
            val isSlave = StreamsUtils.ignoreExceptions(
                    {
                        val hadb = Class.forName("org.neo4j.kernel.ha.HighlyAvailableGraphDatabase")
                        hadb.isInstance(db) && !(hadb.getMethod("isMaster").invoke(db) as Boolean)
                    }, ClassNotFoundException::class.java, IllegalAccessException::class.java,
                    InvocationTargetException::class.java, NoSuchMethodException::class.java)
            if (isSlave != null && isSlave) {
                return false
            }

            val role = getMemberRole(db)
            return role.equals(LEADER, ignoreCase = true)
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return true
            }
            throw e
        }
    }

    fun getLogService(db: GraphDatabaseAPI): LogService {
        return db.dependencyResolver
                .resolveDependency(LogService::class.java)
    }

    fun isCluster(db: GraphDatabaseAPI): Boolean {
        try {
            getMemberRole(db)
            return true
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    private fun getMemberRole(db: GraphDatabaseAPI) = db.execute("CALL dbms.cluster.role(\$database)",
            mapOf("database" to db.databaseName())) { it.columnAs<String>("role").next() }

    fun clusterHasLeader(db: GraphDatabaseAPI): Boolean {
        try {
            return db.execute("""
                |CALL dbms.cluster.overview() YIELD databases
                |RETURN databases[${'$'}database] AS role
            """.trimMargin(), mapOf("database" to db.databaseName())) {
                it.columnAs<String>("role")
                        .stream()
                        .toList()
                        .contains(LEADER)
            }
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI, action: () -> T?): T? {
        if (isWriteableInstance(db)) {
            return action()
        } else {
            return null
        }
    }

    fun executeInLeader(db: GraphDatabaseAPI, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!clusterHasLeader(db) && System.currentTimeMillis() - start < timeout) {
                log.info("$LEADER not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            executeInWriteableInstance(db, action)
        }
    }

    fun waitForTheLeader(db: GraphDatabaseAPI, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!clusterHasLeader(db) && System.currentTimeMillis() - start < timeout) {
                log.info("$LEADER not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            action()
        }
    }

}