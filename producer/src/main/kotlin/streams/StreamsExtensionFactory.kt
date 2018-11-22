package streams

import org.neo4j.graphdb.Node
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter

class StreamsExtensionFactory : KernelExtensionFactory<StreamsExtensionFactory.Dependencies>("StreamsProcedures") {
    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        val db = dependencies.graphdatabaseAPI()
        val log = dependencies.log()
        val configuration = dependencies.config()
        val streamHandler = StreamsEventRouterFactory.getStreamsEventRouter(log, configuration)
        val streamsEventRouterConfiguration = StreamsEventRouterConfiguration.from(configuration.raw)
        return StreamsEventRouterLifecycle(db, streamHandler, streamsEventRouterConfiguration, log)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
    }
}

class StreamsEventRouterLifecycle(val db: GraphDatabaseAPI, val streamHandler: StreamsEventRouter,
                                  val streamsEventRouterConfiguration: StreamsEventRouterConfiguration,
                                  private val log: LogService): LifecycleAdapter() {
    private val streamsLog = log.getUserLog(StreamsExtensionFactory::class.java)
    private lateinit var txHandler: StreamsTransactionEventHandler

    override fun start() {
        try {
            streamHandler.start()
            registerTransactionEventHandler()
        } catch (e: Exception) {
            e.printStackTrace()
            streamsLog.error("Error initializing the streaming producer", e)
        }
    }

    fun registerTransactionEventHandler() {
        txHandler = StreamsTransactionEventHandler(streamHandler, streamsEventRouterConfiguration)
        db.registerTransactionEventHandler(txHandler)
    }

    fun unregisterTransactionEventHandler() {
        db.unregisterTransactionEventHandler(txHandler)
    }

    override fun stop() {
        unregisterTransactionEventHandler()
        streamHandler.stop()
    }
}

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}