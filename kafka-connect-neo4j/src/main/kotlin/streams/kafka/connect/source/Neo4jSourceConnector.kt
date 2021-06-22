package streams.kafka.connect.source

import com.github.jcustenborder.kafka.connect.utils.config.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import streams.kafka.connect.utils.PropertiesUtil

@Title("Neo4j Source Connector")
@Description("The Neo4j Source connector reads data from Neo4j and and writes the data to a Kafka Topic")
class Neo4jSourceConnector: SourceConnector() {
    private lateinit var settings: Map<String, String>
    private lateinit var config: Neo4jSourceConnectorConfig

    // TODO Add monitor thread when we want to have schema on LABELS and RELATIONSHIP query type

    // TODO: for now we support just one task we need to implement
    //  a SKIP/LIMIT mechanism in case we want parallelize
    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> = listOf(settings)

    override fun start(props: MutableMap<String, String>?) {
        settings = props!!
        config = Neo4jSourceConnectorConfig(settings)
    }

    override fun stop() {}

    override fun version(): String = PropertiesUtil.getVersion()

    override fun taskClass(): Class<out Task> = Neo4jSourceTask::class.java

    override fun config(): ConfigDef = Neo4jSourceConnectorConfig.config()

}