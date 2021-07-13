package streams.kafka.connect.utils

import org.slf4j.LoggerFactory
import java.util.*

class PropertiesUtil {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PropertiesUtil::class.java)
        private const val DEFAULT_VERSION = "unknown"
        private var properties: Properties? = null
        private var VERSION: String? = null
        init {
            properties = Properties()
            properties!!.load(PropertiesUtil::class.java.getResourceAsStream("/kafka-connect-version.properties"))
            properties!!.load(PropertiesUtil::class.java.getResourceAsStream("/kafka-connect-neo4j.properties"))
            VERSION = try {
                properties!!.getProperty("version", DEFAULT_VERSION).trim()
            } catch (e: Exception) {
                LOGGER.warn("error while loading version:", e)
                DEFAULT_VERSION
            }
        }

        fun getVersion(): String {
            return VERSION!!
        }

        fun getProperty(key: String): String {
            return properties!!.getProperty(key)
        }
    }
}