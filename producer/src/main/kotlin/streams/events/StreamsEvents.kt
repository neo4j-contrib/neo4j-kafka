package streams.events

import org.neo4j.graphdb.schema.ConstraintType

enum class OperationType { created, updated, deleted }

data class Meta(val timestamp: Long,
                val username: String,
                val txId: Long,
                val txEventId: Int,
                val txEventsCount: Int,
                val operation: OperationType,
                val source: Map<String, Any> = emptyMap())


enum class EntityType { node, relationship }

data class RelationshipNodeChange(val id: String,
                                  val labels: List<String>?)

abstract class RecordChange{ abstract val properties: Map<String, Any>? }
data class NodeChange(override val properties: Map<String, Any>?,
                      val labels: List<String>?): RecordChange()

data class RelationshipChange(override val properties: Map<String, Any>?): RecordChange()

abstract class Payload {
    abstract val id: String
    abstract val type: EntityType
    abstract val before: RecordChange?
    abstract val after: RecordChange?
}
data class NodePayload(override val id: String,
                       override val before: RecordChange?,
                       override val after: RecordChange?,
                       override val type: EntityType = EntityType.node): Payload()

data class RelationshipPayload(override val id: String,
                               val start: RelationshipNodeChange,
                               val end: RelationshipNodeChange,
                               override val before: RecordChange?,
                               override val after: RecordChange?,
                               val label: String,
                               override val type: EntityType = EntityType.relationship): Payload()

data class Constraint(val label: String?,
                      val properties: List<String>,
                      val type: ConstraintType)

data class Schema(val properties: List<String> = emptyList(),
                  val constraints: List<Constraint>? = null)

data class StreamsEvent(val meta: Meta, val payload: Payload, val schema: Schema)