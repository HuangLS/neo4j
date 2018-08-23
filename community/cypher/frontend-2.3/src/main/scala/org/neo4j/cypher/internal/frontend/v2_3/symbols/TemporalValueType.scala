package org.neo4j.cypher.internal.frontend.v2_3.symbols

/**
  * Created by song on 2018-08-20.
  */
object TemporalValueType {
  val instance = new TemporalValueType() {
    val parentType = CTAny
    override val toString = "TValue"
  }
}

sealed abstract class TemporalValueType extends CypherType

object TimePointType {
  val instance = new TimePointType() {
    val parentType = CTAny
    override val toString = "TPoint"
  }
}

sealed abstract class TimePointType extends CypherType

object TimeIntervalType {
  val instance = new TimeIntervalType() {
    val parentType = CTAny
    override val toString = "TRange"
  }
}

sealed abstract class TimeIntervalType extends CypherType