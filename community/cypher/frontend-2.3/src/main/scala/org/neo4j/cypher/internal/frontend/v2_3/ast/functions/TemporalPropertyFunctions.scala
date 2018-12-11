package org.neo4j.cypher.internal.frontend.v2_3.ast.functions

import org.neo4j.cypher.internal.frontend.v2_3.ast.{Function, SimpleTypedFunction}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-12-11.
  */
case object TemporalContainsFunc extends Function with SimpleTypedFunction  {
  override def name: String = "temporalcontains"

  override val signatures: Seq[TemporalContainsFunc.Signature] = Vector(
    Signature(argumentTypes = Vector(CTAny, CTTValue), outputType = CTBoolean)
  )
}

