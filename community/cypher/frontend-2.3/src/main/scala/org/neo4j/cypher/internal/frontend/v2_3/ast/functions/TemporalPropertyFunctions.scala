package org.neo4j.cypher.internal.frontend.v2_3.ast.functions

import org.neo4j.cypher.internal.frontend.v2_3.{SemanticCheck, SemanticCheckResult, SemanticError}
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Function, FunctionInvocation, Parameter, PatternExpression, Property, PropertyKeyName, SignedDecimalIntegerLiteral, SignedIntegerLiteral, SimpleTypedFunction, StringLiteral, UnsignedIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-12-11.
  */
case object TemporalContainsFunc extends Function with SimpleTypedFunction  {
  override def name: String = "temporalContains"

  override val signatures: Seq[TemporalContainsFunc.Signature] = Vector(
    Signature(argumentTypes = Vector(CTAny, CTTValue), outputType = CTBoolean)
  )
}


case object TemporalProjectionFunc extends Function with SimpleTypedFunction  {
  override def name: String = "tp"

  override val signatures: Seq[TemporalProjectionFunc.Signature] = Vector(
    Signature(argumentTypes = Vector(CTTValue, CTTimeRange), outputType = CTTValue)
  )
}


case object SnapshotValueFunc extends Function with SimpleTypedFunction  {
  override def name: String = "valueAt"

  override val signatures: Seq[SnapshotValueFunc.Signature] = Vector(
    Signature(argumentTypes = Vector(CTTValue, CTTimePoint), outputType = CTAny)
  )
}


case object TemporalAggregationMinFunc extends Function{
  override def name: String = "tAggrMin"

  // modified from org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Exist class.
  override def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation): SemanticCheck = {
    checkArgs(invocation, 3) ifOkChain {
      val tmp = invocation.arguments // same as invocation.args, but former is a list :: later is a Vector.
      tmp.head.expectType(CTAny.covariant) chain invocation.specifyType(CTTValue) chain {
        (tmp.head, tmp(1), tmp(2)) match {
          case (_: Property, s:SignedDecimalIntegerLiteral, e:SignedDecimalIntegerLiteral) =>
            if(0 < s.value)
              if(s.value <= e.value ) None else Some(SemanticError("start time must less or equal than end time.", s.position, e.position))
            else Some(SemanticError("Time must be larger than 0.", s.position, e.position))
          case (_: Property, _: Parameter, _: Parameter) => None // don't known why this could happen (semanticCheck called twice), weird.
          case (p, _, _) =>
            Some(SemanticError(s"First argument to ${invocation.name}(...) is not a property",  p.position, invocation.position))
        }
      }
    }
  }
}