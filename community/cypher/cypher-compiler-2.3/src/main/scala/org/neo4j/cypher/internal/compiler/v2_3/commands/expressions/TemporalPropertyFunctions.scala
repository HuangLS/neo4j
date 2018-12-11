package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.TemporalContains
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTBoolean


/**
  * Created by song on 2018-12-11.
  */
case class TemporalContainsFunction(property: Expression, tVal: Expression) extends Expression{
  override def rewrite(f: (Expression) => Expression): Expression = TemporalContains(property, tVal).rewrite(f)

  override def arguments: Seq[Expression] = Seq(property, tVal)

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    throw new RuntimeException("TGraph SNH: because this is rewrite to TemporalContains Expression so this should not be called.")
  }

  override protected def calculateType(symbols: SymbolTable): CypherType = CTBoolean

  override def toString: String = "TemporalContainsFunc("+property.toString()+")"

  override def symbolTableDependencies: Set[String] = Set()
}
