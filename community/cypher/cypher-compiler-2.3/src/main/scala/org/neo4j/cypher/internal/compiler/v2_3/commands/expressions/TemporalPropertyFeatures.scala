package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.TemporalContains
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

import org.neo4j.cypher.internal.frontend.v2_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTBoolean
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression => ASTExpression, Parameter, TimeInterval}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-08-20.
  */
abstract class TemporalValueCommandExpression extends Expression {

  def calculateType(symbols: SymbolTable) = CTTValue

}

case class TemporalValueLiteral(items: Seq[(TimeInterval, ASTExpression)]) extends TemporalValueCommandExpression{
  def apply(ctx: ExecutionContext)(implicit state: QueryState):Seq[Tuple3[Long,Long,Any]] = {
    items.map(entry=>{
      val timeInterval = entry._1.items
      val startT = parVal(timeInterval._1.item)
      val endT = parVal(timeInterval._2.item)
      val value = parVal(entry._2)
      (startT, endT) match {
        case (s:Long,e:Long) => (s, e, value)
        case _ => throw new RuntimeException("TGraph SNH: type mismatch.")
      }
    })
  }

  private def parVal(e:ASTExpression)(implicit state:QueryState):Any = {
    e match {
      case i:Parameter => state.getParam(i.name)
      case _ => throw new RuntimeException("TGraph SNH: type mismatch")
    }
  }

  override def toString: String = items.toString()

  def rewrite(f: (Expression) => Expression): Expression = f(this)

  def arguments = Seq()

  def symbolTableDependencies = Set()
}

case class TemporalProperty() extends TemporalValueCommandExpression{
  def apply(ctx: ExecutionContext)(implicit state: QueryState):Any = {

  }

  override def rewrite(f: (Expression) => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq()

  override def symbolTableDependencies: Set[String] = Set()
}


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
