package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression => ASTExpression, Parameter, TimeInterval}
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-08-20.
  */
case class TemporalValueExpression(items: Seq[(TimeInterval, ASTExpression)]) extends Expression {
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

  def calculateType(symbols: SymbolTable) = CTTValue

  def symbolTableDependencies = Set()
}