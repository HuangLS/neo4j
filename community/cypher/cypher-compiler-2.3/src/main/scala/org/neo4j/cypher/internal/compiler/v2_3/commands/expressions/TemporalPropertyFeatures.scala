package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.TemporalContains
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.spi.Operations
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTBoolean
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Parameter, Start, TimeInterval, Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

/**
  * Created by song on 2018-08-20.
  */
abstract class TemporalValueCommandExpression extends Expression {

  def calculateType(symbols: SymbolTable) = CTTValue

}

case class TemporalValueLiteral(items: Seq[(TimeInterval, ASTExpression)]) extends TemporalValueCommandExpression{
  def apply(ctx: ExecutionContext)(implicit state: QueryState):Seq[Tuple3[Long,Long,Any]] = {
    items.map(entry=>{
      val timeInterval = entry._1
      val startT = parVal(timeInterval.startT)
      val endT = parVal(timeInterval.endT)
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


case class TemporalAggregationMinFunction(property: Expression, tStart: Expression, tEnd:Expression) extends Expression{
  override def rewrite(f: (Expression) => Expression): Expression = this

  override def arguments: Seq[Expression] = Seq(property, tStart, tEnd)

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    (property, tStart, tEnd) match {
      case (Property(mapExpr, propertyKey), start: ParameterExpression, end: ParameterExpression) =>
        val s = state.getParam(start.parameterName)
        val e = state.getParam(end.parameterName)
        (mapExpr(ctx), s, e) match {
          case (n: Node, start:Long, end:Long) =>{
            val propId = propertyKey.getOrCreateId(state.query)
            temporalPropertyValueMatch(state.query.nodeOps, n.getId, propId, start, end)
          }
          case (r: Relationship, start:Long, end:Long) => temporalPropertyValueMatch(state.query.relationshipOps, r.getId, propertyKey.getOrCreateId(state.query), start, end)
        }
    }
  }

  def temporalPropertyValueMatch[T <: PropertyContainer](op: Operations[T], entityId: Long, propertyId: Int, start: Long, end: Long): Any = {
    var min:Int = Int.MaxValue
    for (t <- start.toInt to end.toInt){
      val value = op.getTemporalProperty(entityId, propertyId, t)
      value match {
        case v: Int => if (min > v) min = v
        case v: Long => if (min > v) min = v.toInt
        case null => //do nothing if invalid (no value)
      }
    }
    if(min==Int.MaxValue) null
    else min
  }



  override protected def calculateType(symbols: SymbolTable): CypherType = CTBoolean

  override def toString: String = "SnapshotValueFunc("+property.toString()+")"

  override def symbolTableDependencies: Set[String] = Set()
}


case class SnapshotValueFunction(property: Expression, tVal: Expression) extends Expression{
  override def rewrite(f: (Expression) => Expression): Expression = TemporalContains(property, tVal).rewrite(f)

  override def arguments: Seq[Expression] = Seq(property, tVal)

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {


  }

  override protected def calculateType(symbols: SymbolTable): CypherType = CTBoolean

  override def toString: String = "SnapshotValueFunc("+property.toString()+")"

  override def symbolTableDependencies: Set[String] = Set()
}


