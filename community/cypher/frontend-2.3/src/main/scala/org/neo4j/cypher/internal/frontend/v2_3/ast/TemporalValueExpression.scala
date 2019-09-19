package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticError}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-08-20.
  */
case class TemporalValueExpression (items: Seq[(TimeInterval, Expression)])(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTTValue

  var tpVal: Seq[(Int, Int, Any)] = List()

  /**
   * 可能的错误情况：
   * 1. Now不在最后
   * 2. Init不在最前
   * 3. 时间不递增
   * @param ctx
   * @return
   */
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    var t:Long = -2
    for( a <- items ){
      val s = a._1.start
      val e = a._1.end
      if( t<s && s<=e ) t = e
      else return SemanticError("time decrease!", position)
    }
    if(t>=Int.MaxValue) return SemanticError("time too large! must less than Int.max_value", position)
    this.tpVal = items.map(i => {
      (i._1.start, i._1.end, i._2)
    })
    SemanticCheckResult.success
  }
}


sealed trait TimePoint extends Expression {
  var time:Int = 0
  def possibleTypes: TypeSpec = CTTimePoint
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class TimePointRegular (item: Expression)(val position: InputPosition) extends TimePoint with SimpleTyping {
  override def semanticCheck(ctx: SemanticContext): SemanticCheck ={
    item match {
      case i:UnsignedIntegerLiteral => {
        this.time = Math.toIntExact(i.value)
      }
      case i:StringLiteral => {
        // TODO: parse date time strings.
      }
      case i:Parameter => {
        //
      }
      case _ => {
        new RuntimeException("TGraph SNH: item type mismatch")
        SemanticError("invalid literal number", position)
      }
    }
    SemanticCheckResult.success
//    super.semanticCheck(ctx)
  }
}

case class TimePointNow()(val position: InputPosition) extends TimePoint with SimpleTyping {
  time = Int.MaxValue
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class TimePointInit()(val position: InputPosition) extends TimePoint with SimpleTyping {
  time = -1
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class TimeInterval (items: (TimePoint, TimePoint))(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTTimeRange
  var start:Int = 0
  var end:Int = 0
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}
