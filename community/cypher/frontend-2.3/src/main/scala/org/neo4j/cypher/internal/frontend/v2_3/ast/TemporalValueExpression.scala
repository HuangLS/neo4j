package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticError}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-08-20.
  */
case class TemporalValueExpression (items: Seq[(TimeInterval, Expression)])(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTTValue
  /**
   * 可能的错误情况：
   * 1. Now不在最后
   * 2. Init不在最前
   * 3. 时间不递增
   * 该方法会被调用两次，第一次传入的items中的TimePointRegular中的item是UnsignedIntegerLiteral，然后可能发生了AST重写？
   * 然后第二次调用该方法，传入的items中的TimePointRegular中的item是Parameter类型
   * @param ctx
   * @return
   */
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    var t:Long = -2
    var valueType:String = null
    try{
      for( a <- items ){
        val startTimePointChk = a._1.startT.semanticCheck(ctx)
        val endTimePointChk = a._1.endT.semanticCheck(ctx)
        if( startTimePointChk != SemanticCheckResult.success) return startTimePointChk
        if( endTimePointChk != SemanticCheckResult.success) return endTimePointChk

        val s = a._1.startT.time
        val e = a._1.endT.time
        if( t<s && s<=e ) t = e
        else {
          return SemanticError("time decrease!", position)
        }

        if(valueType==null) valueType = a._2.getClass.getName
        else if(valueType!=a._2.getClass.getName) return SemanticError("values in TemporalValue must be same type!", position)
      }
      if(t>Int.MaxValue) return SemanticError("time too large! must less than Int.max_value", position)
    }catch{
      case NoNeedCheck => //so just exit check process
    }
    SemanticCheckResult.success
  }
}

private object NoNeedCheck extends RuntimeException

sealed trait TimePoint extends Expression {
  var time:Int = 0
  def possibleTypes: TypeSpec = CTTimePoint
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class TimePointRegular (item: Expression)(val position: InputPosition) extends TimePoint with SimpleTyping {
  override def semanticCheck(ctx: SemanticContext): SemanticCheck ={
    item match {
      case i:UnsignedIntegerLiteral => {
        if(i.value > Int.MaxValue) return SemanticError("TCypher: TimePoint timestamp should in [0, Int.Max)", position)
        else this.time = Math.toIntExact(i.value)
      }
      case _:StringLiteral => {
        // TODO: parse date time strings.
      }
      case _:Parameter => {
        throw NoNeedCheck
      }
      case i => {
        return SemanticError("TGraph SNH: TimePoint value type mismatch. got "+i.getClass.getName, position)
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

case class TimeInterval (startT: TimePoint, endT: TimePoint)(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes: TypeSpec = CTTimeRange
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheckResult.success
}
