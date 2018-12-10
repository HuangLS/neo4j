package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheck}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

/**
  * Created by song on 2018-08-20.
  */
case class TemporalValueExpression (items: Seq[(TimeInterval, Expression)])(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes = CTTValue

  var tpVal: Seq[(Int, Int, Any)] = List()

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = {
      this.tpVal = items.map(i => {
        // TODO: check expression type
//        i._2 match {
//          case
//        }
        i._1.semanticCheck(ctx) chain i._2.semanticCheck(ctx)
        (i._1.items._1.time, i._1.items._2.time, i._2)
      })
    items.map(_._2).semanticCheck(ctx)
  }




}

case class TimePoint (item: Expression)(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes = CTTimePoint

  var time:Int = 0

  override def semanticCheck(ctx: SemanticContext): SemanticCheck ={
    item match {
      case i:UnsignedIntegerLiteral => {
        this.time = Math.toIntExact(i.value)
      }
      case i:StringLiteral => {
        // TODO: parse date time strings.
      }
      case i:Parameter => {

      }
      case _ => {
        throw new RuntimeException("TGraph SNH: item type mismatch")
      }
    }
    super.semanticCheck(ctx)
  }

}

case class TimeInterval (items: (TimePoint, TimePoint))(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes = CTTimeRange

  var start:Int = 0
  var end:Int = 0

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    items._1.semanticCheck(ctx) chain items._2.semanticCheck(ctx)
    // assert time inc.
    if(items._1.time > items._2.time){
      throw new RuntimeException("Cypher+Temporal: time must not decrease.")
    }else{
      this.start = items._1.time
      this.end = items._2.time
    }
    super.semanticCheck(ctx)
  }

}