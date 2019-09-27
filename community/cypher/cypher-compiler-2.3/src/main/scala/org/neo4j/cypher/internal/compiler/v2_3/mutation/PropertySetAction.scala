/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.spi.Operations
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.helpers.ThisShouldNotHappenError

case class PropertySetAction(prop: Property, valueExpression: Expression)
  extends SetAction {

  val Property(mapExpr, propertyKey) = prop

  def localEffects(symbols: SymbolTable) = Effects.propertyWrite(mapExpr, symbols)(propertyKey.name)

  def exec(context: ExecutionContext, state: QueryState) = {
    implicit val s = state

    val qtx = state.query

    val expr = mapExpr(context)
    if (expr != null) {
      val (id, ops) = expr match {
        case (e: Relationship) => (e.getId, qtx.relationshipOps)
        case (e: Node) => (e.getId, qtx.nodeOps)
        case _ => throw new ThisShouldNotHappenError("Stefan", "This should be a node or a relationship")
      }
      val v = valueExpression(context)
      if (isTemporalValue(v)){
        setTemporalVal(id, propertyKey.getOrCreateId(qtx), v, ops )
      }else makeValueNeoSafe(v) match {
        case null => propertyKey.getOptId(qtx).foreach(ops.removeProperty(id, _))
        case value => ops.setProperty(id, propertyKey.getOrCreateId(qtx), value)
      }
    }

    Iterator(context)
  }

// TGraph: union type definition: https://stackoverflow.com/questions/3508077/how-to-define-type-disjunction-union-types
  type ¬[A] = A => Nothing
  type ∨[T, U] = ¬[¬[T] with ¬[U]]
  type ¬¬[A] = ¬[¬[A]]
  type |∨|[T, U] = { type λ[X] = ¬¬[X] <:< (T ∨ U) }

//  private def setTemporalVal(entityId:Long, propId:Int, value:Any, op:Operations[ Node |∨| Relationship]) = {
  private def setTemporalVal(entityId:Long, propId:Int, value:Any, op:Operations[ _ >: Node with Relationship <: PropertyContainer]): Unit = {
    value match {
      case v:Seq[(Int, Int, Long)] => v.foreach(i =>{
        val start = i._1
        val end = i._2
        op.setTemporalProperty(entityId, propId, start, end, i._3)
      })
      case v:Seq[(Int, Int, Double)] => v.foreach(i =>{
        val start = i._1
        val end = i._2
        op.setTemporalProperty(entityId, propId, start, end, i._3)
      })
      case v:Seq[(Int, Int, String)] => v.foreach(i =>{
        val start = i._1
        val end = i._2
        op.setTemporalProperty(entityId, propId, start, end, i._3)
      })
      case _ => throw new RuntimeException("TGraph SNH: type mismatch")
    }
  }

  private def isTemporalValue(value:Any):Boolean = value match {
    case i:Seq[(Int, Int, Any)] => true
    case _ => false
  }

  def identifiers = Nil

  def children = Seq(prop, valueExpression)

  def rewrite(f: (Expression) => Expression): PropertySetAction = PropertySetAction(prop, valueExpression.rewrite(f))

  def symbolTableDependencies = prop.symbolTableDependencies ++ valueExpression.symbolTableDependencies
}
