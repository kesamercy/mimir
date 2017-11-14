package mimir.sql.sparksql

import java.sql.SQLException

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.algebra.function.FunctionRegistry
import mimir.ctables._
import mimir.Database
import mimir.ctables.vgterm.BestGuess
import mimir.models.SimpleSparkClassifierModel
import mimir.util._
import org.apache.spark.sql.Row

class BestGuessVGTerm(db: Database) {
  
  def bestGuessVGTerm(modelName : String, idx: Int, args:Seq[Any]) : Int = {
    /*
    val value_mimir : ( Int,Type) => PrimitiveValue = (idx, t) => {
      t match {
        case TInt()    => IntPrimitive(args(idx).asInstanceOf[Long])
        case TFloat()  => FloatPrimitive(args(idx).asInstanceOf[Double])
        case TAny()    => args(idx) match {
                case intVal : Int => IntPrimitive(intVal.toLong)
                case longVal : Long => IntPrimitive(longVal)
                case doubleVal : Double   => FloatPrimitive(doubleVal)
                case strVal : String => StringPrimitive(strVal)
                case null    => null
              }
         case _       => TextUtils.parsePrimitive(t, args(idx).toString)
      }
    }

    val model = db.models.get(modelName)
    val argList = model.argTypes(idx).
      zipWithIndex.
      map(arg => value_mimir(arg._2+2, arg._1))
    val hintList = model.hintTypes(idx).
      zipWithIndex.
      map(arg => value_mimir(arg._2+argList.length+2, arg._1))
    val guess = model.bestGuess(idx, argList, hintList)

    guess match {
      case IntPrimitive(i)      => i
      case FloatPrimitive(f)    => f
      case StringPrimitive(s)   => s
      case d:DatePrimitive      => d.asString
      case BoolPrimitive(true)  => 1
      case BoolPrimitive(false) => 0
      case RowIdPrimitive(r)    => r
      case NullPrimitive()      => null
    }
    */
    10
  }
}


object VGTermFunctions 
{

  def bestGuessVGTermFn = "BEST_GUESS_VGTERM"

  def register(db: Database, spark:org.apache.spark.sql.SparkSession): Unit =
  {
    spark.udf.register(bestGuessVGTermFn, new BestGuessVGTerm(db).bestGuessVGTerm _)
    db.functions.register(
      bestGuessVGTermFn,
      (args) => { throw new SQLException("Mimir Cannot Execute VGTerm Functions Internally") },
      (_) => TInt()
    )
  }

  def rowUDF(model: mimir.models.Model) = org.apache.spark.sql.functions.udf((r: Row) => {
    val m: SimpleSparkClassifierModel = model.asInstanceOf[SimpleSparkClassifierModel]
    val A: Int = r.get(0).asInstanceOf[Int]
    val B: Int = r.get(1).asInstanceOf[Int]
    val C: Int = r.get(2).asInstanceOf[Int]
    val rowID = r.get(3)
    val res = m.classify(RowIdPrimitive(rowID.toString),Seq[PrimitiveValue](IntPrimitive(A),IntPrimitive(B),NullPrimitive()))

    r.get(0).asInstanceOf[Int] + r.get(1).asInstanceOf[Int]
  })

  def specialize(e: Expression): Expression = {
    e match {
      case VGTerm(model, idx, args, hints) => 
        Function(
          bestGuessVGTermFn, 
          List(StringPrimitive(model), IntPrimitive(idx))++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case BestGuess(model, idx, args, hints) =>
        Function(
          bestGuessVGTermFn,
          List(StringPrimitive(model.name), IntPrimitive(idx))++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case _ => e.recur(specialize(_))
    }
  }

  def specialize(o: Operator): Operator =
    o.recur(specialize(_)).recurExpressions(specialize(_))
}