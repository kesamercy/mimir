package mimir.algebra.function;

import java.sql.SQLException

import mimir.parser.ExpressionParser
import mimir.algebra._
import mimir.Database

sealed abstract class RegisteredFunction { val name: String }

case class NativeFunction(
	name: String, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[Type] => Type,
	passthrough:Boolean = false
) extends RegisteredFunction

case class ExpressionFunction(
  name: String,
  args:Seq[String], 
  expr: Expression
) extends RegisteredFunction
{
  def unfold(argValues: Seq[Expression]) =
  {
    Eval.inline(expr, args.zip(argValues).toMap)
  }
}

case class FoldFunction(
  name: String, 
  expr: Expression
) extends RegisteredFunction
{
  def unfold(argValues: Seq[Expression]) =
  {
    argValues.tail.foldLeft[Expression](argValues.head){ case (curr,next) => 
      Eval.inline(expr, Map("CURR" -> curr, "NEXT" -> next)) }    
  }
}

class FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
    GeoFunctions.register(this)
    JsonFunctions.register(this)
    NumericFunctions.register(this)
    SampleFunctions.register(this)
    StringFunctions.register(this)
    TypeFunctions.register(this)
    UtilityFunctions.register(this)
    RandomnessFunctions.register(this)
    TimeFunctions.register(this)
	}

  def register(
    fname:String,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[Type] => Type
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker))
    
  def registerPassthrough(
    fname:String,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[Type] => Type
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker, true))

  def registerExpr(fname:String, args:Seq[String], expr:String): Unit =
    registerExpr(fname, args, ExpressionParser.expr(expr))
  def registerExpr(fname:String, args:Seq[String], expr:Expression): Unit =
    register(new ExpressionFunction(fname, args, expr))

  def registerFold(fname:String, expr:String): Unit =
    registerFold(fname, ExpressionParser.expr(expr))
  def registerFold(fname:String, expr:Expression): Unit =
    register(new FoldFunction(fname, expr))

	def register(fn: RegisteredFunction) =
    functionPrototypes.put(fn.name, fn)

  def get(fname: String): RegisteredFunction =
  {
    functionPrototypes.get(fname) match { 
      case Some(func) => func
      case None => throw new RAException(s"Unknown function '$fname'")
    }
  }

  def getOption(fname: String): Option[RegisteredFunction] =
    functionPrototypes.get(fname)

  def unfold(fname: String, args: Seq[Expression]): Option[Expression] = 
    get(fname) match {
      case _:NativeFunction     => None
      case e:ExpressionFunction => Some(e.unfold(args))
      case e:FoldFunction       => Some(e.unfold(args))
    }
}
