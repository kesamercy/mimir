package mimir.algebra;

import org.specs2.mutable._

import mimir.parser._
import mimir.test._

object TypecheckQuerySpec extends SQLTestSpecification("Typechecker") {
  
  def check(t: Type)(e: Expression) = 
    db.interpreter.eval(TypecheckQuery(e)) must be equalTo TypePrimitive(t)

  "The Typechecker Expression Generator" should {
    "Check Primitives" >> {
      check(TInt())   { IntPrimitive(1) }
      check(TFloat()) { FloatPrimitive(1.0) }
      check(TString()){ StringPrimitive("Yo") }
      check(TDate())  { DatePrimitive(1,2,3) }
      check(TBool())  { BoolPrimitive(true) }
    }

    "Check Arithmetic and Comparisons" >> {

      check(TInt())   { IntPrimitive(1    ).add(IntPrimitive(2))  }
      check(TFloat()) { FloatPrimitive(1.0).add(FloatPrimitive(2.0))  }
      check(TFloat()) { IntPrimitive(1    ).add(FloatPrimitive(2.0))  }
      check(TBool())  { IntPrimitive(1).gt(IntPrimitive(0)) }
      check(TBool())  { IntPrimitive(1).isNull }
      check(TBool())  { IntPrimitive(1).isNull.not }

    }

    "Check CAST operations" >> {
      check(TInt())   { CastExpression(TInt(), StringPrimitive("1")) }
    }

    "Check Conditionals" >> {
      check(TInt())   { BoolPrimitive(true).thenElse { IntPrimitive(1) } { IntPrimitive(2) } }
      check(TFloat()) { BoolPrimitive(true).thenElse { FloatPrimitive(1.0) } { FloatPrimitive(2) } }
      check(TFloat()) { BoolPrimitive(true).thenElse { FloatPrimitive(1.0) } { IntPrimitive(2) } }
    }

    "Check Functions" >> {
      check(TInt())   { Function("BITWISE_AND", Seq(IntPrimitive(1), IntPrimitive(3))) }
      check(TFloat()) { Function("SQRT", Seq(FloatPrimitive(4.0))) }
    }

    
  }

}