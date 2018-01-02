package mimir.algebra

import mimir.Database
import mimir.algebra.function._
import mimir.statistics.SystemCatalog

// Need To Implement Functions
// - MIMIR_TYPE_LU_BOUND
// - MIMIR_TYPE_OF_AGGREGATE
// - MIMIR_TYPE_ESCALATE

object TypecheckQuery 
{

  val LU_BOUND_AGGREGATE = "MIMIR_TYPE_LU_BOUND_AG"
  val LU_BOUND_FUNCTION = "MIMIR_TYPE_LU_BOUND_FUN"
  val GET_AGG_TYPE = "MIMIR_TYPE_OF_AGGREGATE"
  val GET_FN_TYPE = "MIMIR_TYPE_OF_FUNCTION"
  val GET_ARITH_TYPE = "MIMIR_TYPE_OF_ARITH"
  val GET_VGTERM_TYPE = "MIMIR_TYPE_OF_VGTERM"

  def apply(expr: Expression, bindings: Map[String, Expression] = Map()): Expression =
  {
    expr match {
      case p: PrimitiveValue => TypePrimitive(p.getType)

      case CastExpression(t, _) => TypePrimitive(t)

      case Arithmetic(op, a, b) => Function(GET_ARITH_TYPE, Seq(
                                                StringPrimitive(Arith.opString(op)), 
                                                apply(a), 
                                                apply(b)
                                            ))

      case (_:Comparison) | (_:IsNullExpression) | (_:Not) => TypePrimitive(TBool())

      case Conditional(cond, t, e) => Function(LU_BOUND_FUNCTION, Seq(apply(t), apply(e)))

      case Function(fn, args) => Function(GET_FN_TYPE, Seq(StringPrimitive(fn)) ++ args.map { apply(_) } )

      case JDBCVar(t) => return TypePrimitive(t)

      case RowIdVar() => return TypePrimitive(TRowId())

      case VGTerm(name, idx, args, hints) => Function(GET_VGTERM_TYPE, Seq(StringPrimitive(name), IntPrimitive(idx)) ++ args.map { apply(_) })

      case p:Proc => throw new RAException(s"Query-based typechecking of Procs unsupported in $expr")

      case Var(v) => apply(bindings(v))
    }
  }
  
  def mapSchema(
    srcSchema: Operator, 
    projections:Seq[(String, Expression)], 
    postprocess:(Operator => Operator) = (x => x)
  ): Operator =
  {
    // Recursively compute the schema of the nested expression
    val relevantColumns = 
      projections
        // Start with the expressions
        .map { _._2 }
        // Grab the variables from each
        .flatMap { ExpressionUtils.getColumns(_) }
        // Eliminate duplicates
        .toSet

    // Pivot the "Relevant Column" rows of the input schema into columns
    // For Example:
    // ___________________
    // |__Name__|__Type__|
    // |   A    | String |
    // |___B____|__Int___|
    //
    // becomes:
    // __________________
    // |___A____|___B___|
    // |_String_|__Int__|
    // 
    val schemaPivot = 
      srcSchema.pivot(
        SystemCatalog.attrNameColumn,
        SystemCatalog.attrTypeColumn,
        relevantColumns.toSeq.map { col => StringPrimitive(col) -> col }
      )

    // Assemble the new schema as one row
    // For example if we're mapping Project[C <- A+B]( R(A,B) )
    // The result should be something like:
    // Project[C <- TYPE_ESCALATE('+', A, B)]( schemaPivot )
    val projectionTypeComputations = 
      projections.map { proj => proj._1 -> apply(proj._2) }
    val newSchemaPivoted = 
      schemaPivot.map { projectionTypeComputations:_* }

    // Apply any relevant post-processing steps before unpivoting
    val newSchemaPostprocessed = postprocess(newSchemaPivoted)

    // Split the row up into independent columns by reversing the pivot
    val newSchemaByRows =
      newSchemaPostprocessed.unpivot(
        SystemCatalog.attrNameColumn,
        SystemCatalog.attrTypeColumn,
        newSchemaPostprocessed.columnNames.map { col => col -> StringPrimitive(col) }
      )

    return newSchemaByRows
  }

  def apply(oper: Operator): Operator =
  {
    oper match {

      case Project(args, src) => 
      {
        return mapSchema(apply(src), args.map { arg => (arg.name, arg.expression) })
      }

      case Select(cond, src) => 
      {
        return apply(src)
      }

      case Join(lhs, rhs) => 
      {
        return apply(lhs).union { apply(rhs) }
      }

      case LeftOuterJoin(lhs, rhs, cond) => 
      {
        return apply(lhs).union { apply(rhs) }
      }        

      case Union(lhs, rhs) => 
      {
        return apply(lhs)
            .union { apply(rhs) }
            .groupBy( Var(SystemCatalog.attrNameColumn) ){
              AggFunction(
                LU_BOUND_AGGREGATE, false, 
                Seq(Var(SystemCatalog.attrTypeColumn)), 
                SystemCatalog.attrTypeColumn
              )
            }
      }

      case Aggregate(groupBy, aggregates, src) => 
      {

        // To make our lives easier, we're first going to standardize the 
        // aggregate function arguments so that each argument is in a 
        // predictably named variable.  For example:
        // AggFunction("FOO", false, Seq(Var(A), Arithmetic(Arith.Plus, Var(B), Var(C))), "COOKIE")
        // becomes:
        // AggFunction("FOO", false, Seq(Var(1_COOKIE), Var(2_COOKIE)))
        // and we pull out the equivalencies:
        // 1_COOKIE <- A, 2_COOKIE <- B+C

        val aggregateExpressions = 
          aggregates.flatMap { case AggFunction(name, _, args, alias) =>
            args.zipWithIndex.map { arg => 
              ( arg._2+"_"+alias -> arg._1 )
            }
          }

        // We use the projection mapper
        return mapSchema(
          apply(src),
          groupBy.map { gb => gb.name -> gb } ++ aggregateExpressions,
          // and as a final step, we also need to apply the aggregate functions
          // This method gets a horizontal schema as an input and is responsible
          // for outputting the aggregate function types.
          aggregateInputSchema => {
            val aggregateTypeExpressions = 
              aggregates.map { case AggFunction(name, _, args, alias) =>
                alias -> Function(
                    GET_AGG_TYPE, 
                    Seq(StringPrimitive(name))
                      ++ (0 until args.length).map { i => Var(i+"_"+alias) }
                  )
              }

            val outputSchemaExpressions =
              groupBy.map { gb => gb.name -> gb } ++ aggregateTypeExpressions

            aggregateInputSchema.map { outputSchemaExpressions:_* }
          }
        )
      }

      case Sort(sorts, src) => ???

      case Limit(offset, count, src) => ???

      case Table(name, alias, sch, metadata) => ???

      case HardTable(schema, data) => ???

      case View(name, query, annotations) => ???

      case AdaptiveView(schema, name, query, annotations) => ???

      case Annotate(src, invisSchema) => ???

      case Recover(src, invisSchema) => ???

      case ProvenanceOf(query) => ???
    }
  }

  def registerFunctions(db: Database)
  {
    db.functions.register(
      GET_ARITH_TYPE,
      { 
        case Seq(StringPrimitive(op), TypePrimitive(a), TypePrimitive(b)) => 
          Typechecker.escalate(a, b, Arith.fromString(op)) match {
            case Some(t) => TypePrimitive(t)
            case None => NullPrimitive()
          }
        case _ => NullPrimitive()
      },
      (_) => TType()
    )
    db.functions.register(
      LU_BOUND_FUNCTION,
      { 
        case Seq(TypePrimitive(a), TypePrimitive(b)) => 
          Typechecker.leastUpperBound(a, b) match {
            case Some(t) => TypePrimitive(t)
            case None => NullPrimitive()
          }
        case _ => NullPrimitive()
      },
      (_) => TType()
    )
    db.functions.register(
      GET_FN_TYPE,
      (args:Seq[PrimitiveValue]) => {
        val fname = args(0).asString
        val argTypes = args.tail.map { case TypePrimitive(t) => t; case _ => TAny() }

        db.functions.getOption(fname) match {
          case Some(f: NativeFunction) => 
            TypePrimitive(f.typechecker(argTypes))
          case Some(f: ExpressionFunction) => 
            TypePrimitive(
              db.typechecker.typeOf(f.expr, f.args.zip(argTypes).toMap)
            )
          case Some(f: FoldFunction) => {
            val placeholders = (0 until argTypes.length).map { "PLACEHOLDER_"+_ }
            TypePrimitive(
              db.typechecker.typeOf(
                f.unfold(placeholders.map { Var(_) }),
                placeholders.zip(argTypes).toMap
              )
            )
          }
          case None => NullPrimitive()
        }
      },
      (_) => TType()
    )
    db.functions.register(
      GET_VGTERM_TYPE,
      (args:Seq[PrimitiveValue]) => {
        val model = args(0).asString
        val idx   = args(1).asLong.toInt
        val argTypes = args.tail.tail.map { case TypePrimitive(t) => t; case _ => TAny() }

        TypePrimitive(db.models.get(model).varType(idx, argTypes))
      },
      (_) => TType()
    )
  }

}