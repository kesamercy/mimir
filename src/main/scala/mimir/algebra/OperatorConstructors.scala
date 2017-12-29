package mimir.algebra

import mimir.parser.ExpressionParser

trait OperatorConstructors
{
  def toOperator: Operator

  def union(other: Operator): Operator =
    Union(toOperator, other)

  def join(other: Operator, on: Expression = BoolPrimitive(true)): Operator =
    Join(toOperator, other).filter(on)

  def filter(condition: Expression): Operator =
    condition match {
      case BoolPrimitive(true)  => toOperator
      case _ => Select(condition, toOperator)
    }
  def filterParsed(condition: String): Operator =
    filter(ExpressionParser.expr(condition))

  def project(cols: String*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) } )

  def projectNoInline(cols: String*): Operator =
    mapImpl(cols.map { col => (col, Var(col)) }, noInline = true)

  def mapParsed(cols: (String, String)*): Operator =
    mapImpl(cols.map { case (name, expr) => (name, ExpressionParser.expr(expr)) } )

  def map(cols: (String, Expression)*): Operator =
    mapImpl(cols)

  def mapParsedNoInline(cols: (String, String)*): Operator =
    mapImpl(
      cols.map { case (name, expr) => (name, ExpressionParser.expr(expr)) },
      noInline = true
    )

  def mapNoInline(cols: (String, Expression)*): Operator =
    mapImpl(cols, noInline = true)

  def mapImpl(cols: Seq[(String, Expression)], noInline:Boolean = false): Operator =
  {
    val (oldProjections, strippedOperator) =
      if(noInline) { 
        (toOperator.columnNames.map { x => ProjectArg(x, Var(x)) }, toOperator)
      } else {
        OperatorUtils.extractProjections(toOperator)
      }
    val lookupMap: Map[String,Expression] = 
      oldProjections
        .map { _.toBinding }
        .toMap

    Project(
      cols.map { case (col, expr) => 
        ProjectArg(col, Eval.inline(expr, lookupMap))
      },
      strippedOperator
    )
  }

  def rename(targets: (String, String)*): Operator =
  {
    val renamings = targets.toMap
    map(toOperator.columnNames.map { 
      case name if renamings contains name => (renamings(name), Var(name)) 
      case name => (name, Var(name))
    }:_*)
  }

  def removeColumns(targets: String*): Operator =
    removeColumn(targets:_*)
  def removeColumn(targets: String*): Operator =
  {
    val isTarget = targets.toSet
    val (cols, src) = OperatorUtils.extractProjections(toOperator)
    Project(
      cols.filterNot { col => isTarget(col.name.toUpperCase) },
      src
    )
  }

  def addColumn(newCols: (String, Expression)*): Operator =
  {
    val (cols, src) = OperatorUtils.extractProjections(toOperator)
    val bindings = cols.map { _.toBinding }.toMap
    Project(
      cols ++ newCols.map { col => ProjectArg(col._1, Eval.inline(col._2, bindings)) },
      src
    )
  }

  def distinct: Operator =
  {
    val base = toOperator
    Aggregate(
      base.columnNames.map { Var(_) },
      Seq(),
      base
    )
  }

  def aggregateParsed(agg: (String, String)*): Operator =
    groupByParsed()(agg:_*)

  def aggregate(agg: AggFunction*): Operator =
    groupBy()(agg:_*)

  def groupByParsed(gb: String*)(agg: (String, String)*): Operator =
    groupBy(gb.map { Var(_) }:_*)(
      agg.map { case (alias, fnExpr) => 
        val fn = ExpressionParser.function(fnExpr)
        AggFunction(fn.op, false, fn.params, alias)
      }:_*)

  def groupBy(gb: Var*)(agg: AggFunction*): Operator =
  {
    Aggregate(gb, agg, toOperator)
  }


  def count(distinct: Boolean = false, alias: String = "COUNT"): Operator =
  {
    Aggregate(
      Seq(), 
      Seq(AggFunction("COUNT", distinct, Seq(), alias)), 
      toOperator
    )
  }

  def sort(sortCols: (String, Boolean)*): Operator =
    Sort( sortCols.map { col => SortColumn(Var(col._1), col._2) }, toOperator )

  def limit(count: Int = -1, offset: Int = 0 ): Operator =
    Limit( offset, if(count >= 0) { Some(count) } else { None }, toOperator )


  /**
   * Pivot the specified operator (https://en.wikipedia.org/wiki/Pivot_table)
   * 
   * The general idea is that this transformation "rotates" rows into columns
   * For example, let's assume we start with the table: 
   * _________________
   * |__Name__|__ID__|
   * |  Alice |  1   |
   * |  Bob   |  2   |
   * |  Carol |  3   |
   * |__Dave__|__4___|
   * 
   * If we call pivot("Name", "ID", Seq(("Alice", "Bob", ...))), we get:
   * ______________________________
   * |_Alice_|_Bob_|_Carol_|_Dave_|
   * |___1___|__2__|___3___|__4___|
   * 
   * This is a glorified aggregation, with the added caveat that it first 
   * partitions the data by the pivot column and puts each aggregate into a
   * separate column.  Accordingly, it's possible to add group-by columns 
   * (which in turn creates multiple output rows).  
   *
   * It's also possible to change the aggregate from the default of FIRST
   * 
   * Note that because this is a syntactic transformation, and not an 
   * entirely new operator, the set of pivot values needs to be provided
   * explicitly.
   * 
   * @arg pivotColumn     The column containing the values to pivot on
   * @arg valueColumn     The column containing the values to put in the 
   *                      pivot cells
   * @arg columnForValue  The list of distinct PrimitiveValue objects 
   *                      to create columns for and column names for each
   * @arg groupColumns    Group-By columns (default: None)
   * @arg aggregate       How to merge multiple values (default: FIRST)
   * @returns             A table with one column for each element in
   *                      groupColumns ++ columnForValue.map(_._2)
   */
  def pivot(
    pivotColumn: String, 
    valueColumn: String, 
    columnForValue: Seq[(PrimitiveValue, String)], 
    groupColumns: Seq[String] = Seq(),
    aggregate: String = "FIRST"
  ): Operator =
  {
    Aggregate(
      groupColumns.map { Var(_) },
      columnForValue.map { case (columnValue, outputColumnName) => 
        AggFunction(aggregate, false, 
          Seq(
            Var(pivotColumn)
              .eq(columnValue)
              .thenElse { Var(valueColumn) }
                        { NullPrimitive() }
          ),
          outputColumnName
        )
      },
      toOperator
    )
  }

  /**
   * Inverse of the pivot operation: Convert columns of a table into rows.
   *
   * Assume we start with
   * ______________________________
   * |_Alice_|_Bob_|_Carol_|_Dave_|
   * |___1___|__2__|___3___|__4___|
   *
   * If we call pivot("Name", "ID", Seq(("Alice", "Bob", ...))), we get:
   * _________________
   * |__Name__|__ID__|
   * |  Alice |  1   |
   * |  Bob   |  2   |
   * |  Carol |  3   |
   * |__Dave__|__4___|
   * 
   * Optionally, retain some columns in the result.  For example:
   * _______________________________________
   * |_Alice_|_Bob_|_Carol_|_Dave_|_Cookie_|
   * |___1___|__2__|___3___|__4___|__"C"___|
   * 
   * If we call pivot("Name", "ID", Seq(("Alice", "Bob", ...)), retainColumns = "Cookie"), we get:
   * __________________________
   * |__Name__|__ID__|_Cookie_|
   * |  Alice |  1   |  "C"   |
   * |  Bob   |  2   |  "C"   |
   * |  Carol |  3   |  "C"   |
   * |__Dave__|__4___|__"C"___|
   * 
   */
  def unpivot(
    pivotColumn: String, 
    valueColumn: String, 
    valueForColumnName: Seq[(String, PrimitiveValue)],
    retainColumns: Seq[String] = Seq()
  ): Operator =
  {

    val pivotType = 
      Typechecker.leastUpperBound( 
        valueForColumnName.map { _._2.getType } 
      ).getOrElse { 
        throw new RAException(
          s"Unpivot called with incompatible pivot values: ${valueForColumnName.map { _._2 }.mkString(", ")}", 
          Some(toOperator)
        ) 
      }
    Project(
      retainColumns.map { col => ProjectArg(col, Var(col)) }
        ++ Seq(
          ProjectArg(pivotColumn, Var(pivotColumn)),
          ProjectArg(valueColumn, ExpressionUtils.makeCaseExpression(
            valueForColumnName.map { case (columnName, columnValue) => 
              ( Var(pivotColumn).eq(columnValue), Var(columnName) )
            },
            NullPrimitive()
          ))
        ),
      Join(
        HardTable(
          Seq((pivotColumn, pivotType)),
          valueForColumnName.toSeq.map { _._2 }.map { Seq(_) }
        ),
        toOperator
      )
    )

  }
}