# Lexicon: DuckDB-Inspired Query Language

## Executive Summary

This plan outlines the evolution of parcat's query language from basic WHERE filtering to a comprehensive SQL dialect inspired by DuckDB. The goal is to provide powerful analytical capabilities while maintaining simplicity and performance for the CLI use case.

**Timeline**: 4-6 months (phased rollout)
**Complexity**: High (major architectural changes)
**Impact**: Transforms parcat from a simple reader to a powerful analytical tool

## Current State Assessment

### What We Have (v1.0)
```sql
select * from file.parquet where age > 30 AND active = true
```

**Capabilities**:
- ✅ SELECT * (all columns)
- ✅ FROM single file
- ✅ WHERE with comparisons (=, !=, <, >, <=, >=)
- ✅ Boolean logic (AND, OR)
- ✅ Type coercion (numbers, strings, booleans)

**Limitations**:
- ❌ No column projection (can't select specific columns)
- ❌ No aggregations (COUNT, SUM, AVG, etc.)
- ❌ No GROUP BY
- ❌ No ORDER BY
- ❌ No JOINs
- ❌ No functions (string, date, math)
- ❌ No subqueries
- ❌ No LIMIT/OFFSET (have flag-based limit only)

### DuckDB Target Features

DuckDB provides a rich SQL dialect optimized for analytics:

**Core SQL**:
- Full column projection
- Aggregations with GROUP BY
- ORDER BY with multiple columns
- LIMIT/OFFSET
- DISTINCT
- CASE expressions
- Type casting

**Advanced Features**:
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- CTEs (WITH clauses)
- Subqueries (scalar, correlated)
- Multiple file queries (UNION, glob patterns)
- JOINs (INNER, LEFT, RIGHT, FULL)
- Rich function library (150+ functions)

**Analytics-Specific**:
- PIVOT/UNPIVOT
- Time series functions
- Array/List operations
- JSON operations
- Statistical functions

## Goals & Non-Goals

### Goals ✅
1. **Column Projection**: `SELECT name, age FROM file.parquet`
2. **Aggregations**: `SELECT COUNT(*), AVG(age) FROM file.parquet`
3. **GROUP BY**: `SELECT status, COUNT(*) FROM file.parquet GROUP BY status`
4. **ORDER BY**: `SELECT * FROM file.parquet ORDER BY age DESC`
5. **LIMIT/OFFSET**: `SELECT * FROM file.parquet LIMIT 10 OFFSET 20`
6. **Functions**: String (UPPER, LOWER, CONCAT), Math (ROUND, ABS), Date (DATE_TRUNC)
7. **IN operator**: `WHERE status IN ('active', 'pending')`
8. **LIKE operator**: `WHERE name LIKE 'alice%'`
9. **BETWEEN**: `WHERE age BETWEEN 25 AND 40`
10. **DISTINCT**: `SELECT DISTINCT status FROM file.parquet`

### Stretch Goals 🎯
11. **Window Functions**: `SELECT name, ROW_NUMBER() OVER (ORDER BY age) FROM file.parquet`
12. **CTEs**: `WITH active_users AS (...) SELECT * FROM active_users`
13. **Subqueries**: `SELECT * FROM file.parquet WHERE age > (SELECT AVG(age) FROM file.parquet)`
14. **Multi-file**: `SELECT * FROM 'data/*.parquet'`
15. **JOINs**: `SELECT * FROM users.parquet JOIN orders.parquet ON users.id = orders.user_id`

### Non-Goals ❌
- **Data modification**: No INSERT/UPDATE/DELETE (read-only tool)
- **Schema modification**: No CREATE/ALTER/DROP
- **Transactions**: No BEGIN/COMMIT/ROLLBACK
- **Stored procedures**: No procedural SQL
- **Full DuckDB compatibility**: Subset focused on analytics

## Architecture Changes Required

### Current Architecture
```
Query String → Lexer → Parser → AST → Filter → Results
```

**Current AST**:
- `Query` (table name + filter)
- `BinaryExpr` (AND/OR)
- `ComparisonExpr` (column op value)

**Limitations**:
- No column selection (always SELECT *)
- No aggregations or grouping
- No sorting
- Filter-only evaluation (no projection)

### Target Architecture
```
Query String → Lexer → Parser → AST → Query Planner → Executor → Results
```

**New Components**:

1. **Enhanced Lexer**:
   - More keywords (GROUP, BY, ORDER, LIMIT, OFFSET, DISTINCT, AS, IN, LIKE, BETWEEN)
   - Function names as identifiers
   - Parentheses for grouping and function calls
   - Comma for list separators

2. **Enhanced Parser**:
   - SELECT list parsing (columns, aggregates, expressions)
   - FROM clause with aliases
   - GROUP BY clause
   - HAVING clause (post-aggregation filtering)
   - ORDER BY clause with ASC/DESC
   - LIMIT/OFFSET clause

3. **New AST Nodes**:
   - `SelectStmt` (complete query structure)
   - `SelectList` (projected columns)
   - `ColumnRef` (column references)
   - `FunctionCall` (function invocations)
   - `AggregateExpr` (COUNT, SUM, AVG, etc.)
   - `OrderBy` (sort specification)
   - `GroupBy` (grouping specification)

4. **Query Planner**:
   - Validates column references
   - Plans execution order (filter → group → aggregate → sort → limit)
   - Optimizes filters (push down where possible)
   - Handles expression evaluation order

5. **Executor**:
   - Row filtering (WHERE)
   - Column projection (SELECT)
   - Grouping (GROUP BY)
   - Aggregation (COUNT, SUM, etc.)
   - Sorting (ORDER BY)
   - Limiting (LIMIT/OFFSET)

### Data Flow Example

**Query**:
```sql
SELECT status, COUNT(*) as total, AVG(age) as avg_age
FROM users.parquet
WHERE active = true
GROUP BY status
HAVING total > 10
ORDER BY avg_age DESC
LIMIT 5
```

**Execution Plan**:
1. **Scan**: Read users.parquet
2. **Filter**: Apply `WHERE active = true`
3. **Project**: Extract columns: status, age
4. **Group**: Group rows by status
5. **Aggregate**: Compute COUNT(*) and AVG(age) per group
6. **Filter**: Apply `HAVING total > 10`
7. **Sort**: Order by avg_age DESC
8. **Limit**: Take first 5 rows
9. **Project**: Output status, total, avg_age

## Implementation Phases

### Phase 1: Column Projection & Basic Functions (Weeks 1-3)

**Goal**: Support `SELECT col1, col2 FROM file.parquet` and simple functions.

**Changes**:
1. **Lexer**: Add tokens for comma, parentheses, AS keyword
2. **Parser**: Parse SELECT list instead of assuming *
3. **AST**: Add `SelectList`, `ColumnRef`, `FunctionCall` nodes
4. **Executor**: Implement column projection
5. **Functions**: Implement basic functions (UPPER, LOWER, ABS, ROUND)

**New Query Support**:
```sql
SELECT name, age FROM users.parquet WHERE age > 30
SELECT UPPER(name), age * 2 as double_age FROM users.parquet
SELECT name, ROUND(score, 2) FROM users.parquet
```

**Deliverables**:
- [ ] Update lexer with new tokens
- [ ] Parse SELECT list (columns, aliases, expressions)
- [ ] Implement column projection in executor
- [ ] Add function registry
- [ ] Implement 10 basic functions (string, math)
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- Breaking change (SELECT * now needs to be explicit or default)
- Parser complexity increases significantly

**Mitigation**:
- Keep backward compatibility: default to SELECT * if not specified
- Comprehensive test suite for new syntax

---

### Phase 2: Aggregations & GROUP BY (Weeks 4-6)

**Goal**: Support aggregations and grouping.

**Changes**:
1. **Lexer**: Add GROUP, BY, HAVING keywords
2. **Parser**: Parse GROUP BY and HAVING clauses
3. **AST**: Add `AggregateExpr`, `GroupBy` nodes
4. **Executor**: Implement grouping and aggregation logic
5. **Aggregates**: Implement COUNT, SUM, AVG, MIN, MAX

**New Query Support**:
```sql
SELECT COUNT(*) FROM users.parquet
SELECT status, COUNT(*) as total FROM users.parquet GROUP BY status
SELECT status, AVG(age) FROM users.parquet GROUP BY status HAVING AVG(age) > 30
SELECT department, status, COUNT(*) FROM users.parquet GROUP BY department, status
```

**Deliverables**:
- [ ] Parse GROUP BY clause
- [ ] Parse HAVING clause
- [ ] Implement grouping algorithm (hash-based)
- [ ] Implement 5 aggregate functions
- [ ] Support aggregate aliases in HAVING
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- Memory usage for large groups
- Complexity of HAVING clause (references aggregates)

**Mitigation**:
- Use efficient hash-based grouping
- Limit group count (security)
- Clear error messages for invalid queries

---

### Phase 3: ORDER BY & LIMIT/OFFSET (Weeks 7-8)

**Goal**: Support sorting and pagination.

**Changes**:
1. **Lexer**: Add ORDER, BY, ASC, DESC, OFFSET keywords
2. **Parser**: Parse ORDER BY and LIMIT/OFFSET clauses
3. **AST**: Add `OrderBy` node
4. **Executor**: Implement sorting logic

**New Query Support**:
```sql
SELECT * FROM users.parquet ORDER BY age DESC
SELECT * FROM users.parquet ORDER BY department ASC, age DESC
SELECT * FROM users.parquet ORDER BY age LIMIT 10
SELECT * FROM users.parquet ORDER BY age LIMIT 10 OFFSET 20
```

**Deliverables**:
- [ ] Parse ORDER BY with multiple columns
- [ ] Parse ASC/DESC modifiers
- [ ] Implement multi-column sorting
- [ ] Move LIMIT/OFFSET from flags to SQL
- [ ] Support ORDER BY with aliases
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- Sorting large datasets (memory)
- ORDER BY with aggregates/expressions

**Mitigation**:
- Warn on large sorts
- Support ORDER BY referring to SELECT list positions

---

### Phase 4: Enhanced Operators (Weeks 9-10)

**Goal**: Support IN, LIKE, BETWEEN, IS NULL, DISTINCT.

**Changes**:
1. **Lexer**: Add IN, LIKE, BETWEEN, DISTINCT, NULL, IS keywords
2. **Parser**: Parse new operator syntax
3. **AST**: Add `InExpr`, `LikeExpr`, `BetweenExpr`, `IsNullExpr`
4. **Executor**: Implement operator logic

**New Query Support**:
```sql
SELECT DISTINCT status FROM users.parquet
SELECT * FROM users.parquet WHERE status IN ('active', 'pending')
SELECT * FROM users.parquet WHERE name LIKE 'alice%'
SELECT * FROM users.parquet WHERE age BETWEEN 25 AND 40
SELECT * FROM users.parquet WHERE email IS NULL
SELECT * FROM users.parquet WHERE email IS NOT NULL
```

**Deliverables**:
- [ ] Parse IN with value list
- [ ] Implement LIKE with wildcards (%, _)
- [ ] Implement BETWEEN operator
- [ ] Implement IS NULL / IS NOT NULL
- [ ] Implement DISTINCT (hash-based deduplication)
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- LIKE performance on large datasets
- DISTINCT memory usage

**Mitigation**:
- Optimize LIKE with prefix matching
- Limit distinct row count
- Consider approximate distinct

---

### Phase 5: Rich Function Library (Weeks 11-12)

**Goal**: Implement comprehensive function library.

**Function Categories**:

1. **String Functions** (15 functions):
   - `UPPER(s)`, `LOWER(s)`, `LENGTH(s)`, `TRIM(s)`
   - `LTRIM(s)`, `RTRIM(s)`, `CONCAT(s1, s2, ...)`
   - `SUBSTRING(s, start, len)`, `REPLACE(s, old, new)`
   - `SPLIT(s, delim)`, `REVERSE(s)`, `CONTAINS(s, substr)`
   - `STARTS_WITH(s, prefix)`, `ENDS_WITH(s, suffix)`
   - `REPEAT(s, n)`

2. **Math Functions** (12 functions):
   - `ABS(x)`, `ROUND(x, decimals)`, `FLOOR(x)`, `CEIL(x)`
   - `SQRT(x)`, `POW(x, y)`, `MOD(x, y)`
   - `MIN(x, y)`, `MAX(x, y)`, `SIGN(x)`
   - `TRUNC(x)`, `RANDOM()`

3. **Date/Time Functions** (10 functions):
   - `NOW()`, `CURRENT_DATE()`, `CURRENT_TIME()`
   - `DATE_TRUNC(unit, date)`, `DATE_PART(unit, date)`
   - `DATE_ADD(date, interval)`, `DATE_SUB(date, interval)`
   - `DATE_DIFF(date1, date2)`, `YEAR(date)`, `MONTH(date)`

4. **Type Conversion** (5 functions):
   - `CAST(x AS type)`, `TRY_CAST(x AS type)`
   - `TO_STRING(x)`, `TO_NUMBER(s)`, `TO_DATE(s)`

5. **Conditional** (3 functions):
   - `CASE WHEN ... THEN ... ELSE ... END`
   - `COALESCE(v1, v2, ...)`, `NULLIF(v1, v2)`

6. **Aggregate Functions** (8 functions):
   - `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`
   - `MIN(col)`, `MAX(col)`, `STDDEV(col)`, `VARIANCE(col)`

**Deliverables**:
- [ ] Create function registry system
- [ ] Implement 50+ functions across categories
- [ ] Add function documentation
- [ ] Support variadic functions (CONCAT, COALESCE)
- [ ] Implement CASE expressions
- [ ] Update tests (function-specific test suite)
- [ ] Update documentation with function reference

**Risks**:
- Large number of functions to implement
- Type system complexity (dates, casting)
- Function documentation maintenance

**Mitigation**:
- Implement core functions first, add more iteratively
- Use table-driven tests
- Auto-generate function reference from code

---

### Phase 6: Window Functions (Weeks 13-15) [STRETCH]

**Goal**: Support analytical window functions.

**Changes**:
1. **Lexer**: Add OVER, PARTITION, ROWS, RANGE keywords
2. **Parser**: Parse window specifications
3. **AST**: Add `WindowExpr`, `WindowSpec` nodes
4. **Executor**: Implement window evaluation

**Window Functions**:
- **Ranking**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`
- **Value**: `FIRST_VALUE(col)`, `LAST_VALUE(col)`, `NTH_VALUE(col, n)`
- **Offset**: `LAG(col, offset)`, `LEAD(col, offset)`
- **Aggregate**: `SUM() OVER`, `AVG() OVER`, `COUNT() OVER`

**New Query Support**:
```sql
SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) as rank
FROM users.parquet

SELECT name, department, salary,
       AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM employees.parquet

SELECT date, value,
       LAG(value) OVER (ORDER BY date) as prev_value,
       LEAD(value) OVER (ORDER BY date) as next_value
FROM timeseries.parquet
```

**Deliverables**:
- [ ] Parse OVER clause
- [ ] Parse PARTITION BY
- [ ] Parse window frame specification (ROWS, RANGE)
- [ ] Implement window execution model
- [ ] Implement 10 window functions
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- High complexity
- Memory usage for large windows
- Performance for multiple window functions

**Mitigation**:
- Start with simple windows (ORDER BY only)
- Optimize window evaluation (share partitions)
- Clear documentation of limitations

---

### Phase 7: CTEs & Subqueries (Weeks 16-18) [STRETCH]

**Goal**: Support WITH clauses and subqueries.

**Changes**:
1. **Lexer**: Add WITH, RECURSIVE keywords
2. **Parser**: Parse CTE and subquery syntax
3. **AST**: Add `CTE`, `Subquery` nodes
4. **Executor**: Implement materialization and subquery evaluation

**New Query Support**:
```sql
-- CTE
WITH active_users AS (
  SELECT * FROM users.parquet WHERE active = true
)
SELECT status, COUNT(*) FROM active_users GROUP BY status

-- Scalar subquery
SELECT name, age FROM users.parquet
WHERE age > (SELECT AVG(age) FROM users.parquet)

-- IN subquery
SELECT * FROM users.parquet
WHERE department IN (SELECT dept FROM large_depts.parquet)

-- EXISTS
SELECT * FROM users.parquet u
WHERE EXISTS (SELECT 1 FROM orders.parquet o WHERE o.user_id = u.id)
```

**Deliverables**:
- [ ] Parse WITH clause (non-recursive)
- [ ] Parse subqueries in FROM, WHERE, SELECT
- [ ] Implement CTE materialization
- [ ] Implement scalar subquery evaluation
- [ ] Implement IN/EXISTS with subquery
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- Very high complexity
- Memory for materialized CTEs
- Correlated subqueries (performance)

**Mitigation**:
- Start with simple CTEs (no recursion)
- Materialize CTEs (don't re-evaluate)
- Document correlated subquery limitations

---

### Phase 8: Multi-File & JOINs (Weeks 19-22) [STRETCH]

**Goal**: Support multiple files and JOIN operations.

**Changes**:
1. **Lexer**: Add JOIN, INNER, LEFT, RIGHT, FULL, ON keywords
2. **Parser**: Parse multiple FROM sources and JOIN syntax
3. **Reader**: Support multiple file reading
4. **Executor**: Implement join algorithms (hash join, nested loop)

**New Query Support**:
```sql
-- Multi-file
SELECT * FROM 'data/*.parquet' WHERE date > '2024-01-01'

-- JOIN
SELECT u.name, o.order_date, o.amount
FROM users.parquet u
JOIN orders.parquet o ON u.id = o.user_id
WHERE o.amount > 100

-- LEFT JOIN
SELECT u.name, COUNT(o.id) as order_count
FROM users.parquet u
LEFT JOIN orders.parquet o ON u.id = o.user_id
GROUP BY u.name
```

**Deliverables**:
- [ ] Parse glob patterns in FROM
- [ ] Implement multi-file reading
- [ ] Parse JOIN syntax (all types)
- [ ] Implement hash join algorithm
- [ ] Implement nested loop join
- [ ] Support join conditions (equi and non-equi)
- [ ] Update tests
- [ ] Update documentation

**Risks**:
- Extremely high complexity
- Memory for hash tables
- Performance for large joins
- Cross product (Cartesian) dangers

**Mitigation**:
- Start with INNER JOIN only
- Require equi-join conditions
- Limit result set size
- Warn on missing join conditions

---

## Enhanced AST Design

### Current AST
```go
type Query struct {
    TableName string
    Filter    Expression
}

type Expression interface {
    Evaluate(row map[string]interface{}) (bool, error)
}

type BinaryExpr struct {
    Left     Expression
    Operator TokenType
    Right    Expression
}

type ComparisonExpr struct {
    Column   string
    Operator TokenType
    Value    interface{}
}
```

### Target AST
```go
// SelectStmt is the root of a SELECT query
type SelectStmt struct {
    SelectList  []SelectItem      // Columns/expressions to return
    From        *FromClause       // Table source
    Where       Expression        // Row filter
    GroupBy     *GroupByClause    // Grouping specification
    Having      Expression        // Post-aggregation filter
    OrderBy     *OrderByClause    // Sort specification
    Limit       *int64            // Row limit
    Offset      *int64            // Row offset
    Distinct    bool              // DISTINCT modifier
}

// SelectItem represents a column or expression in SELECT list
type SelectItem struct {
    Expr  Expression  // Column, function, or expression
    Alias string      // Optional alias (AS name)
}

// FromClause specifies data source
type FromClause struct {
    Source TableSource  // Table or subquery
    Alias  string       // Optional table alias
}

// TableSource is a source of rows
type TableSource interface {
    isTableSource()
}

// TableRef references a file
type TableRef struct {
    Path string  // File path or glob pattern
}

// SubqueryRef references a subquery
type SubqueryRef struct {
    Query *SelectStmt
}

// Expression is any evaluatable expression
type Expression interface {
    Evaluate(ctx *EvalContext) (interface{}, error)
}

// ColumnRef references a column
type ColumnRef struct {
    Table  string  // Optional table qualifier
    Column string  // Column name
}

// LiteralExpr is a constant value
type LiteralExpr struct {
    Value interface{}
}

// BinaryExpr is a binary operation
type BinaryExpr struct {
    Left     Expression
    Operator TokenType
    Right    Expression
}

// FunctionCall is a function invocation
type FunctionCall struct {
    Name string
    Args []Expression
    // WindowSpec for window functions
    Window *WindowSpec
}

// AggregateExpr is an aggregate function
type AggregateExpr struct {
    Function string      // COUNT, SUM, AVG, etc.
    Arg      Expression  // Argument (or nil for COUNT(*))
    Distinct bool        // DISTINCT modifier
}

// CaseExpr is a CASE expression
type CaseExpr struct {
    Cases []CaseWhen
    Else  Expression
}

type CaseWhen struct {
    When Expression
    Then Expression
}

// InExpr is an IN expression
type InExpr struct {
    Expr   Expression
    List   []Expression  // Value list or subquery
    Negate bool          // NOT IN
}

// LikeExpr is a LIKE expression
type LikeExpr struct {
    Expr    Expression
    Pattern string
    Negate  bool  // NOT LIKE
}

// BetweenExpr is a BETWEEN expression
type BetweenExpr struct {
    Expr   Expression
    Lower  Expression
    Upper  Expression
    Negate bool  // NOT BETWEEN
}

// IsNullExpr is an IS NULL expression
type IsNullExpr struct {
    Expr   Expression
    Negate bool  // IS NOT NULL
}

// GroupByClause specifies grouping
type GroupByClause struct {
    Columns []Expression
}

// OrderByClause specifies sorting
type OrderByClause struct {
    Items []OrderByItem
}

type OrderByItem struct {
    Expr Expression
    Desc bool  // DESC vs ASC
}

// WindowSpec specifies a window
type WindowSpec struct {
    PartitionBy []Expression
    OrderBy     *OrderByClause
    Frame       *WindowFrame
}

type WindowFrame struct {
    Type  FrameType  // ROWS or RANGE
    Start FrameBound
    End   FrameBound
}

type FrameBound struct {
    Type   BoundType  // UNBOUNDED, CURRENT, OFFSET
    Offset int64
}
```

## Executor Design

### Current Executor
```go
// Simple filter-only execution
rows, _ := reader.ReadAll()
filtered, _ := ApplyFilter(rows, query.Filter)
formatter.Format(filtered)
```

### Target Executor
```go
// ExecutionContext holds query state
type ExecutionContext struct {
    Query    *SelectStmt
    Schema   *Schema
    Stats    *ExecutionStats
}

// Executor executes a query
type Executor struct {
    ctx *ExecutionContext
}

// Execute runs the query
func (e *Executor) Execute(query *SelectStmt, source DataSource) ([]Row, error) {
    // 1. Scan: Read from source
    rows := e.scan(source)

    // 2. Filter: Apply WHERE clause
    if query.Where != nil {
        rows = e.filter(rows, query.Where)
    }

    // 3. Group & Aggregate: Apply GROUP BY
    if query.GroupBy != nil {
        rows = e.groupAndAggregate(rows, query.GroupBy, query.SelectList)

        // 4. Having: Apply HAVING clause
        if query.Having != nil {
            rows = e.filter(rows, query.Having)
        }
    }

    // 5. Project: Apply SELECT list
    rows = e.project(rows, query.SelectList)

    // 6. Distinct: Remove duplicates
    if query.Distinct {
        rows = e.distinct(rows)
    }

    // 7. Sort: Apply ORDER BY
    if query.OrderBy != nil {
        rows = e.sort(rows, query.OrderBy)
    }

    // 8. Limit/Offset: Apply pagination
    if query.Limit != nil || query.Offset != nil {
        rows = e.limitOffset(rows, query.Limit, query.Offset)
    }

    return rows, nil
}
```

### Execution Components

**Scanner**:
```go
type Scanner interface {
    Scan() ([]Row, error)
}

type ParquetScanner struct {
    path   string
    reader *reader.Reader
}
```

**Filter**:
```go
type Filter struct {
    expr Expression
}

func (f *Filter) Apply(rows []Row) []Row {
    result := make([]Row, 0)
    for _, row := range rows {
        match, _ := f.expr.Evaluate(row)
        if match {
            result = append(result, row)
        }
    }
    return result
}
```

**Grouper**:
```go
type Grouper struct {
    groupBy   []Expression
    aggregates []AggregateExpr
}

func (g *Grouper) Apply(rows []Row) []Row {
    // Hash-based grouping
    groups := make(map[string]*Group)

    for _, row := range rows {
        key := g.computeGroupKey(row)
        if group, exists := groups[key]; exists {
            group.Aggregate(row)
        } else {
            groups[key] = NewGroup(row, g.aggregates)
        }
    }

    return g.materializeGroups(groups)
}
```

**Sorter**:
```go
type Sorter struct {
    orderBy []OrderByItem
}

func (s *Sorter) Apply(rows []Row) []Row {
    sort.Slice(rows, func(i, j int) bool {
        for _, item := range s.orderBy {
            cmp := s.compare(rows[i], rows[j], item)
            if cmp != 0 {
                if item.Desc {
                    return cmp > 0
                }
                return cmp < 0
            }
        }
        return false
    })
    return rows
}
```

## Function Registry

```go
// Function represents a scalar function
type Function interface {
    Name() string
    Arity() int  // -1 for variadic
    Evaluate(args []interface{}) (interface{}, error)
}

// FunctionRegistry manages function lookup
type FunctionRegistry struct {
    functions map[string]Function
}

// Register functions
func init() {
    registry := NewRegistry()

    // String functions
    registry.Register(&UpperFunc{})
    registry.Register(&LowerFunc{})
    registry.Register(&ConcatFunc{})

    // Math functions
    registry.Register(&AbsFunc{})
    registry.Register(&RoundFunc{})

    // Date functions
    registry.Register(&NowFunc{})

    // Aggregate functions (handled separately)
    // COUNT, SUM, AVG, MIN, MAX, etc.
}

// Example function implementation
type UpperFunc struct{}

func (f *UpperFunc) Name() string { return "UPPER" }
func (f *UpperFunc) Arity() int { return 1 }
func (f *UpperFunc) Evaluate(args []interface{}) (interface{}, error) {
    str, ok := args[0].(string)
    if !ok {
        return nil, fmt.Errorf("UPPER expects string argument")
    }
    return strings.ToUpper(str), nil
}
```

## Performance Considerations

### Memory Management
- **Streaming where possible**: Don't materialize entire result sets
- **Group limits**: Cap number of distinct groups
- **Sort limits**: Warn on large sorts without LIMIT
- **Window frame limits**: Restrict window sizes

### Optimizations
- **Predicate pushdown**: Apply filters early
- **Projection pushdown**: Only read needed columns from parquet
- **Join ordering**: Smaller table as build side
- **Index usage**: Consider adding indexes for large files

### Resource Limits
```go
const (
    MaxRowsInMemory    = 10_000_000  // 10M rows
    MaxGroupCount      = 100_000      // 100K groups
    MaxSortSize        = 1_000_000    // 1M rows
    MaxFunctionDepth   = 100          // Nested function calls
    MaxJoinSize        = 1_000_000    // Join result size
)
```

## Testing Strategy

### Unit Tests
- **Lexer**: Every keyword, operator, edge case
- **Parser**: Every clause, error case, precedence
- **Functions**: Every function, type combinations
- **Aggregates**: Empty groups, nulls, type mismatches
- **Sorting**: Multiple columns, nulls, DESC/ASC
- **Window**: Partitioning, ordering, frames

### Integration Tests
- **End-to-end queries**: Real parquet files
- **Complex queries**: Multi-clause combinations
- **Performance**: Large file handling
- **Error cases**: Invalid queries, type errors

### Regression Tests
- **Backward compatibility**: Phase 1 queries still work
- **Edge cases**: Empty results, nulls, type coercion

## Migration & Compatibility

### Backward Compatibility
**v1.0 queries must continue to work**:
```sql
select * from file.parquet where age > 30  # Still works
```

**Default behavior**:
- `select * from file.parquet` = same as before
- Flags `-limit`, `-f` still work (deprecated in favor of SQL)

### Migration Path
1. **Phase 0**: Current implementation (v1.0)
2. **Phase 1-4**: Core SQL (v2.0) - BREAKING: Must be explicit about SELECT *
3. **Phase 5-8**: Advanced SQL (v3.0) - Additive only

### Deprecation Plan
- **v2.0**: Deprecate `-limit` flag (use SQL LIMIT)
- **v3.0**: Remove flag (breaking change with warning)

## Documentation Plan

### User Documentation
1. **Query Reference**: Complete SQL syntax guide
2. **Function Reference**: All functions with examples
3. **Examples**: Common queries and patterns
4. **Migration Guide**: v1.0 → v2.0 upgrade path

### Developer Documentation
1. **Architecture**: Lexer → Parser → Executor flow
2. **Adding Functions**: How to add new functions
3. **Performance**: Query optimization guide
4. **Testing**: Test guidelines

## Success Metrics

### Functionality
- ✅ 90% of common analytical queries supported
- ✅ 50+ functions implemented
- ✅ Full column projection support
- ✅ Aggregations and grouping
- ✅ Sorting and pagination

### Performance
- ✅ Handle 10M+ row files
- ✅ Sub-second simple queries
- ✅ <10s complex aggregations
- ✅ Memory-efficient grouping

### Quality
- ✅ 90%+ test coverage
- ✅ Comprehensive error messages
- ✅ Type safety throughout
- ✅ No security vulnerabilities

## Risks & Mitigation

### Technical Risks

**Risk**: Complexity explosion
**Mitigation**: Phased rollout, comprehensive tests, clear architecture

**Risk**: Performance degradation
**Mitigation**: Benchmarking, profiling, optimization passes

**Risk**: Memory exhaustion
**Mitigation**: Resource limits, streaming where possible

### Product Risks

**Risk**: Breaking changes alienate users
**Mitigation**: Backward compatibility, migration guide, versioning

**Risk**: Scope creep (trying to be full DB)
**Mitigation**: Stick to read-only analytics, no CRUD

**Risk**: DuckDB already exists
**Mitigation**: Focus on CLI simplicity, single binary, no setup

## Alternative Approaches

### Option A: Embed DuckDB
**Pros**: Full SQL, battle-tested, high performance
**Cons**: CGo dependency, binary size, less control

### Option B: Use SQL Parser Library
**Pros**: Faster implementation, standard syntax
**Cons**: Still need executor, less control over features

### Option C: Keep It Simple (Current)
**Pros**: Simple, fast, no complexity
**Cons**: Limited functionality, doesn't meet user needs

**Decision**: Implement custom (Option D) - Balance of control, features, simplicity

## Timeline & Resources

### Phase-by-Phase Timeline
- **Phase 1**: Weeks 1-3 (Column projection)
- **Phase 2**: Weeks 4-6 (Aggregations)
- **Phase 3**: Weeks 7-8 (Sorting)
- **Phase 4**: Weeks 9-10 (Operators)
- **Phase 5**: Weeks 11-12 (Functions)
- **Phase 6-8**: Weeks 13-22 (Advanced features - optional)

### Resource Requirements
- **1 Senior Engineer**: Architecture, parser, executor
- **1 Mid-level Engineer**: Functions, testing
- **1 Technical Writer**: Documentation

### Milestones
- **M1** (Week 3): Column projection working
- **M2** (Week 6): Aggregations working
- **M3** (Week 8): Sorting working
- **M4** (Week 10): v2.0 Beta release
- **M5** (Week 12): v2.0 GA release
- **M6** (Week 22): v3.0 GA release (stretch)

## Conclusion

This plan transforms parcat from a simple WHERE-filter tool into a powerful analytical query engine comparable to DuckDB's core features. The phased approach allows incremental delivery of value while managing complexity and risk.

**Key Success Factors**:
1. ✅ Phased rollout prevents overwhelming complexity
2. ✅ Backward compatibility maintains user trust
3. ✅ Comprehensive testing ensures quality
4. ✅ Clear scope prevents feature creep
5. ✅ Focus on analytics (not CRUD) keeps tool simple

**Next Steps**:
1. Review and approve plan
2. Set up project tracking
3. Begin Phase 1 implementation
4. Create technical design docs for each phase
5. Establish testing infrastructure

---

**Document Version**: 1.0
**Last Updated**: 2026-02-11
**Authors**: Claude (with parcat team)
**Status**: Proposed
