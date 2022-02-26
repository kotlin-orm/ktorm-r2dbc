package org.ktorm.r2dbc.dsl

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.r2dbc.BaseTest
import org.ktorm.r2dbc.expression.ScalarExpression
import org.ktorm.r2dbc.schema.DoubleSqlType
import org.ktorm.r2dbc.schema.IntSqlType
import org.ktorm.r2dbc.schema.LongSqlType
import org.ktorm.r2dbc.schema.VarcharSqlType

/**
 * Created by vince on Dec 07, 2018.
 */
class QueryTest : BaseTest() {

    @Test
    fun testSelect() = runBlocking {
        val query = database.from(Departments).select()
        val rows = query.asFlow().toList()
        assert(rows.size == 2)

        for (row in rows) {
            println(row[Departments.name] + ": " + row[Departments.location])
        }
    }

    @Test
    fun testSelectDistinct() = runBlocking {
        val ids = database
            .from(Employees)
            .selectDistinct(Employees.departmentId)
            .map {
                it[Employees.id]
                IntSqlType.getResult(it,0)!!
            }
            .sortedDescending()

        assert(ids.size == 2)
        assert(ids[0] == 2)
        assert(ids[1] == 1)
    }

    @Test
    fun testWhere() = runBlocking {
        val name = database
            .from(Employees)
            .select(Employees.name)
            .where { Employees.managerId.isNull() and (Employees.departmentId eq 1) }
            .map { VarcharSqlType.getResult(it,0) }
            .first()

        assert(name == "vince")
    }

    @Test
    fun testWhereWithConditions() = runBlocking {
        val t = Employees.aliased("t")

        val name = database
            .from(t)
            .select(t.name)
            .whereWithConditions {
                it += t.managerId.isNull()
                it += t.departmentId eq 1
            }
            .map { VarcharSqlType.getResult(it,0) }
            .first()

        assert(name == "vince")
    }

    @Test
    fun testCombineConditions() = runBlocking {
        val t = Employees.aliased("t")

        val names = database
            .from(t)
            .select(t.name)
            .where { emptyList<ScalarExpression<Boolean>>().combineConditions() }
            .orderBy(t.id.asc())
            .map { it.get(0, String::class.java) }

        assert(names.size == 4)
        assert(names[0] == "vince")
        assert(names[1] == "marry")
    }

    @Test
    fun testOrderBy() = runBlocking {
        val names = database
            .from(Employees)
            .select(Employees.name)
            .where { Employees.departmentId eq 1 }
            .orderBy(Employees.salary.desc())
            .map { VarcharSqlType.getResult(it,0) }

        assert(names.size == 2)
        assert(names[0] == "vince")
        assert(names[1] == "marry")
    }

    @Test
    fun testAggregation() = runBlocking {
        val t = Employees

        val salaries = database
            .from(t)
            .select(t.departmentId, sum(t.salary))
            .groupBy(t.departmentId)
            .associate { IntSqlType.getResult(it,0) to LongSqlType.getResult(it,1) }

        assert(salaries.size == 2)
        assert(salaries[1]!! == 150L)
        assert(salaries[2]!! == 300L)
    }

    @Test
    fun testHaving() = runBlocking {
        val t = Employees

        val salaries = database
            .from(t)
            .select(t.departmentId, avg(t.salary))
            .groupBy(t.departmentId)
            .having(avg(t.salary).greater(100.0))
            .associate { IntSqlType.getResult(it,0) to DoubleSqlType.getResult(it,1) }

        println(salaries)
        assert(salaries.size == 1)
        assert(salaries.keys.first() == 2)
    }

    @Test
    fun testColumnAlias() = runBlocking {
        val deptId = Employees.departmentId.aliased("dept_id")
        val salaryAvg = avg(Employees.salary).aliased("salary_avg")

        val salaries = database
            .from(Employees)
            .select(deptId, salaryAvg)
            .groupBy(deptId)
            .having { salaryAvg greater 100.0 }
            .associate { row ->
                row[deptId] to row[salaryAvg]
            }

        println(salaries)
        assert(salaries.size == 1)
        assert(salaries.keys.first() == 2)
        assert(salaries.values.first() == 150.0)
    }

    @Test
    fun testColumnAlias1() = runBlocking {
        val salary = (Employees.salary + 100).aliased(null)

        val salaries = database
            .from(Employees)
            .select(salary)
            .where { salary greater 200L }
            .map { LongSqlType.getResult(it,0) }

        println(salaries)
        assert(salaries.size == 1)
        assert(salaries.first() == 300L)
    }

    @Test
    fun testLimit() = runBlocking {
        try {
            val query = database.from(Employees).select().orderBy(Employees.id.desc()).limit(0, 2)
            assert(query.totalRecords() == 4L)
            val records = query.asFlow().toList()
            val ids = records.map { it[Employees.id] }
            assert(ids[0] == 4)
            assert(ids[1] == 3)

        } catch (e: UnsupportedOperationException) {
            // Expected, pagination should be provided by dialects...
        }
    }

    @Test
    fun testBetween() = runBlocking {
        val names = database
            .from(Employees)
            .select(Employees.name)
            .where { Employees.salary between 100L..200L }
            .map { VarcharSqlType.getResult(it,0) }

        assert(names.size == 3)
        println(names)
    }

    @Test
    fun testInList() = runBlocking {
        val query = database
            .from(Employees)
            .select()
            .where { Employees.id.inList(1, 2, 3) }

        assert(query.totalRecords() == 3L)
    }

    @Test
    fun testInNestedQuery() = runBlocking {
        val departmentIds = database.from(Departments).selectDistinct(Departments.id)

        val query = database
            .from(Employees)
            .select()
            .where { Employees.departmentId inList departmentIds }

        assert(query.totalRecords() == 4L)

        println(query.sql)
    }

    @Test
    fun testExists() = runBlocking {
        val query = database
            .from(Employees)
            .select()
            .where {
                Employees.id.isNotNull() and exists(
                    database
                        .from(Departments)
                        .select()
                        .where { Departments.id eq Employees.departmentId }
                )
            }

        assert(query.totalRecords() == 4L)
        println(query.sql)
    }

    @Test
    fun testUnion() = runBlocking {
        val query = database
            .from(Employees)
            .select(Employees.id)
            .unionAll(
                database.from(Departments).select(Departments.id)
            )
            .unionAll(
                database.from(Departments).select(Departments.id)
            )
            .orderBy(Employees.id.desc())

        assert(query.totalRecords() == 8L)

        println(query.sql)
    }

    @Test
    fun testMod() = runBlocking {
        val query = database.from(Employees).select().where { Employees.id % 2 eq 1 }
        assert(query.totalRecords() == 2L)
        println(query.sql)
    }

    @Test
    fun testFlatMap()= runBlocking {
        val names = database
            .from(Employees)
            .select(Employees.name)
            .where { Employees.departmentId eq 1 }
            .orderBy(Employees.salary.desc())
            .flatMapIndexed { index, row -> listOf("$index:${VarcharSqlType.getResult(row,0)}") }

        assert(names.size == 2)
        assert(names[0] == "0:vince")
        assert(names[1] == "1:marry")
    }
}