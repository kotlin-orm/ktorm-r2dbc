package org.ktorm.r2dbc.dsl

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.r2dbc.BaseTest
import org.ktorm.r2dbc.entity.*

/**
 * Created by vince on Dec 09, 2018.
 */
class AggregationTest : BaseTest() {

    @Test
    fun testCount() = runBlocking {
        val count = database.employees.count { it.departmentId eq 1 }
        assert(count == 2)
    }

    @Test
    fun testCountAll() = runBlocking {
        val count = database.employees.count()
        assert(count == 4)
    }

    @Test
    fun testSum() = runBlocking {
        val sum = database.employees.sumBy { it.salary + 1 }
        assert(sum == 454L)
    }

    @Test
    fun testMax() = runBlocking {
        val max = database.employees.maxBy { it.salary - 1 }
        assert(max == 199L)
    }

    @Test
    fun testMin() = runBlocking {
        val min = database.employees.minBy { it.salary }
        assert(min == 50L)
    }

    @Test
    fun testAvg() = runBlocking {
        val avg = database.employees.averageBy { it.salary }
        println(avg)
    }

    @Test
    fun testNone() = runBlocking {
        assert(database.employees.none { it.salary greater 200L })
    }

    @Test
    fun testAny() = runBlocking {
        assert(!database.employees.any { it.salary greater 200L })
    }

    @Test
    fun testAll() = runBlocking {
        assert(database.employees.all { it.salary greater 0L })
    }

    @Test
    fun testAggregate() = runBlocking {
        val result = database.employees.aggregateColumns { max(it.salary) - min(it.salary) }
        println(result)
        assert(result == 150L)
    }

    @Test
    fun testAggregate2() = runBlocking {
        val (max, min) = database.employees.aggregateColumns { tupleOf(max(it.salary), min(it.salary)) }
        assert(max == 200L)
        assert(min == 50L)
    }

    @Test
    fun testGroupAggregate3() = runBlocking {
        database.employees
            .groupingBy { it.departmentId }
            .aggregateColumns { tupleOf(max(it.salary), min(it.salary)) }
            .forEach { departmentId, (max, min) ->
                println("$departmentId:$max:$min")
            }
    }
}