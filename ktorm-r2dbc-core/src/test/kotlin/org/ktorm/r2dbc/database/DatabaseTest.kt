package org.ktorm.r2dbc.database

import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.r2dbc.BaseTest
import org.ktorm.r2dbc.dsl.*
import org.ktorm.r2dbc.entity.*
import org.ktorm.r2dbc.schema.*
import java.lang.IllegalStateException

/**
 * Created by vince on Dec 02, 2018.
 */

@ExperimentalUnsignedTypes
class DatabaseTest : BaseTest() {

    @Test
    fun testMetadata() {
        with(database) {
            println(productName)
            println(productVersion)
            println(keywords.toString())
            println(identifierQuoteString)
            println(extraNameCharacters)
        }
    }

    @Test
    fun testKeywordWrapping() = runBlocking {
        val configs = object : Table<Nothing>("T_CONFIG") {
            val key = varchar("KEY").primaryKey()
            val value = varchar("VALUE")
        }

        database.useConnection { conn ->
            val sql = """CREATE TABLE T_CONFIG("KEY" VARCHAR(128) PRIMARY KEY, "VALUE" VARCHAR(128))"""
            conn.createStatement(sql).execute().awaitFirst()
        }

        database.insert(configs) {
            set(it.key, "test")
            set(it.value, "test value")
        }

        assert(database.sequenceOf(configs).count { it.key eq "test" } == 1)

        database.delete(configs) { it.key eq "test" }
        Unit
    }

    @Test
    fun testTransaction() = runBlocking {
        class DummyException : Exception()

        try {
            database.useTransaction {
                database.insert(Departments) {
                    set(it.name, "administration")
                    set(it.location, LocationWrapper("Hong Kong"))
                }

                assert(database.departments.count() == 3)

                throw DummyException()
            }

        } catch (e: DummyException) {
            assert(database.departments.count() == 2)
        }
    }

    @Test
    fun testRawSql() = runBlocking {
        val names = database.useConnection { conn ->
            val sql = """
                select "name" from "t_employee"
                where "department_id" = ?
                order by "id"
            """

            val statement = conn.createStatement(sql)
            statement.bind(0, 1)
            statement.execute().awaitFirst().map { row, _ -> row[0, String::class.java] }.toList()
        }

        assert(names.size == 2)
        assert(names[0] == "vince")
        assert(names[1] == "marry")
    }

    fun BaseTable<*>.ulong(name: String): Column<ULong> {
        return registerColumn(name, LongSqlType.transform({ it.toULong() }, { it.toLong() }))
    }

    interface TestUnsigned : Entity<TestUnsigned> {
        companion object : Entity.Factory<TestUnsigned>()

        var id: ULong
    }

    @Test
    fun testUnsigned() = runBlocking {
        val t = object : Table<TestUnsigned>("T_TEST_UNSIGNED") {
            val id = ulong("ID").primaryKey().bindTo { it.id }
        }

        database.useConnection { conn ->
            val sql = """CREATE TABLE T_TEST_UNSIGNED(ID BIGINT NOT NULL PRIMARY KEY)"""
            val statement = conn.createStatement(sql)
            statement.execute().awaitFirst()
        }

        val unsigned = TestUnsigned { id = 5UL }
        assert(unsigned.id == 5UL)
        database.sequenceOf(t).add(unsigned)

        val ids = database.sequenceOf(t).toList().map { it.id }
        println(ids)
        assert(ids == listOf(5UL))

        database.insert(t) {
            set(it.id, 6UL)
        }

        val ids2 = database.from(t).select(t.id).map { row -> row[t.id] }
        println(ids2)
        assert(ids2 == listOf(5UL, 6UL))

        assert(TestUnsigned().id == 0UL)
    }

    interface TestUnsignedNullable : Entity<TestUnsignedNullable> {
        companion object : Entity.Factory<TestUnsignedNullable>()

        var id: ULong?
    }

    @Test
    fun testUnsignedNullable() = runBlocking {
        val t = object : Table<TestUnsignedNullable>("T_TEST_UNSIGNED_NULLABLE") {
            val id = ulong("ID").primaryKey().bindTo { it.id }
        }

        database.useConnection { conn ->
            val sql = """CREATE TABLE T_TEST_UNSIGNED_NULLABLE(ID BIGINT NOT NULL PRIMARY KEY)"""
            val statement = conn.createStatement(sql)
            statement.execute().awaitFirst()
        }

        val unsigned = TestUnsignedNullable { id = 5UL }
        assert(unsigned.id == 5UL)
        database.sequenceOf(t).add(unsigned)

        val ids = database.sequenceOf(t).toList().map { it.id }
        println(ids)
        assert(ids == listOf(5UL))

        assert(TestUnsignedNullable().id == null)
    }

    @Test
    fun testDefaultValueReferenceEquality() {
        assert(Boolean::class.javaPrimitiveType!!.defaultValue === Boolean::class.javaPrimitiveType!!.defaultValue)
        assert(Char::class.javaPrimitiveType!!.defaultValue === Char::class.javaPrimitiveType!!.defaultValue)
        assert(Byte::class.javaPrimitiveType!!.defaultValue === Byte::class.javaPrimitiveType!!.defaultValue)
        assert(Short::class.javaPrimitiveType!!.defaultValue === Short::class.javaPrimitiveType!!.defaultValue)
        assert(Int::class.javaPrimitiveType!!.defaultValue === Int::class.javaPrimitiveType!!.defaultValue)
        assert(Long::class.javaPrimitiveType!!.defaultValue === Long::class.javaPrimitiveType!!.defaultValue)
        assert(Float::class.javaPrimitiveType!!.defaultValue !== Float::class.javaPrimitiveType!!.defaultValue)
        assert(Double::class.javaPrimitiveType!!.defaultValue !== Double::class.javaPrimitiveType!!.defaultValue)
        assert(String::class.java.defaultValue === String::class.java.defaultValue)
        assert(UByte::class.java.defaultValue !== UByte::class.java.defaultValue)
        assert(UShort::class.java.defaultValue !== UShort::class.java.defaultValue)
        assert(UInt::class.java.defaultValue !== UInt::class.java.defaultValue)
        assert(ULong::class.java.defaultValue !== ULong::class.java.defaultValue)
        assert(UByteArray::class.java.defaultValue !== UByteArray::class.java.defaultValue)
        assert(UShortArray::class.java.defaultValue !== UShortArray::class.java.defaultValue)
        assert(UIntArray::class.java.defaultValue !== UIntArray::class.java.defaultValue)
        assert(ULongArray::class.java.defaultValue !== ULongArray::class.java.defaultValue)
    }
}
