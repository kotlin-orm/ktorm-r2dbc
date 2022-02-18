package org.ktorm.database

import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.BaseTest
import org.ktorm.r2dbc.database.toList
import org.ktorm.r2dbc.dsl.insert
import org.ktorm.r2dbc.entity.count
import org.ktorm.r2dbc.entity.forEach
import org.ktorm.r2dbc.entity.sequenceOf

/**
 * Created by vince on Dec 02, 2018.
 */
@ExperimentalUnsignedTypes
class DatabaseTest : BaseTest() {


    /*@Test
    fun testKeywordWrapping(): Unit = runBlocking {
        val configs = object : Table<Nothing>("t_config") {
            val key = varchar("key").primaryKey()
            val value = varchar("value")
        }

        database.useConnection {
            val sql =  """CREATE TABLE T_CONFIG(KEY VARCHAR(128) PRIMARY KEY, VALUE VARCHAR(128))"""
            it.createStatement(sql).execute().awaitFirstOrNull()
        }

        database.insert(configs) {
            set(it.key, "test")
            set(it.value, "test value")
        }

        assert(database.sequenceOf(configs).count { it.key eq "test" } == 1)

        database.delete(configs) { it.key eq "test" }
    }*/

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
                SELECT "NAME" FROM "T_EMPLOYEE"
                WHERE "DEPARTMENT_ID" = ?
                ORDER BY "ID"
            """

            val statement = conn.createStatement(sql)
            statement.bind(0, 1)
            statement.execute().awaitFirstOrNull()?.map { row, _ ->
                row.get(0)
            }?.toList() ?: emptyList()
        }

        assert(names.size == 2)
        assert(names[0] == "VINCE")
        assert(names[1] == "MARRY")
    }

    @Test
    fun tableTest() = runBlocking {
        database.useTransaction {
            database.useTransaction {
                for (employee in database.employees) {
                    println(it)
                }
                throw RuntimeException()
            }
        }
        assert(true)
    }

    /*fun BaseTable<*>.ulong(name: String): Column<ULong> {
        return registerColumn(name, object : SqlType<ULong>(Types.BIGINT, "bigint unsigned") {
            override fun doSetParameter(ps: PreparedStatement, index: Int, parameter: ULong) {
                ps.setLong(index, parameter.toLong())
            }

            override fun doGetResult(rs: ResultSet, index: Int): ULong? {
                return rs.getLong(index).toULong()
            }
        })
    }

    interface TestUnsigned : Entity<TestUnsigned> {
        companion object : Entity.Factory<TestUnsigned>()
        var id: ULong
    }

    @Test
    fun testUnsigned() {
        val t = object : Table<TestUnsigned>("T_TEST_UNSIGNED") {
            val id = ulong("ID").primaryKey().bindTo { it.id }
        }

        database.useConnection { conn ->
            conn.createStatement().use { statement ->
                val sql = """CREATE TABLE T_TEST_UNSIGNED(ID BIGINT UNSIGNED NOT NULL PRIMARY KEY)"""
                statement.executeUpdate(sql)
            }
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
    fun testUnsignedNullable() {
        val t = object : Table<TestUnsignedNullable>("T_TEST_UNSIGNED_NULLABLE") {
            val id = ulong("ID").primaryKey().bindTo { it.id }
        }

        database.useConnection { conn ->
            conn.createStatement().use { statement ->
                val sql = """CREATE TABLE T_TEST_UNSIGNED_NULLABLE(ID BIGINT UNSIGNED NOT NULL PRIMARY KEY)"""
                statement.executeUpdate(sql)
            }
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
    }*/
}
