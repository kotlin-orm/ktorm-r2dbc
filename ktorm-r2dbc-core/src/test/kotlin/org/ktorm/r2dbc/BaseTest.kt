package org.ktorm.r2dbc

import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.ktorm.r2dbc.database.Database
import org.ktorm.r2dbc.database.SqlDialect
import org.ktorm.r2dbc.entity.Entity
import org.ktorm.r2dbc.entity.sequenceOf
import org.ktorm.r2dbc.expression.ArgumentExpression
import org.ktorm.r2dbc.expression.QueryExpression
import org.ktorm.r2dbc.expression.SqlFormatter
import org.ktorm.r2dbc.logging.ConsoleLogger
import org.ktorm.r2dbc.logging.LogLevel
import org.ktorm.r2dbc.schema.*
import java.io.Serializable
import java.time.LocalDate

/**
 * Created by vince on Dec 07, 2018.
 */
open class BaseTest {
    lateinit var database: Database

    @Before
    open fun init() {
        runBlocking {
            database = Database.connect(
                url = "r2dbc:h2:mem:///testdb?DB_CLOSE_DELAY=-1",
                logger = ConsoleLogger(threshold = LogLevel.TRACE),
                dialect =  getH2Dialect(),
                alwaysQuoteIdentifiers = true
            )

            execSqlScript("init-data.sql")
        }
    }

    fun getH2Dialect() = object: SqlDialect {
        override val identifierQuoteString: String = "\""
        override val extraNameCharacters: String = ""
        override val supportsMixedCaseIdentifiers: Boolean = false
        override val storesMixedCaseIdentifiers: Boolean = false
        override val storesUpperCaseIdentifiers: Boolean = true
        override val storesLowerCaseIdentifiers: Boolean = false
        override val supportsMixedCaseQuotedIdentifiers: Boolean = true
        override val storesMixedCaseQuotedIdentifiers: Boolean = true
        override val storesUpperCaseQuotedIdentifiers: Boolean = false
        override val storesLowerCaseQuotedIdentifiers: Boolean = false
        override val sqlKeywords: Set<String> = emptySet()
        override val maxColumnNameLength: Int = 0
        override fun createSqlFormatter(database: Database, beautifySql: Boolean, indentSize: Int): SqlFormatter {
            return object:SqlFormatter(database, beautifySql, indentSize) {
                override fun writePagination(expr: QueryExpression) {
                    newLine(Indentation.SAME)
                    if (expr.limit != null) {
                        writeKeyword("limit ? ")
                        _parameters += ArgumentExpression(expr.limit, IntSqlType)
                    }
                    if (expr.offset != null) {
                        writeKeyword("offset ? ")
                        _parameters += ArgumentExpression(expr.offset, IntSqlType)
                    }
                }

            }
        }
    }

    @After
    open fun destroy() {
        runBlocking {
            execSqlScript("drop-data.sql")
        }
    }

    protected suspend fun execSqlScript(filename: String) {
        database.useConnection { conn ->
            javaClass.classLoader
                ?.getResourceAsStream(filename)
                ?.bufferedReader()
                ?.use { reader ->
                    for (sql in reader.readText().split(";")) {
                        if (sql.any { it.isLetterOrDigit() }) {
                            val statement = conn.createStatement(sql)
                            statement.execute().awaitFirstOrNull()
                        }
                    }
                }
        }
    }

    data class LocationWrapper(val underlying: String = "") : Serializable

    interface Department : Entity<Department> {
        companion object : Entity.Factory<Department>()

        val id: Int
        var name: String
        var location: LocationWrapper
        var mixedCase: String?
    }

    interface Employee : Entity<Employee> {
        companion object : Entity.Factory<Employee>()

        var id: Int
        var name: String
        var job: String
        var manager: Employee?
        var hireDate: LocalDate
        var salary: Long
        var department: Department

        val upperName get() = name.uppercase()
        fun upperName() = name.uppercase()
    }

    interface Customer : Entity<Customer> {
        companion object : Entity.Factory<Customer>()

        var id: Int
        var name: String
        var email: String
        var phoneNumber: String
    }

    open class Departments(alias: String?) : Table<Department>("t_department", alias) {
        companion object : Departments(null)

        override fun aliased(alias: String) = Departments(alias)

        val id = int("id").primaryKey().bindTo { it.id }
        val name = varchar("name").bindTo { it.name }
        val location = varchar("location").transform({ LocationWrapper(it) }, { it.underlying }).bindTo { it.location }
        val mixedCase = varchar("mixedCase").bindTo { it.mixedCase }
    }

    open class Employees(alias: String?) : Table<Employee>("t_employee", alias) {
        companion object : Employees(null)

        override fun aliased(alias: String) = Employees(alias)

        val id = int("id").primaryKey().bindTo { it.id }
        val name = varchar("name").bindTo { it.name }
        val job = varchar("job").bindTo { it.job }
        val managerId = int("manager_id").bindTo { it.manager?.id }
        val hireDate = date("hire_date").bindTo { it.hireDate }
        val salary = long("salary").bindTo { it.salary }
        val departmentId = int("department_id").references(Departments) { it.department }
        val department = departmentId.referenceTable as Departments
    }

    open class Customers(alias: String?) : Table<Customer>("t_customer", alias, schema = "company") {
        companion object : Customers(null)

        override fun aliased(alias: String) = Customers(alias)

        val id = int("id").primaryKey().bindTo { it.id }
        val name = varchar("name").bindTo { it.name }
        val email = varchar("email").bindTo { it.email }
        val phoneNumber = varchar("phone_number").bindTo { it.phoneNumber }
    }

    val Database.departments get() = this.sequenceOf(Departments)

    val Database.employees get() = this.sequenceOf(Employees)

    val Database.customers get() = this.sequenceOf(Customers)
}
