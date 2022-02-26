package org.ktorm.r2dbc.database

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.r2dbc.BaseTest
import org.ktorm.r2dbc.dsl.eq
import org.ktorm.r2dbc.dsl.from
import org.ktorm.r2dbc.dsl.joinReferencesAndSelect
import org.ktorm.r2dbc.entity.*
import org.ktorm.r2dbc.schema.*
import java.time.LocalDate

/**
 * Created by vince at Apr 07, 2020.
 */
class CompoundKeysTest : BaseTest() {

    interface Staff : Entity<Staff> {
        var id: Int
        var departmentId: Int
        var name: String
        var job: String
        var managerId: Int
        var hireDate: LocalDate
        var salary: Long
    }

    object Staffs : Table<Staff>("t_employee") {
        val id = int("id").primaryKey().bindTo { it.id }
        val departmentId = int("department_id").primaryKey().bindTo { it.departmentId }
        val name = varchar("name").bindTo { it.name }
        val job = varchar("job").bindTo { it.job }
        val managerId = int("manager_id").bindTo { it.managerId }
        val hireDate = date("hire_date").bindTo { it.hireDate }
        val salary = long("salary").bindTo { it.salary }
    }

    interface StaffRef : Entity<StaffRef> {
        val id: Int
        val staff: Staff
    }

    object StaffRefs : Table<StaffRef>("t_staff_ref") {
        val id = int("id").primaryKey().bindTo { it.id }
        val staffId = int("staff_id").references(Staffs) { it.staff }
    }

    val Database.staffs get() = this.sequenceOf(Staffs)

    val Database.staffRefs get() = this.sequenceOf(StaffRefs)

    @Test
    fun testAdd() = runBlocking {
        val staff = Entity.create<Staff>()
        staff.departmentId = 1
        staff.name = "jerry"
        staff.job = "engineer"
        staff.managerId = 1
        staff.hireDate = LocalDate.now()
        staff.salary = 100
        database.staffs.add(staff)
        println(staff)
        assert(staff.id == 0)
    }

    @Test
    fun testFlushChanges() = runBlocking {
        var staff = database.staffs.find { it.id eq 2 } ?: throw AssertionError()
        staff.job = "engineer"
        staff.salary = 100
        staff.flushChanges()
        staff.flushChanges()

        staff = database.staffs.find { it.id eq 2 } ?: throw AssertionError()
        assert(staff.job == "engineer")
        assert(staff.salary == 100L)
    }

    @Test
    fun testDeleteEntity() = runBlocking {
        val staff = database.staffs.find { it.id eq 2 } ?: throw AssertionError()
        staff.delete()

        assert(database.staffs.count() == 3)
    }

    @Test
    fun testUpdatePrimaryKey() = runBlocking {
        try {
            val staff = database.staffs.find { it.id eq 1 } ?: return@runBlocking
            staff.departmentId = 2
            throw AssertionError()

        } catch (e: UnsupportedOperationException) {
            // expected
            println(e.message)
        }
    }

    @Test
    fun testReferenceTableWithCompoundKeys() = runBlocking {
        try {
            database.from(StaffRefs).joinReferencesAndSelect()
            throw AssertionError("unexpected")

        } catch (e: ExceptionInInitializerError) {
            val ex = e.cause as IllegalStateException
            println(ex.message)
        }
    }
}