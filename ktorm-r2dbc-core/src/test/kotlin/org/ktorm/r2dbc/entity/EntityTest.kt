package org.ktorm.r2dbc.entity

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.ktorm.r2dbc.BaseTest
import org.ktorm.r2dbc.database.Database
import org.ktorm.r2dbc.dsl.*
import org.ktorm.r2dbc.schema.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.time.LocalDate
import java.util.*
import kotlin.reflect.jvm.jvmErasure

/**
 * Created by vince on Dec 09, 2018.
 */
class EntityTest : BaseTest() {

    @Test
    fun testTypeReference() {
        println(Employee)
        println(Employee.referencedKotlinType)
        assert(Employee.referencedKotlinType.jvmErasure == Employee::class)

        println(Employees)
        println(Employees.entityClass)
        assert(Employees.entityClass == Employee::class)

        println(Employees.aliased("t"))
        println(Employees.aliased("t").entityClass)
        assert(Employees.aliased("t").entityClass == Employee::class)
    }

    @Test
    fun testEntityProperties() {
        val employee = Employee {
            name = "vince"
        }

        println(employee)

        assert(employee["name"] == "vince")
        assert(employee.name == "vince")
        assert(employee.upperName == "VINCE")
        assert(employee.upperName() == "VINCE")

        assert(employee["job"] == null)
        assert(employee.job == "")
    }

    @Test
    fun testSerialize() = runBlocking {
        val employee = Employee {
            name = "jerry"
            job = "trainee"
            manager = database.employees.find { it.name eq "vince" }
            hireDate = LocalDate.now()
            salary = 50
            department = database.departments.find { it.name eq "tech" } ?: throw AssertionError()
        }

        val bytes = serialize(employee)
        println(Base64.getEncoder().encodeToString(bytes))
    }

    @Test
    fun testDeserialize() = runBlocking {
        Department {
            name = "test"
            println(this.javaClass)
            println(this)
        }

        Employee {
            name = "test"
            println(this.javaClass)
            println(this)
        }

        val str =
            "rO0ABXN9AAAAAQAhb3JnLmt0b3JtLnIyZGJjLkJhc2VUZXN0JEVtcGxveWVleHIAF2phdmEubGFuZy5yZWZsZWN0LlByb3h54SfaIMwQQ8sCAAFMAAFodAAlTGphdmEvbGFuZy9yZWZsZWN0L0ludm9jYXRpb25IYW5kbGVyO3hwc3IAK29yZy5rdG9ybS5yMmRiYy5lbnRpdHkuRW50aXR5SW1wbGVtZW50YXRpb24AAAAAAAAAAQMABEwAC2RvRGVsZXRlRnVudAAaTGtvdGxpbi9yZWZsZWN0L0tGdW5jdGlvbjtMABBkb0ZsdXNoQ2hhbmdlRnVucQB+AAVMAAtlbnRpdHlDbGFzc3QAF0xrb3RsaW4vcmVmbGVjdC9LQ2xhc3M7TAAGdmFsdWVzdAAZTGphdmEvdXRpbC9MaW5rZWRIYXNoTWFwO3hwdyMAIW9yZy5rdG9ybS5yMmRiYy5CYXNlVGVzdCRFbXBsb3llZXNyABdqYXZhLnV0aWwuTGlua2VkSGFzaE1hcDTATlwQbMD7AgABWgALYWNjZXNzT3JkZXJ4cgARamF2YS51dGlsLkhhc2hNYXAFB9rBwxZg0QMAAkYACmxvYWRGYWN0b3JJAAl0aHJlc2hvbGR4cD9AAAAAAAAMdwgAAAAQAAAABnQABG5hbWV0AAVqZXJyeXQAA2pvYnQAB3RyYWluZWV0AAdtYW5hZ2Vyc3EAfgAAc3EAfgAEdyMAIW9yZy5rdG9ybS5yMmRiYy5CYXNlVGVzdCRFbXBsb3llZXNxAH4ACT9AAAAAAAAMdwgAAAAQAAAAB3QAAmlkc3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAFxAH4ADHQABXZpbmNlcQB+AA50AAhlbmdpbmVlcnEAfgAQc3EAfgAAc3EAfgAEdyMAIW9yZy5rdG9ybS5yMmRiYy5CYXNlVGVzdCRFbXBsb3llZXNxAH4ACT9AAAAAAAAMdwgAAAAQAAAAAXEAfgAUcHgAeHQACGhpcmVEYXRlc3IADWphdmEudGltZS5TZXKVXYS6GyJIsgwAAHhwdwcDAAAH4gEBeHQABnNhbGFyeXNyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cQB+ABYAAAAAAAAAZHQACmRlcGFydG1lbnRzfQAAAAEAI29yZy5rdG9ybS5yMmRiYy5CYXNlVGVzdCREZXBhcnRtZW50eHEAfgABc3EAfgAEdyUAI29yZy5rdG9ybS5yMmRiYy5CYXNlVGVzdCREZXBhcnRtZW50c3EAfgAJP0AAAAAAAAx3CAAAABAAAAAEdAACaWRxAH4AF3QABG5hbWV0AAR0ZWNodAAIbG9jYXRpb25zcgAob3JnLmt0b3JtLnIyZGJjLkJhc2VUZXN0JExvY2F0aW9uV3JhcHBlcuw3KJ5eUyi8AgABTAAKdW5kZXJseWluZ3QAEkxqYXZhL2xhbmcvU3RyaW5nO3hwdAAJR3Vhbmd6aG91dAAJbWl4ZWRDYXNlcHgAeHgAeHEAfgAdc3EAfgAedwcDAAAH5gISeHEAfgAgc3EAfgAhAAAAAAAAADJxAH4AI3NxAH4AJHNxAH4ABHclACNvcmcua3Rvcm0ucjJkYmMuQmFzZVRlc3QkRGVwYXJ0bWVudHNxAH4ACT9AAAAAAAAMdwgAAAAQAAAABHEAfgAocQB+ABdxAH4AKXEAfgAqcQB+ACtzcQB+ACxxAH4AL3EAfgAwcHgAeHgAeA=="
        val bytes = Base64.getDecoder().decode(str)

        val employee = deserialize(bytes) as Employee
        println(employee.javaClass)
        println(employee)

        assert(employee.name == "jerry")
        assert(employee.job == "trainee")
        assert(employee.manager?.name == "vince")
        assert(employee.salary == 50L)
        assert(employee.department.name == "tech")
    }

    private fun serialize(obj: Any): ByteArray {
        ByteArrayOutputStream().use { buffer ->
            ObjectOutputStream(buffer).use { output ->
                output.writeObject(obj)
                output.flush()
                return buffer.toByteArray()
            }
        }
    }

    private fun deserialize(bytes: ByteArray): Any {
        ByteArrayInputStream(bytes).use { buffer ->
            ObjectInputStream(buffer).use { input ->
                return input.readObject()
            }
        }
    }

    @Test
    fun testFind() = runBlocking {
        val employee = database.employees.find { it.id eq 1 } ?: throw AssertionError()
        println(employee)

        assert(employee.name == "vince")
        assert(employee.job == "engineer")
    }

    @Test
    fun testFindWithReference() = runBlocking {
        val employees = database.employees
            .filter { it.department.location like "%Guangzhou%" }
            .sortedBy { it.id }
            .toList()

        assert(employees.size == 2)
        assert(employees[0].name == "vince")
        assert(employees[1].name == "marry")
    }

    @Test
    fun testCreateEntity() = runBlocking {
        val employees = database
            .from(Employees)
            .joinReferencesAndSelect()
            .where {
                val dept = Employees.departmentId.referenceTable as Departments
                dept.location like "%Guangzhou%"
            }
            .orderBy(Employees.id.asc())
            .map { Employees.createEntity(it) }

        assert(employees.size == 2)
        assert(employees[0].name == "vince")
        assert(employees[1].name == "marry")
    }

    @Test
    fun testUpdate() = runBlocking {
        var employee = Employee()
        employee.id = 2
        employee.job = "engineer"
        employee.salary = 100
        // employee.manager = null
        database.employees.update(employee)

        employee = database.employees.find { it.id eq 2 } ?: throw AssertionError()
        assert(employee.job == "engineer")
        assert(employee.salary == 100L)
        assert(employee.manager?.id == 1)
    }

    @Test
    fun testFlushChanges() = runBlocking {
        var employee = database.employees.find { it.id eq 2 } ?: throw AssertionError()
        employee.job = "engineer"
        employee.salary = 100
        employee.manager = null
        employee.flushChanges()
        employee.flushChanges()

        employee = database.employees.find { it.id eq 2 } ?: throw AssertionError()
        assert(employee.job == "engineer")
        assert(employee.salary == 100L)
        assert(employee.manager == null)
    }

    @Test
    fun testDeleteEntity() = runBlocking {
        val employee = database.employees.find { it.id eq 2 } ?: throw AssertionError()
        employee.delete()

        assert(database.employees.count() == 3)
    }

    @Test
    fun testSaveEntity() = runBlocking {
        val employees = database.employees.toList()
        println(employees)
        var employee = Employee {
            name = "jerry"
            job = "trainee"
            manager = null
            hireDate = LocalDate.now()
            salary = 50
            department = database.departments.find { it.name eq "tech" } ?: throw AssertionError()
        }

        database.employees.add(employee)
        println(employee)

        employee = database.employees.find { it.id eq 5 } ?: throw AssertionError()
        assert(employee.name == "jerry")
        assert(employee.department.name == "tech")

        employee.job = "engineer"
        employee.salary = 100
        employee.flushChanges()

        employee = database.employees.find { it.id eq 5 } ?: throw AssertionError()
        assert(employee.job == "engineer")
        assert(employee.salary == 100L)

        employee.delete()
        assert(database.employees.count() == 4)
    }

    @Test
    fun testFindMapById() = runBlocking {
        val employees = database.employees.filter { it.id.inList(1, 2) }.associateBy { it.id }
        assert(employees.size == 2)
        assert(employees[1]?.name == "vince")
        assert(employees[2]?.name == "marry")
    }

    interface Parent : Entity<Parent> {
        companion object : Entity.Factory<Parent>()

        var child: Child?
    }

    interface Child : Entity<Child> {
        companion object : Entity.Factory<Child>()

        var grandChild: GrandChild?
    }

    interface GrandChild : Entity<GrandChild> {
        companion object : Entity.Factory<GrandChild>()

        var id: Int?
    }

    object Parents : Table<Parent>("t_employee") {
        val id = int("id").primaryKey().bindTo { it.child?.grandChild?.id }
    }

    @Test
    fun testHasColumnValue() {
        val p1 = Parent()
        assert(!p1.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p1.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p2 = Parent {
            child = null
        }
        assert(p2.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p2.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p3 = Parent {
            child = Child()
        }
        assert(!p3.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p3.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p4 = Parent {
            child = Child {
                grandChild = null
            }
        }
        assert(p4.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p4.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p5 = Parent {
            child = Child {
                grandChild = GrandChild()
            }
        }
        assert(!p5.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p5.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p6 = Parent {
            child = Child {
                grandChild = GrandChild {
                    id = null
                }
            }
        }
        assert(p6.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p6.implementation.getColumnValue(Parents.id.binding!!) == null)

        val p7 = Parent {
            child = Child {
                grandChild = GrandChild {
                    id = 6
                }
            }
        }
        assert(p7.implementation.hasColumnValue(Parents.id.binding!!))
        assert(p7.implementation.getColumnValue(Parents.id.binding!!) == 6)
    }

    @Test
    fun testUpdatePrimaryKey() = runBlocking {
        try {
            val parent = database.sequenceOf(Parents).find { it.id eq 1 } ?: throw AssertionError()
            assert(parent.child?.grandChild?.id == 1)

            parent.child?.grandChild?.id = 2
            throw AssertionError()

        } catch (e: UnsupportedOperationException) {
            // expected
            println(e.message)
        }
    }

    interface EmployeeTestForReferencePrimaryKey : Entity<EmployeeTestForReferencePrimaryKey> {
        var employee: Employee
        var manager: EmployeeManagerTestForReferencePrimaryKey
    }

    interface EmployeeManagerTestForReferencePrimaryKey : Entity<EmployeeManagerTestForReferencePrimaryKey> {
        var employee: Employee
    }

    object EmployeeTestForReferencePrimaryKeys : Table<EmployeeTestForReferencePrimaryKey>("t_employee0") {
        val id = int("id").primaryKey().references(Employees) { it.employee }
        val managerId = int("manager_id").bindTo { it.manager.employee.id }
    }

    @Test
    fun testUpdateReferencesPrimaryKey() = runBlocking {
        val e = database.sequenceOf(EmployeeTestForReferencePrimaryKeys).find { it.id eq 2 } ?: return@runBlocking
        e.manager.employee = database.sequenceOf(Employees).find { it.id eq 1 } ?: return@runBlocking

        try {
            e.employee = database.sequenceOf(Employees).find { it.id eq 1 } ?: return@runBlocking
            throw AssertionError()
        } catch (e: UnsupportedOperationException) {
            // expected
            println(e.message)
        }

        e.flushChanges()
    }

    @Test
    fun testForeignKeyValue() = runBlocking {
        val employees = database
            .from(Employees)
            .select()
            .orderBy(Employees.id.asc())
            .map { Employees.createEntity(it) }

        val vince = employees[0]
        assert(vince.manager == null)
        assert(vince.department.id == 1)

        val marry = employees[1]
        assert(marry.manager?.id == 1)
        assert(marry.department.id == 1)

        val tom = employees[2]
        assert(tom.manager == null)
        assert(tom.department.id == 2)

        val penny = employees[3]
        assert(penny.manager?.id == 3)
        assert(penny.department.id == 2)
    }

    @Test
    fun testCreateEntityWithoutReferences() = runBlocking {
        val employees = database
            .from(Employees)
            .leftJoin(Departments, on = Employees.departmentId eq Departments.id)
            .select(Employees.columns + Departments.columns)
            .map { Employees.createEntity(it, withReferences = false) }

        employees.forEach { println(it) }

        assert(employees.size == 4)
        assert(employees[0].department.id == 1)
        assert(employees[1].department.id == 1)
        assert(employees[2].department.id == 2)
        assert(employees[3].department.id == 2)
    }

    @Test
    fun testAutoDiscardChanges() = runBlocking {
        var department = database.departments.find { it.id eq 2 } ?: return@runBlocking
        department.name = "tech"

        val employee = Employee()
        employee.department = department
        employee.name = "jerry"
        employee.job = "trainee"
        employee.manager = database.employees.find { it.name eq "vince" }
        employee.hireDate = LocalDate.now()
        employee.salary = 50
        database.employees.add(employee)

        department.location = LocationWrapper("Guangzhou")
        department.flushChanges()

        department = database.departments.find { it.id eq 2 } ?: return@runBlocking
        assert(department.name == "tech")
        assert(department.location.underlying == "Guangzhou")
    }

    interface Emp : Entity<Emp> {
        companion object : Entity.Factory<Emp>()

        val id: Int
        var employee: Employee
        var manager: Employee
        var hireDate: LocalDate
        var salary: Long
        var departmentId: Int
    }

    object Emps : Table<Emp>("t_employee") {
        val id = int("id").primaryKey().bindTo { it.id }
        val name = varchar("name").bindTo { it.employee.name }
        val job = varchar("job").bindTo { it.employee.job }
        val managerId = int("manager_id").bindTo { it.manager.id }
        val hireDate = date("hire_date").bindTo { it.hireDate }
        val salary = long("salary").bindTo { it.salary }
        val departmentId = int("department_id").bindTo { it.departmentId }
    }

    val Database.emps get() = this.sequenceOf(Emps)

    @Test
    fun testCheckUnexpectedFlush() = runBlocking {
        val emp1 = database.emps.find { it.id eq 1 } ?: return@runBlocking
        emp1.employee.name = "jerry"
        // emp1.flushChanges()

        val emp2 = Emp {
            employee = emp1.employee
            hireDate = LocalDate.now()
            salary = 100
            departmentId = 1
        }

        try {
            database.emps.add(emp2)
            throw AssertionError("failed")

        } catch (e: IllegalStateException) {
            assert(e.message == "this.employee.name may be unexpectedly discarded, please save it to database first.")
        }
    }

    @Test
    fun testCheckUnexpectedFlush0() = runBlocking {
        val emp1 = database.emps.find { it.id eq 1 } ?: return@runBlocking
        emp1.employee.name = "jerry"
        // emp1.flushChanges()

        val emp2 = database.emps.find { it.id eq 2 } ?: return@runBlocking
        emp2.employee = emp1.employee

        try {
            emp2.flushChanges()
            throw AssertionError("failed")

        } catch (e: IllegalStateException) {
            assert(e.message == "this.employee.name may be unexpectedly discarded, please save it to database first.")
        }
    }

    @Test
    fun testCheckUnexpectedFlush1() = runBlocking {
        val employee = database.employees.find { it.id eq 1 } ?: return@runBlocking
        employee.name = "jerry"
        // employee.flushChanges()

        val emp = database.emps.find { it.id eq 2 } ?: return@runBlocking
        emp.employee = employee

        try {
            emp.flushChanges()
            throw AssertionError("failed")

        } catch (e: IllegalStateException) {
            assert(e.message == "this.employee.name may be unexpectedly discarded, please save it to database first.")
        }
    }

    @Test
    fun testFlushChangesForDefaultValues() = runBlocking {
        var emp = database.emps.find { it.id eq 1 } ?: return@runBlocking
        emp.manager.id = 2
        emp.flushChanges()

        emp = database.emps.find { it.id eq 1 } ?: return@runBlocking
        assert(emp.manager.id == 2)
    }

    @Test
    fun testDefaultValuesCache() = runBlocking {
        val department = Department()
        assert(department.id == 0)
        assert(department["id"] == null)
    }

    @Test
    fun testCopyStatus() = runBlocking {
        var employee = database.employees.find { it.id eq 2 }?.copy() ?: return@runBlocking
        employee.name = "jerry"
        employee.manager?.id = 3
        employee.flushChanges()

        employee = database.employees.find { it.id eq 2 } ?: return@runBlocking
        assert(employee.name == "jerry")
        assert(employee.manager?.id == 3)
    }

    @Test
    fun testDeepCopy() = runBlocking {
        val employee = database.employees.find { it.id eq 2 } ?: return@runBlocking
        val copy = employee.copy()

        assert(employee == copy)
        assert(employee !== copy)
        assert(employee.hireDate !== copy.hireDate) // should not be the same instance because of deep copy.
        assert(copy.manager?.implementation?.parent === copy.implementation) // should keep the parent relationship.
    }

    @Test
    fun testRemoveIf() = runBlocking {
        database.employees.removeIf { it.departmentId eq 1 }
        assert(database.employees.count() == 2)
    }

    @Test
    fun testClear() = runBlocking {
        database.employees.clear()
        assert(database.employees.isEmpty())
    }

    @Test
    fun testAddAndFlushChanges() = runBlocking {
        var employee = Employee {
            name = "jerry"
            job = "trainee"
            manager = database.employees.find { it.name eq "vince" }
            hireDate = LocalDate.now()
            salary = 50
            department = database.departments.find { it.name eq "tech" } ?: throw AssertionError()
        }

        database.employees.add(employee)

        employee.job = "engineer"
        employee.flushChanges()

        employee = database.employees.find { it.id eq employee.id } ?: throw AssertionError()
        assert(employee.job == "engineer")
    }

    @Test
    fun testValueEquality() = runBlocking {
        val now = LocalDate.now()
        val employee1 = Employee {
            id = 1
            name = "Eric"
            job = "contributor"
            hireDate = now
            salary = 50
        }

        val employee2 = Employee {
            id = 1
            name = "Eric"
            job = "contributor"
            hireDate = now
            salary = 50
        }

        assert(employee1 == employee2)
    }

    @Test
    fun testDifferentClassesSameValuesNotEqual() {
        val employee = Employee {
            name = "name"
        }

        val department = Department {
            name = "name"
        }

        assert(employee != department)
    }
}
