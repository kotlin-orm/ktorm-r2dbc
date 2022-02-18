package org.ktorm.r2dbc.schema

import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import io.r2dbc.spi.Statement
import kotlin.reflect.KClass

public interface SqlType<T : Any> {

    public val javaType: Class<T>

    public fun bindParameter(statement: Statement, index: Int, value: T?)

    public fun bindParameter(statement: Statement, name: String, value: T?)

    public fun getResult(row: Row, index: Int): T?

    public fun getResult(row: Row, name: String): T?

}

public fun <T : Any, R : Any> SqlType<T>.transform(
    fromUnderlyingValue: (T) -> R,
    toUnderlyingValue: (R) -> T,
    javaType: Class<R>
): SqlType<R> {
    return TransformedSqlType(this, fromUnderlyingValue, toUnderlyingValue, javaType)
}

public open class SimpleSqlType<T : Any>(public val kotlinType: KClass<T>) : SqlType<T> {

    override fun bindParameter(statement: Statement, index: Int, value: T?) {
        if (value == null) {
            statement.bindNull(index, kotlinType.javaObjectType)
        } else {
            statement.bind(index, value)
        }
    }

    override fun bindParameter(statement: Statement, name: String, value: T?) {
        if (value == null) {
            statement.bindNull(name, kotlinType.javaObjectType)
        } else {
            statement.bind(name, value)
        }
    }

    override fun getResult(row: Row, index: Int): T? {
        return row.get(index, kotlinType.javaObjectType)
    }

    override fun getResult(row: Row, name: String): T? {
        return row.get(name, kotlinType.javaObjectType)
    }

    override val javaType: Class<T>
        get() = kotlinType.javaObjectType

}

public class TransformedSqlType<T : Any, R : Any>(
    public val underlyingType: SqlType<T>,
    public val fromUnderlyingValue: (T) -> R,
    public val toUnderlyingValue: (R) -> T,
    public override val javaType: Class<R>
) : SqlType<R> {

    override fun bindParameter(statement: Statement, index: Int, value: R?) {
        underlyingType.bindParameter(statement, index, value?.let(toUnderlyingValue))
    }

    override fun bindParameter(statement: Statement, name: String, value: R?) {
        underlyingType.bindParameter(statement, name, value?.let(toUnderlyingValue))
    }

    override fun getResult(row: Row, index: Int): R? {
        return underlyingType.getResult(row, index)?.let(fromUnderlyingValue)
    }

    override fun getResult(row: Row, name: String): R? {
        return underlyingType.getResult(row, name)?.let(fromUnderlyingValue)
    }
}
