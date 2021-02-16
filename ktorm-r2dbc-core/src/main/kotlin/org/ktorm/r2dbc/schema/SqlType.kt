package org.ktorm.r2dbc.schema

import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import io.r2dbc.spi.Statement
import kotlin.reflect.KClass

public interface SqlType<T : Any> {

    public fun bindParameter(statement: Statement, index: Int, value: T?)

    public fun bindParameter(statement: Statement, name: String, value: T?)
    
    public fun getResult(row: Row, metadata: RowMetadata, index: Int): T?
    
    public fun getResult(row: Row, metadata: RowMetadata, name: String): T?

    public fun <R : Any> transform(fromUnderlyingValue: (T) -> R, toUnderlyingValue: (R) -> T): SqlType<R> {
        return TransformedSqlType(this, fromUnderlyingValue, toUnderlyingValue)
    }
}

public class SimpleSqlType<T : Any>(public val kotlinType: KClass<T>) : SqlType<T> {

    override fun bindParameter(statement: Statement, index: Int, value: T?) {
        if (value == null) {
            statement.bindNull(index, kotlinType.java)
        } else {
            statement.bind(index, value)
        }
    }

    override fun bindParameter(statement: Statement, name: String, value: T?) {
        if (value == null) {
            statement.bindNull(name, kotlinType.java)
        } else {
            statement.bind(name, value)
        }
    }

    override fun getResult(row: Row, metadata: RowMetadata, index: Int): T? {
        return row.get(index, kotlinType.java)
    }

    override fun getResult(row: Row, metadata: RowMetadata, name: String): T? {
        return row.get(name, kotlinType.java)
    }

//    public companion object {
//        public inline operator fun <reified T : Any> invoke(): SimpleSqlType<T> = SimpleSqlType(T::class)
//    }
}

public class TransformedSqlType<T : Any, R : Any>(
    public val underlyingType: SqlType<T>,
    public val fromUnderlyingValue: (T) -> R,
    public val toUnderlyingValue: (R) -> T
) : SqlType<R> {

    override fun bindParameter(statement: Statement, index: Int, value: R?) {
        underlyingType.bindParameter(statement, index, value?.let(toUnderlyingValue))
    }

    override fun bindParameter(statement: Statement, name: String, value: R?) {
        underlyingType.bindParameter(statement, name, value?.let(toUnderlyingValue))
    }

    override fun getResult(row: Row, metadata: RowMetadata, index: Int): R? {
        return underlyingType.getResult(row, metadata, index)?.let(fromUnderlyingValue)
    }

    override fun getResult(row: Row, metadata: RowMetadata, name: String): R? {
        return underlyingType.getResult(row, metadata, name)?.let(fromUnderlyingValue)
    }
}
