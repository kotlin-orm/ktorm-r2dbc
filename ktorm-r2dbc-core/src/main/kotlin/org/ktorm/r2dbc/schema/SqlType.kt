package org.ktorm.r2dbc.schema

import io.r2dbc.spi.ColumnMetadata
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import kotlin.reflect.KClass

/**
 * SQL data type interface.
 *
 * Based on R2DBC, [SqlType] and its subclasses encapsulate the common operations of obtaining data from a [Row]
 * and setting parameters to a [Statement].
 *
 */
public interface SqlType<T : Any> {

    /**
     * Binding the [value] to a given [Statement]
     */
    public fun bindParameter(statement: Statement, index: Int, value: T?)

    /**
     * Binding the [value] to a given [Statement]
     */
    public fun bindParameter(statement: Statement, name: String, value: T?)

    /**
     * Obtain a result from a given [Row] by [index], the result may be null.
     */
    public fun getResult(row: Row, index: Int): T?

    /**
     * Obtain a result from a given [Row] by [name], the result may be null.
     */
    public fun getResult(row: Row, name: String): T?

}

/**
 * Transform this [SqlType] to another. The returned [SqlType] performs a specific conversion on the column value.
 *
 * This function enables a user-friendly syntax to extend more data types. For example, the following code defines
 * a column of type `Column<UserRole>`, based on the existing [IntSqlType]:
 *
 * ```kotlin
 * val role by registerColumn("role", IntSqlType.transform({ UserRole.fromCode(it) }, { it.code }))
 * ```
 *
 * @param fromUnderlyingValue a function that transforms a value of underlying type to the user's type.
 * @param toUnderlyingValue a function that transforms a value of user's type the to the underlying type.
 * @return a [SqlType] instance based on this underlying type with specific transformations.
 */
public fun <T : Any, R : Any> SqlType<T>.transform(
    fromUnderlyingValue: (T) -> R,
    toUnderlyingValue: (R) -> T,
): SqlType<R> {
    return TransformedSqlType(this, fromUnderlyingValue, toUnderlyingValue)
}

/**
 * Simple [SqlType] implementation, pass the specified type [kotlinType] to [Statement] for binding or acquisition,
 * and r2dbc driver parses the specified Java type
 *
 * @param kotlinType Specify the associated kotlin type
 */
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

}

/**
 * Convertible SqlType type abstraction
 *
 * In the r2dbc query result, the metadata information [ColumnMetadata] of the data result is included, which includes
 * the Java type corresponding to each column. However, what Java type corresponds to the specific SQL type is
 * determined by R2DBC driver implementation, maybe the type is exactly what we want, maybe not. [ConvertibleSqlType]
 * and its subclasses can convert the object returned by the R2DBC driver to the specified type, E.g:
 *
 * ```kotlin
 * public object IntSqlType : ConvertibleSqlType<Int>(Int::class) {
 *      override fun convert(value: Any): Int {
 *          return when (value) {
 *              is Number -> value.toInt()
 *              is String -> value.toInt()
 *              else -> throw IllegalStateException("Converting type is not supported from value:$value")
 *          }
 *      }
 * }
 *
 * ```
 *
 * @param kotlinType Specify the kotlin type
 */
public abstract class ConvertibleSqlType<R : Any>(kotlinType: KClass<R>) : SimpleSqlType<R>(kotlinType) {

    /**
     * Convert the object returned by the R2DBC driver query to the specified kotlinType
     * @param value Value returned from R2DBC query
     */
    public abstract fun convert(value: Any): R

    override fun getResult(row: Row, index: Int): R? {
        val metadata = row.metadata.getColumnMetadata(index)
        val value = row.get(index, metadata.javaType) ?: return null
        return convert(value)
    }

    override fun getResult(row: Row, name: String): R? {
        val metadata = row.metadata.getColumnMetadata(name)
        val value = row.get(name, metadata.javaType) ?: return null
        return convert(value)
    }
}

/**
 * Transform [underlyingType] to another. this [SqlType] performs a specific conversion on the column value.
 *
 * This function enables a user-friendly syntax to extend more data types. For example, the following code defines
 * a column of type `Column<UserRole>`, based on the existing [IntSqlType]:
 *
 * ```kotlin
 * val role by registerColumn("role", IntSqlType.transform({ UserRole.fromCode(it) }, { it.code }))
 * ```
 *
 * @see [transform]
 * @param underlyingType [SqlType] to be converted
 * @param fromUnderlyingValue a function that transforms a value of underlying type to the user's type.
 * @param toUnderlyingValue a function that transforms a value of user's type the to the underlying type.
 */
public class TransformedSqlType<T : Any, R : Any>(
    public val underlyingType: SqlType<T>,
    public val fromUnderlyingValue: (T) -> R,
    public val toUnderlyingValue: (R) -> T,
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
