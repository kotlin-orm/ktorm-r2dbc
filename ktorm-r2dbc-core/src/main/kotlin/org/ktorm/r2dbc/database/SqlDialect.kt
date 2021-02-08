package org.ktorm.r2dbc.database

import org.ktorm.r2dbc.expression.QueryExpression
import org.ktorm.r2dbc.expression.SqlFormatter
import java.util.*

/**
 * Created by vince on Feb 08, 2021.
 */
public interface SqlDialect {

    /**
     * Create a [SqlFormatter] instance, formatting SQL expressions as strings with their execution arguments.
     *
     * @param database the current database instance executing the formatted SQL.
     * @param beautifySql if we should output beautiful SQL strings with line-wrapping and indentation.
     * @param indentSize the indent size.
     * @return a [SqlFormatter] object, generally typed of subclasses to support dialect-specific sql expressions.
     */
    public fun createSqlFormatter(database: Database, beautifySql: Boolean, indentSize: Int): SqlFormatter {
        return object : SqlFormatter(database, beautifySql, indentSize) {
            override fun writePagination(expr: QueryExpression) {
                throw DialectFeatureNotSupportedException("Pagination is not supported in Standard SQL.")
            }
        }
    }
}

/**
 * Thrown to indicate that a feature is not supported by the current dialect.
 *
 * @param message the detail message, which is saved for later retrieval by [Throwable.message].
 * @param cause the cause, which is saved for later retrieval by [Throwable.cause].
 */
public class DialectFeatureNotSupportedException(
    message: String? = null,
    cause: Throwable? = null
) : UnsupportedOperationException(message, cause) {

    private companion object {
        private const val serialVersionUID = 1L
    }
}

/**
 * Auto detect a dialect implementation.
 */
public fun detectDialectImplementation(): SqlDialect {
    val dialects = ServiceLoader.load(SqlDialect::class.java).toList()
    return when (dialects.size) {
        0 -> object : SqlDialect { }
        1 -> dialects[0]
        else -> error("More than one dialect implementations found in the classpath, " +
            "please choose one manually, they are: $dialects")
    }
}
