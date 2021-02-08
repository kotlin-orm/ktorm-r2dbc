package org.ktorm.r2dbc.database

import java.util.*

/**
 * Created by vince on Feb 08, 2021.
 */
public interface SqlDialect

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
