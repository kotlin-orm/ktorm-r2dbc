package org.ktorm.r2dbc.database

import org.ktorm.r2dbc.expression.QueryExpression
import org.ktorm.r2dbc.expression.SqlFormatter
import java.util.*

/**
 * Representation of a SQL dialect.
 *
 * It's known that there is a uniform standard for SQL language, but beyond the standard, many databases still have
 * their special features. The interface provides an extension mechanism for Ktorm and its extension modules to support
 * those dialect-specific SQL features.
 *
 * Implementations of this interface are recommended to be published as separated modules independent of ktorm-core.
 *
 * To enable a dialect, applications should add the dialect module to the classpath first, then configure the `dialect`
 * parameter to the dialect implementation while creating database instances via [Database.connect] functions.
 *
 * Ktorm's dialect modules start following the convention of JDK [ServiceLoader] SPI, so we don't
 * need to specify the `dialect` parameter explicitly anymore while creating [Database] instances. Ktorm auto detects
 * one for us from the classpath. We just need to insure the dialect module exists in the dependencies.
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

    /**
     * What is the string used to quote SQL identifiers? This returns a space if identifier quoting
     * isn't supported. A JDBC Compliant driver will always use a double quote character.
     */
    public val identifierQuoteString: String

    /**
     * All the "extra" characters that can be used in unquoted identifier names (those beyond a-z, A-Z, 0-9 and _).
     */
    public val extraNameCharacters: String

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case sensitive and as a result
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val supportsMixedCaseIdentifiers: Boolean

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val storesMixedCaseIdentifiers: Boolean

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in upper case.
     *
     * @since 3.1.0
     */
    public val storesUpperCaseIdentifiers: Boolean

    /**
     * Whether this database treats mixed case unquoted SQL identifiers as case insensitive and
     * stores them in lower case.
     *
     * @since 3.1.0
     */
    public val storesLowerCaseIdentifiers: Boolean

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case sensitive and as a result
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val supportsMixedCaseQuotedIdentifiers: Boolean

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in mixed case.
     *
     * @since 3.1.0
     */
    public val storesMixedCaseQuotedIdentifiers: Boolean

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in upper case.
     *
     * @since 3.1.0
     */
    public val storesUpperCaseQuotedIdentifiers: Boolean

    /**
     * Whether this database treats mixed case quoted SQL identifiers as case insensitive and
     * stores them in lower case.
     *
     * @since 3.1.0
     */
    public val storesLowerCaseQuotedIdentifiers: Boolean

    /**
     * Retrieves a comma-separated list of all of this database's SQL keywords
     * that are NOT also SQL:2003 keywords.
     *
     * @return the list of this database's keywords that are not also
     *         SQL:2003 keywords
     * @since 3.1.0
     */
    public val sqlKeywords: Set<String>

    /**
     * The maximum number of characters this database allows for a column name. Zero means that there is no limit
     * or the limit is not known.
     *
     * @since 3.1.0
     */
    public val maxColumnNameLength: Int

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
        0 -> object : SqlDialect {
            override val identifierQuoteString: String = ""
            override val extraNameCharacters: String = ""
            override val supportsMixedCaseIdentifiers: Boolean = false
            override val storesMixedCaseIdentifiers: Boolean = false
            override val storesUpperCaseIdentifiers: Boolean = false
            override val storesLowerCaseIdentifiers: Boolean = false
            override val supportsMixedCaseQuotedIdentifiers: Boolean = false
            override val storesMixedCaseQuotedIdentifiers: Boolean = false
            override val storesUpperCaseQuotedIdentifiers: Boolean = false
            override val storesLowerCaseQuotedIdentifiers: Boolean = false
            override val sqlKeywords: Set<String> = emptySet()
            override val maxColumnNameLength: Int = 0
        }
        1 -> dialects[0]
        else -> error(
            "More than one dialect implementations found in the classpath, " +
                    "please choose one manually, they are: $dialects"
        )
    }
}
