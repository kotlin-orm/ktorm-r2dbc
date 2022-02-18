package org.ktorm.r2dbc.database

import io.r2dbc.spi.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.ktorm.r2dbc.expression.ArgumentExpression
import org.ktorm.r2dbc.expression.SqlExpression
import org.ktorm.r2dbc.logging.Logger
import org.ktorm.r2dbc.logging.detectLoggerImplementation
import org.ktorm.r2dbc.schema.IntSqlType
import org.ktorm.r2dbc.schema.SqlType
import java.sql.PreparedStatement
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/**
 * Created by vince on Feb 08, 2021.
 */
public class Database(
    public val connectionFactory: ConnectionFactory,
    public val transactionManager: TransactionManager = CoroutinesTransactionManager(connectionFactory),
    public val dialect: SqlDialect = detectDialectImplementation(),
    public val logger: Logger = detectLoggerImplementation(),
    public val exceptionTranslator: ((R2dbcException) -> Throwable)? = null,
    public val alwaysQuoteIdentifiers: Boolean = false,
    public val generateSqlInUpperCase: Boolean? = null
) {

    /**
     * The name of the connected database product, eg. MySQL, H2.
     */
    public val productName: String

    /**
     * The version of the connected database product.
     */
    public val productVersion: String

    /**
     * A set of all of this database's SQL keywords (including SQL:2003 keywords), all in uppercase.
     */
    public val keywords: Set<String>

    /**
     * The string used to quote SQL identifiers, returns an empty string if identifier quoting is not supported.
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
     * The maximum number of characters this database allows for a column name. Zero means that there is no limit
     * or the limit is not known.
     *
     * @since 3.1.0
     */
    public val maxColumnNameLength: Int


    init {
        fun kotlin.Result<String?>.orEmpty() = getOrNull().orEmpty()

        runBlocking {
            useConnection { conn ->
                val metadata = conn.metadata
                productName = metadata.runCatching { databaseProductName }.orEmpty()
                productVersion = metadata.runCatching { databaseVersion }.orEmpty()
                keywords = ANSI_SQL_2003_KEYWORDS + dialect.sqlKeywords
                identifierQuoteString = dialect.identifierQuoteString
                extraNameCharacters = dialect.extraNameCharacters
                supportsMixedCaseIdentifiers = dialect.supportsMixedCaseIdentifiers
                storesMixedCaseIdentifiers = dialect.storesMixedCaseIdentifiers
                storesUpperCaseIdentifiers = dialect.storesUpperCaseIdentifiers
                storesLowerCaseIdentifiers = dialect.storesLowerCaseIdentifiers
                supportsMixedCaseQuotedIdentifiers = dialect.supportsMixedCaseQuotedIdentifiers
                storesMixedCaseQuotedIdentifiers = dialect.storesMixedCaseQuotedIdentifiers
                storesUpperCaseQuotedIdentifiers = dialect.storesUpperCaseQuotedIdentifiers
                storesLowerCaseQuotedIdentifiers = dialect.storesLowerCaseQuotedIdentifiers
                maxColumnNameLength = dialect.maxColumnNameLength
            }

            if (logger.isInfoEnabled()) {
                val msg = "Connected to productName: %s, productVersion: %s, logger: %s, dialect: %s"
                logger.info(msg.format(productName, productVersion, logger, dialect))
            }
        }
    }

    @OptIn(ExperimentalContracts::class)
    public suspend inline fun <T> useConnection(func: (Connection) -> T): T {
        contract {
            callsInPlace(func, InvocationKind.EXACTLY_ONCE)
        }

        try {
            val transaction = transactionManager.getCurrentTransaction()
            val connection = transaction?.connection ?: connectionFactory.create().awaitSingle()

            try {
                return func(connection)
            } finally {
                if (transaction == null) connection.close().awaitFirstOrNull()
            }
        } catch (e: R2dbcException) {
            throw exceptionTranslator?.invoke(e) ?: e
        }
    }

    @OptIn(ExperimentalContracts::class)
    public suspend fun <T> useTransaction(
        isolation: IsolationLevel? = null,
        func: suspend (Transaction) -> T
    ): T {
        contract {
            callsInPlace(func, InvocationKind.EXACTLY_ONCE)
        }

        val current = transactionManager.getCurrentTransaction()

        if (current != null) {
            return func(current)
        } else {
            return transactionManager.useTransaction(isolation) {
                var throwable: Throwable? = null
                try {
                    func(it)
                } catch (e: R2dbcException) {
                    throwable = exceptionTranslator?.invoke(e) ?: e
                    throw throwable
                } catch (e: Throwable) {
                    throwable = e
                    throw throwable
                } finally {
                    try {
                        if (throwable == null) it.commit() else it.rollback()
                    } finally {
                        it.close()
                    }
                }
            }
        }
    }

    public fun formatExpression(
        expression: SqlExpression,
        beautifySql: Boolean = false,
        indentSize: Int = 2
    ): Pair<String, List<ArgumentExpression<*>>> {
        val formatter = dialect.createSqlFormatter(this, beautifySql, indentSize)
        formatter.visit(expression)
        return Pair(formatter.sql, formatter.parameters)
    }

    @OptIn(ExperimentalContracts::class)
    public suspend inline fun <T> executeExpression(expression: SqlExpression, func: (Result) -> T): T {
        contract {
            callsInPlace(func, InvocationKind.EXACTLY_ONCE)
        }

        val (sql, args) = formatExpression(expression)

        if (logger.isDebugEnabled()) {
            logger.debug("SQL: $sql")
            logger.debug("Parameters: " + args.map { it.value.toString() })
        }

        useConnection { conn ->
            val statement = conn.createStatement(sql)

            for ((i, expr) in args.withIndex()) {
                @Suppress("UNCHECKED_CAST")
                val sqlType = expr.sqlType as SqlType<Any>
                sqlType.bindParameter(statement, i, expr.value)
            }

            return func(statement.execute().awaitSingle())
        }
    }

    public suspend fun executeQuery(expression: SqlExpression): Flow<Row> {
        executeExpression(expression) { result ->
            return result.map { row, _ -> row }.asFlow()
        }
    }

    public suspend fun executeUpdate(expression: SqlExpression): Int {
        executeExpression(expression) { result ->
            val effects = result.rowsUpdated.awaitSingle()

            if (logger.isDebugEnabled()) {
                logger.debug("Effects: $effects")
            }

            return effects
        }
    }

    /**
     * Batch execute the given SQL expressions and return the effected row counts for each expression.
     *
     * Note that this function is implemented based on [Statement.add] and [Statement.execute],
     * and any item in a batch operation must have the same structure, otherwise an exception will be thrown.
     *
     * @since 2.7
     * @param expressions the SQL expressions to be executed.
     * @return the effected row counts for each sub-operation.
     */
    public suspend fun executeBatch(expressions: List<SqlExpression>): IntArray {
        val (sql, _) = formatExpression(expressions[0])

        if (logger.isDebugEnabled()) {
            logger.debug("SQL: $sql")
        }

        useConnection { conn ->
            val statement = conn.createStatement(sql)
            for (expr in expressions) {
                val (subSql, args) = formatExpression(expr)

                if (subSql != sql) {
                    throw IllegalArgumentException(
                        "Every item in a batch operation must generate the same SQL: \n\n$subSql"
                    )
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Parameters: " + args.map { it.value.toString() })
                }

                statement.bindParameters(args)
                statement.add()
            }

            val results = statement.execute().toList()
            return results.map { result -> result.rowsUpdated.awaitFirst() }.toIntArray()
        }
    }

    /**
     * Format the given [expression] to a SQL string with its execution arguments, execute it via
     * [Statement.execute], then return the effected row count along with the generated keys.
     *
     * @since 2.7
     * @param expression the SQL expression to be executed.
     * @return a [Pair] combines the effected row count and the generated keys.
     */
    public suspend fun executeUpdateAndRetrieveKeys(expression: SqlExpression): Pair<Int, Flow<Row>> {
        val (sql, args) = formatExpression(expression)

        if (logger.isDebugEnabled()) {
            logger.debug("SQL: $sql")
            logger.debug("Parameters: " + args.map { "${it.value}(${it.sqlType.javaType.simpleName})" })
        }

        useConnection {
            val statement = it.createStatement(sql)
            statement.bindParameters(args)
            val rowsUpdated = statement.execute().awaitFirst().rowsUpdated.awaitFirst()
            val rows = statement.returnGeneratedValues().execute().awaitFirst().map { row, _ -> row }.asFlow()
            return Pair(rowsUpdated,rows)
        }
    }

    public companion object {

        public fun connect(
            connectionFactory: ConnectionFactory,
            dialect: SqlDialect = detectDialectImplementation(),
            logger: Logger = detectLoggerImplementation()
        ): Database {
            return Database(
                connectionFactory = connectionFactory,
                transactionManager = CoroutinesTransactionManager(connectionFactory),
                dialect = dialect,
                logger = logger
            )
        }

        public fun connect(
            url: String,
            dialect: SqlDialect = detectDialectImplementation(),
            logger: Logger = detectLoggerImplementation()
        ): Database {
            val connectionFactory = ConnectionFactories.get(url)

            return Database(
                connectionFactory = connectionFactory,
                transactionManager = CoroutinesTransactionManager(connectionFactory),
                dialect = dialect,
                logger = logger
            )
        }
    }
}
