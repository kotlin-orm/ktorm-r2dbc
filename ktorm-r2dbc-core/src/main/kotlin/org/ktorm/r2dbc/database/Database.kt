package org.ktorm.r2dbc.database

import io.r2dbc.spi.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.ktorm.r2dbc.expression.ArgumentExpression
import org.ktorm.r2dbc.expression.SqlExpression
import org.ktorm.r2dbc.logging.Logger
import org.ktorm.r2dbc.logging.detectLoggerImplementation
import org.ktorm.r2dbc.schema.SqlType
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




    init {
        fun kotlin.Result<String?>.orEmpty() = getOrNull().orEmpty()
        fun kotlin.Result<Boolean>.orFalse() = getOrDefault(false)

        runBlocking {
            useConnection { conn ->
                val metadata = conn.metadata
                productName = metadata.runCatching { databaseProductName }.orEmpty()
                productVersion = metadata.runCatching { databaseVersion }.orEmpty()
                keywords = ANSI_SQL_2003_KEYWORDS + dialect.sqlKeywords
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
    public suspend inline fun <T> useTransaction(isolation: IsolationLevel? = null, func: (Transaction) -> T): T {
        contract {
            callsInPlace(func, InvocationKind.EXACTLY_ONCE)
        }

        val current = transactionManager.getCurrentTransaction()
        val isOuter = current == null
        val transaction = current ?: transactionManager.newTransaction(isolation)
        var throwable: Throwable? = null

        try {
            return func(transaction)
        } catch (e: R2dbcException) {
            throwable = exceptionTranslator?.invoke(e) ?: e
            throw throwable
        } catch (e: Throwable) {
            throwable = e
            throw throwable
        } finally {
            if (isOuter) {
                try {
                    if (throwable == null) transaction.commit() else transaction.rollback()
                } finally {
                    transaction.close()
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

    public suspend fun executeQuery(expression: SqlExpression): List<Row> {
        executeExpression(expression) { result ->
            val rows = result.map { row, _ -> row }.toList()

            if (logger.isDebugEnabled()) {
                logger.debug("Results: ${rows.size}")
            }

            return rows
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
                    logger.debug("Parameters: " + args.map { "${it.value}(${it.sqlType.javaType.simpleName})" })
                }

                statement.bindParameters(args)
                statement.add()
            }

            val results = statement.execute().toList()

           /* if (logaddBatchger.isDebugEnabled()) {
                logger.debug("Effects: ${results?.contentToString()}")
            }*/

            return results.map { result -> result.rowsUpdated.awaitFirst() }.toIntArray()
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