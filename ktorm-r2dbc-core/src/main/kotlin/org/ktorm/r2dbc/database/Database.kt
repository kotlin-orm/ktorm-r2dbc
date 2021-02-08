package org.ktorm.r2dbc.database

import io.r2dbc.spi.*
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.awaitSingleOrNull
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
    public val exceptionTranslator: ((R2dbcException) -> Throwable)? = null
) {

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
                if (transaction == null) connection.close().awaitSingleOrNull()
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
    public suspend inline fun <T> executeExpression(expression: SqlExpression, func: (Statement) -> T): T {
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

            return func(statement)
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