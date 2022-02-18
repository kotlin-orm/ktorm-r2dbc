package org.ktorm.r2dbc.database

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.IsolationLevel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * Created by vince on Jan 30, 2021.
 */
public class CoroutinesTransactionManager(
    public val connectionFactory: ConnectionFactory
) : TransactionManager {

    override val defaultIsolation: IsolationLevel? = null

    override suspend fun getCurrentTransaction(): Transaction? {
        return coroutineContext[TransactionKey]
    }

    public override suspend fun <T> useTransaction(
        isolation: IsolationLevel?,
        func: suspend (Transaction) -> T
    ): T {
        val currentTransaction = coroutineContext[TransactionKey]
        if (currentTransaction != null) {
            throw IllegalStateException("There is already a transaction in the current context.")
        }
        val transaction = TransactionImpl(connectionFactory.create().awaitSingle(), isolation)
        transaction.begin()
        return withContext(coroutineContext + transaction) {
            func(transaction)
        }
    }

    /**
     * Key of [TransactionImpl] in [CoroutineContext].
     */
    public companion object TransactionKey : CoroutineContext.Key<TransactionImpl>

    private inner class TransactionImpl(
        override val connection: Connection,
        private val desiredIsolation: IsolationLevel?
    ) : Transaction, AbstractCoroutineContextElement(TransactionKey) {

        private val originIsolation = connection.transactionIsolationLevel
        private val originAutoCommit = connection.isAutoCommit

        suspend fun begin() {
            try {
                if (desiredIsolation != null && desiredIsolation != originIsolation) {
                    connection.setTransactionIsolationLevel(desiredIsolation).awaitFirstOrNull()
                }
                if (originAutoCommit) {
                    connection.setAutoCommit(false).awaitFirstOrNull()
                }

                connection.beginTransaction().awaitFirstOrNull()
            } catch (e: Throwable) {
                close()
                throw e
            }
        }

        override suspend fun commit() {
            connection.commitTransaction().awaitFirstOrNull()
        }

        override suspend fun rollback() {
            connection.rollbackTransaction().awaitFirstOrNull()
        }

        override suspend fun close() {
            try {
                if (desiredIsolation != null && desiredIsolation != originIsolation) {
                    connection.setTransactionIsolationLevel(originIsolation).awaitFirstOrNull()
                }
                if (originAutoCommit) {
                    connection.setAutoCommit(true).awaitFirstOrNull()
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    connection.close().awaitFirstOrNull()
                } catch (_: Throwable) {
                }
            }
        }
    }
}
