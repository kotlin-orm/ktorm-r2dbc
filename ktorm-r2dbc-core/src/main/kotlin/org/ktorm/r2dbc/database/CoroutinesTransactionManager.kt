package org.ktorm.r2dbc.database

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.IsolationLevel
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.awaitSingleOrNull

/**
 * Created by vince on Jan 30, 2021.
 */
public class CoroutinesTransactionManager(
    public val connectionFactory: ConnectionFactory
) : TransactionManager {
    private val currentTransaction = CoroutineLocal<Transaction>()

    override val defaultIsolation: IsolationLevel? = null

    override suspend fun getCurrentTransaction(): Transaction? {
        return currentTransaction.get()
    }

    override suspend fun newTransaction(isolation: IsolationLevel?): Transaction {
        if (currentTransaction.get() != null) {
            throw IllegalStateException("There is already a transaction in the current context.")
        }

        val transaction = TransactionImpl(connectionFactory.create().awaitSingle(), isolation)
        currentTransaction.set(transaction)
        transaction.begin()
        return transaction
    }

    private inner class TransactionImpl(
        override val connection: Connection,
        private val desiredIsolation: IsolationLevel?
    ) : Transaction {
        private val originIsolation = connection.transactionIsolationLevel
        private val originAutoCommit = connection.isAutoCommit

        suspend fun begin() {
            try {
                if (desiredIsolation != null && desiredIsolation != originIsolation) {
                    connection.setTransactionIsolationLevel(desiredIsolation).awaitSingleOrNull()
                }
                if (originAutoCommit) {
                    connection.setAutoCommit(false).awaitSingleOrNull()
                }

                connection.beginTransaction().awaitSingleOrNull()
            } catch (e: Throwable) {
                close()
                throw e
            }
        }

        override suspend fun commit() {
            connection.commitTransaction().awaitSingleOrNull()
        }

        override suspend fun rollback() {
            connection.rollbackTransaction().awaitSingleOrNull()
        }

        override suspend fun close() {
            try {
                if (desiredIsolation != null && desiredIsolation != originIsolation) {
                    connection.setTransactionIsolationLevel(originIsolation).awaitSingleOrNull()
                }
                if (originAutoCommit) {
                    connection.setAutoCommit(true).awaitSingleOrNull()
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    connection.close().awaitSingleOrNull()
                } catch (_: Throwable) {
                } finally {
                    currentTransaction.remove()
                }
            }
        }
    }
}
