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

        val connection = connectionFactory.create().awaitSingle()

        val originIsolation = connection.transactionIsolationLevel
        if (isolation != null && isolation != originIsolation) {
            connection.setTransactionIsolationLevel(isolation).awaitSingleOrNull()
        }

        val originAutoCommit = connection.isAutoCommit
        if (originAutoCommit) {
            connection.setAutoCommit(false).awaitSingleOrNull()
        }

        val transaction = object : Transaction {
            override val connection: Connection = connection

            override suspend fun commit() {
                connection.commitTransaction().awaitSingleOrNull()
            }

            override suspend fun rollback() {
                connection.rollbackTransaction().awaitSingleOrNull()
            }

            override suspend fun close() {
                try {
                    if (isolation != null && isolation != originIsolation) {
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

        currentTransaction.set(transaction)
        return transaction
    }
}
