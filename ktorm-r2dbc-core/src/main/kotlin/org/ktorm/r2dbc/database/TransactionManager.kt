/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ktorm.r2dbc.database

import io.r2dbc.spi.Connection
import io.r2dbc.spi.IsolationLevel

/**
 * Transaction manager abstraction used to manage database connections and transactions.
 *
 * Applications can use this interface directly, but it is not primary meant as API:
 * Typically, transactions are used by calling the [Database.useTransaction] function.
 */
public interface TransactionManager {

    /**
     * The default transaction isolation, null for the default isolation level of the underlying datastore.
     */
    public val defaultIsolation: IsolationLevel?

    /**
     * The opened transaction of the current [CoroutineContext], null if there is no transaction opened.
     */
    public suspend fun getCurrentTransaction(): Transaction?

    /**
     * Open a new transaction for the [CoroutineContext] using the specific isolation.
     *
     * @param isolation the transaction isolation, by default, [defaultIsolation] is used.
     * @return the result of the callback function.
     * @throws [IllegalStateException] if there is already a transaction opened.
     */
    public suspend fun <T> useTransaction(
        isolation: IsolationLevel? = defaultIsolation,
        func: suspend (Transaction) -> T
    ): T
}

/**
 * Representation of a transaction.
 *
 * Transactional code can use this interface to retrieve the backend connection, and
 * to programmatically trigger a commit or rollback (instead of implicit commits and rollbacks
 * of using [Database.useTransaction]).
 */
public interface Transaction {

    /**
     * The backend R2DBC connection of this transaction.
     */
    public val connection: Connection

    /**
     * Commit the transaction.
     */
    public suspend fun commit()

    /**
     * Rollback the transaction.
     */
    public suspend fun rollback()

    /**
     * Close the transaction and release its underlying resources (eg. the backend connection).
     */
    public suspend fun close()
}
