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

import io.r2dbc.spi.Statement
import org.ktorm.r2dbc.expression.ArgumentExpression
import org.ktorm.r2dbc.schema.SqlType


/**
 * Set the arguments for this [Statement].
 *
 * @since 2.7
 * @param args the arguments to bind into the statement.
 */
public fun Statement.bindParameters(args: List<ArgumentExpression<*>>) {
    for ((i, expr) in args.withIndex()) {
        @Suppress("UNCHECKED_CAST")
        val sqlType = expr.sqlType as SqlType<Any>
        sqlType.bindParameter(this, i + 1, expr.value)
    }
}
