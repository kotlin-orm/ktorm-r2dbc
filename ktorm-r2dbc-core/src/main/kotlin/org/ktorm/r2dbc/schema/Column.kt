package org.ktorm.r2dbc.schema

import org.ktorm.r2dbc.expression.ArgumentExpression
import org.ktorm.r2dbc.expression.ColumnDeclaringExpression
import org.ktorm.r2dbc.expression.ColumnExpression
import org.ktorm.r2dbc.expression.ScalarExpression

/**
 * Common interface of [Column] and [ScalarExpression].
 */
public interface ColumnDeclaring<T : Any> {

    /**
     * The [SqlType] of this column or expression.
     */
    public val sqlType: SqlType<T>

    /**
     * Convert this instance to a [ScalarExpression].
     *
     * If this instance is a [Column], return a [ColumnExpression], otherwise if it's already a [ScalarExpression],
     * return `this` directly.
     */
    public fun asExpression(): ScalarExpression<T>

    /**
     * Wrap this instance as a [ColumnDeclaringExpression].
     */
    public fun aliased(label: String?): ColumnDeclaringExpression<T>

    /**
     * Wrap the given [argument] as an [ArgumentExpression] using the [sqlType].
     */
    public fun wrapArgument(argument: T?): ArgumentExpression<T>
}
