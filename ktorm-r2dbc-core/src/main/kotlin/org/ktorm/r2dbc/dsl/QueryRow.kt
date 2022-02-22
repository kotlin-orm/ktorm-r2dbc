package org.ktorm.r2dbc.dsl

import io.r2dbc.spi.Row
import org.ktorm.r2dbc.expression.ColumnDeclaringExpression
import org.ktorm.r2dbc.schema.Column
import java.sql.ResultSet

/**
 * Special implementation of [Row], used to hold the [Query] results for Ktorm.
 *
 * Different from normal rows, this class provides additional features:
 *
 *  - **Indexed access operator:** It overloads the indexed access operator, so we can use square brackets `[]` to
 * obtain the value by giving a specific [Column] instance. It’s less error prone by the benefit of the compiler’s
 * static checking. Also, we can still use get functions in the [Row] to obtain our results by labels or
 * column indices.
 *
 * ```kotlin
 * val query = database.from(Employees).select()
 * for (row in query.rowSet) {
 *     println(row[Employees.name])
 * }
 * ```
 */
public class QueryRow internal constructor(public val query: Query, private val row: Row) : Row by row {

    /**
     * Obtain the value of the specific [Column] instance.
     *
     * Note that if the column doesn't exist in the row, this function will return null rather than
     * throwing an exception.
     */
    public operator fun <C : Any> get(column: Column<C>): C? {
        val metadata = row.metadata
        if (query.expression.findDeclaringColumns().isNotEmpty()) {
            // Try to find the column by label.
            for (index in metadata.columnMetadatas.indices) {
                if (metadata.getColumnMetadata(index).name eq column.label) {
                    return column.sqlType.getResult(row,index)
                }
            }
            // Return null if the column doesn't exist in the row.
            return null
        } else {
            // Try to find the column by name and its table name (happens when we are using `select *`).
            val indices = metadata.columnMetadatas.indices.filter { index ->
                metadata.columnMetadatas[index].name eq column.name/* && tableNameMatched*/
            }

            return when (indices.size) {
                0 -> null // Return null if the column doesn't exist in the row.
                1 -> return column.sqlType.getResult(row,indices.first())
                else -> throw IllegalArgumentException(warningConfusedColumnName(column.name))
            }
        }
    }

    /**
     * Obtain the value of the specific [ColumnDeclaringExpression] instance.
     *
     * Note that if the column doesn't exist in the row, this function will return null rather than
     * throwing an exception.
     */
    public operator fun <C : Any> get(column: ColumnDeclaringExpression<C>): C? {
        if (column.declaredName.isNullOrBlank()) {
            throw IllegalArgumentException("Label of the specified column cannot be null or blank.")
        }

        for (index in  row.metadata.columnMetadatas.indices) {
            if (row.metadata.columnMetadatas[index].name eq column.declaredName) {
                return column.sqlType.getResult(row,index)
            }
        }

        // Return null if the column doesn't exist in the row.
        return null
    }

    private infix fun String?.eq(other: String?) = this.equals(other, ignoreCase = true)

    private fun warningConfusedColumnName(name: String): String {
        return "Confused column name, there are more than one column named '$name' in query: \n\n${query.sql}\n"
    }
}
