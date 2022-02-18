package org.ktorm.r2dbc.dsl

import io.r2dbc.spi.Row
import org.ktorm.r2dbc.expression.ColumnDeclaringExpression
import org.ktorm.r2dbc.schema.Column

public class QueryRow internal constructor(public val query: Query, private val row: Row) : Row by row {

    public operator fun <C : Any> get(column: ColumnDeclaringExpression<C>, columnClass: Class<C>): C? {
        if (column.declaredName.isNullOrBlank()) {
            throw IllegalArgumentException("Label of the specified column cannot be null or blank.")
        }
        val metadata = row.metadata
        for (index in metadata.columnMetadatas.indices) {
            if (metadata.getColumnMetadata(index).name eq column.declaredName) {
                return row.get(index, columnClass)
            }
        }
        return null
    }

    /**
     * Obtain the value of the specific [Column] instance.
     *
     * Note that if the column doesn't exist in the result set, this function will return null rather than
     * throwing an exception.
     */
    public operator fun <C : Any> get(column: Column<C>): C? {
        val metadata = row.metadata
        if (query.expression.findDeclaringColumns().isNotEmpty()) {
            // Try to find the column by label.
            for (index in metadata.columnMetadatas.indices) {
                if (metadata.getColumnMetadata(index).name eq column.label) {
                    return column.sqlType.getResult(row,metadata,index)
                }
            }
            // Return null if the column doesn't exist in the result set.
            return null
        } else {
            // Try to find the column by name and its table name (happens when we are using `select *`).
            val indices = metadata.columnMetadatas.indices.filter { index ->
                /*val tableName = metadata.getTableName(index)
                val tableNameMatched = tableName.isBlank() || tableName eq table.alias || tableName eq table.tableName
                val columnName = metaData.getColumnName(index)*/
                metadata.columnMetadatas[index].name eq column.name/* && tableNameMatched*/
            }

            return when (indices.size) {
                0 -> null // Return null if the column doesn't exist in the result set.
                1 -> return column.sqlType.getResult(row,metadata,indices.first())
                else -> throw IllegalArgumentException(warningConfusedColumnName(column.name))
            }
        }
    }


    private infix fun String?.eq(other: String?) = this.equals(other, ignoreCase = true)

    private fun warningConfusedColumnName(name: String): String {
        return "Confused column name, there are more than one column named '$name' in query: \n\n${query.sql}\n"
    }
}