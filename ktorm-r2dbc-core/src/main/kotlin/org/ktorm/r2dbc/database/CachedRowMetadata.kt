package org.ktorm.r2dbc.database

import io.r2dbc.spi.ColumnMetadata
import io.r2dbc.spi.Nullability
import io.r2dbc.spi.RowMetadata
import java.util.*
import kotlin.collections.AbstractCollection

/**
 * Created by vince on Feb 10, 2021.
 */
internal class CachedRowMetadata(metadata: RowMetadata) : RowMetadata {
    private val columns = metadata.columnMetadatas.map { CachedColumnMetadata(it) }

    override fun getColumnMetadata(index: Int): ColumnMetadata {
        return columns[index]
    }

    override fun getColumnMetadata(name: String): ColumnMetadata {
        return columns.first { it.name.equals(name, ignoreCase = true) }
    }

    override fun getColumnMetadatas(): Iterable<ColumnMetadata> {
        return Collections.unmodifiableList(columns)
    }

    override fun getColumnNames(): Collection<String> {
        return object : AbstractCollection<String>() {
            override val size: Int = columns.size

            override fun iterator(): Iterator<String> {
                return TransformedIterator(columns.iterator()) { it.name }
            }

            override fun contains(element: String): Boolean {
                return columns.any { it.name.equals(element, ignoreCase = true) }
            }
        }
    }

    private class TransformedIterator<T, R>(
        private val sourceIterator: Iterator<T>,
        private val transform: (T) -> R
    ) : Iterator<R> {
        override fun hasNext(): Boolean {
            return sourceIterator.hasNext()
        }

        override fun next(): R {
            return transform(sourceIterator.next())
        }
    }

    private class CachedColumnMetadata(metadata: ColumnMetadata) : ColumnMetadata {
        private val _javaType = metadata.javaType
        private val _name = metadata.name
        private val _nativeTypeMetadata = metadata.nativeTypeMetadata
        private val _nullability = metadata.nullability
        private val _precision = metadata.precision
        private val _scale = metadata.scale

        override fun getJavaType(): Class<*>? {
            return _javaType
        }

        override fun getName(): String {
            return _name
        }

        override fun getNativeTypeMetadata(): Any? {
            return _nativeTypeMetadata
        }

        override fun getNullability(): Nullability {
            return _nullability
        }

        override fun getPrecision(): Int? {
            return _precision
        }

        override fun getScale(): Int? {
            return _scale
        }
    }
}
