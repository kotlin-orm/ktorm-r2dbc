package org.ktorm.r2dbc.database

import io.r2dbc.spi.Blob
import io.r2dbc.spi.Clob
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.runBlocking
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.*

/**
 * Created by vince on Feb 10, 2021.
 */
public open class CachedRow(row: Row, metadata: RowMetadata): Row {
    private val _values = readValues(row, metadata)
    private val _metadata = readMetadata(row, metadata)

    public val metadata: RowMetadata get() = _metadata

    private fun readValues(row: Row, metadata: RowMetadata): Map<String, Any?> {
        if (row is CachedRow) {
            return row._values
        } else {
            return metadata.columnMetadatas.reversed().associate { column ->
                val value = row.get(column.name)
                column.name.toUpperCase() to when (value) {
                    is Clob -> Clob.from(CachedPublisher(value.stream()))
                    is Blob -> Blob.from(CachedPublisher(value.stream()))
                    else -> value
                }
            }
        }
    }

    private fun readMetadata(row: Row, metadata: RowMetadata): CachedRowMetadata {
        return when {
            row is CachedRow -> row._metadata
            metadata is CachedRowMetadata -> metadata
            else -> CachedRowMetadata(metadata)
        }
    }

    override fun <T : Any> get(index: Int, type: Class<T>): T? {
        val column = metadata.getColumnMetadata(index)
        return get(column.name, type)
    }

    override fun <T : Any> get(name: String, type: Class<T>): T? {
        val result = when (type.kotlin) {
            String::class -> getString(name)
            Clob::class -> getClob(name)
            Boolean::class -> getBoolean(name)
            Byte::class -> getByte(name)
            Short::class -> getShort(name)
            Int::class -> getInt(name)
            Long::class -> getLong(name)
            Float::class -> getFloat(name)
            Double::class -> getDouble(name)
            BigDecimal::class -> getBigDecimal(name)
            BigInteger::class -> getBigInteger(name)
            ByteBuffer::class -> getByteBuffer(name)
            ByteArray::class -> getBytes(name)
            Blob::class -> getBlob(name)
            LocalDate::class -> getDate(name)
            LocalTime::class -> getTime(name)
            LocalDateTime::class -> getDateTime(name)
            ZonedDateTime::class -> getZonedDateTime(name)
            OffsetDateTime::class -> getOffsetDateTime(name)
            Instant::class -> getInstant(name)
            else -> getColumnValue(name)
        }

        return type.cast(result)
    }

    private fun getColumnValue(name: String): Any? {
        return _values[name.toUpperCase()]
    }

    private fun getString(name: String): String? {
        return when (val value = getColumnValue(name)) {
            is String -> value
            is Clob -> runBlocking { value.toText() } // Won't block if data was pre-fetched by CachedPublisher.
            else -> value?.toString()
        }
    }

    private fun getClob(name: String): Clob? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is Clob -> value
            is String -> Clob.from(IterableAsPublisher(value))
            else -> throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to Clob.")
        }
    }

    private fun getBoolean(name: String): Boolean? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is Boolean -> value
            is Number -> value.toDouble().toBits() != 0.0.toBits()
            else -> value.toString().toDouble().toBits() != 0.0.toBits()
        }
    }

    private fun getByte(name: String): Byte? {
        return when (val value = getColumnValue(name)) {
            is Byte -> value
            is Number -> value.toByte()
            is Boolean -> if (value) 1 else 0
            else -> value?.toString()?.toByte()
        }
    }

    private fun getShort(name: String): Short? {
        return when (val value = getColumnValue(name)) {
            is Short -> value
            is Number -> value.toShort()
            is Boolean -> if (value) 1 else 0
            else -> value?.toString()?.toShort()
        }
    }

    private fun getInt(name: String): Int? {
        return when (val value = getColumnValue(name)) {
            is Int -> value
            is Number -> value.toInt()
            is Boolean -> if (value) 1 else 0
            else -> value?.toString()?.toInt()
        }
    }

    private fun getLong(name: String): Long? {
        return when (val value = getColumnValue(name)) {
            is Long -> value
            is Number -> value.toLong()
            is Boolean -> if (value) 1 else 0
            else -> value?.toString()?.toLong()
        }
    }

    private fun getFloat(name: String): Float? {
        return when (val value = getColumnValue(name)) {
            is Float -> value
            is Number -> value.toFloat()
            is Boolean -> if (value) 1.0F else 0.0F
            else -> value?.toString()?.toFloat()
        }
    }

    private fun getDouble(name: String): Double? {
        return when (val value = getColumnValue(name)) {
            is Double -> value
            is Number -> value.toDouble()
            is Boolean -> if (value) 1.0 else 0.0
            else -> value?.toString()?.toDouble()
        }
    }

    private fun getBigDecimal(name: String): BigDecimal? {
        return when (val value = getColumnValue(name)) {
            is BigDecimal -> value
            is Boolean -> if (value) BigDecimal.ONE else BigDecimal.ZERO
            else -> value?.toString()?.toBigDecimal()
        }
    }

    private fun getBigInteger(name: String): BigInteger? {
        return when (val value = getColumnValue(name)) {
            is BigInteger -> value
            is Boolean -> if (value) BigInteger.ONE else BigInteger.ZERO
            else -> value?.toString()?.toBigInteger()
        }
    }

    private fun getByteBuffer(name: String): ByteBuffer? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is ByteBuffer -> value
            is ByteArray -> ByteBuffer.wrap(value)
            is Blob -> runBlocking { value.toByteBuffer() } // Won't block if data was pre-fetched by CachedPublisher.
            else -> throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to ByteBuffer.")
        }
    }

    private fun getBytes(name: String): ByteArray? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is ByteArray -> value
            is ByteBuffer -> value.toBytes()
            is Blob -> runBlocking { value.toBytes() } // Won't block if data was pre-fetched by CachedPublisher.
            else -> throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to byte[].")
        }
    }

    private fun getBlob(name: String): Blob? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is Blob -> value
            is ByteBuffer -> Blob.from(IterableAsPublisher(value))
            is ByteArray -> Blob.from(IterableAsPublisher(ByteBuffer.wrap(value)))
            else -> throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to Blob.")
        }
    }

    private fun getDate(name: String): LocalDate? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is LocalDate -> value
            is LocalDateTime -> value.toLocalDate()
            is ZonedDateTime -> value.toLocalDate()
            is OffsetDateTime -> value.toLocalDate()
            is Instant -> value.atZone(ZoneId.systemDefault()).toLocalDate()
            is Number -> Instant.ofEpochMilli(value.toLong()).atZone(ZoneId.systemDefault()).toLocalDate()
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number).atZone(ZoneId.systemDefault()).toLocalDate()
                } else {
                    LocalDate.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalDate.")
            }
        }
    }

    private fun getTime(name: String): LocalTime? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is LocalTime -> value
            is LocalDateTime -> value.toLocalTime()
            is ZonedDateTime -> value.toLocalTime()
            is OffsetDateTime -> value.toLocalTime()
            is Instant -> value.atZone(ZoneId.systemDefault()).toLocalTime()
            is Number -> Instant.ofEpochMilli(value.toLong()).atZone(ZoneId.systemDefault()).toLocalTime()
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number).atZone(ZoneId.systemDefault()).toLocalTime()
                } else {
                    LocalTime.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalTime.")
            }
        }
    }

    private fun getDateTime(name: String): LocalDateTime? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is LocalDateTime -> value
            is LocalDate -> value.atStartOfDay()
            is ZonedDateTime -> value.toLocalDateTime()
            is OffsetDateTime -> value.toLocalDateTime()
            is Instant -> value.atZone(ZoneId.systemDefault()).toLocalDateTime()
            is Number -> Instant.ofEpochMilli(value.toLong()).atZone(ZoneId.systemDefault()).toLocalDateTime()
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number).atZone(ZoneId.systemDefault()).toLocalDateTime()
                } else {
                    LocalDateTime.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalDateTime.")
            }
        }
    }

    private fun getZonedDateTime(name: String): ZonedDateTime? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is ZonedDateTime -> value
            is LocalDate -> value.atStartOfDay(ZoneId.systemDefault())
            is LocalDateTime -> value.atZone(ZoneId.systemDefault())
            is OffsetDateTime -> value.toZonedDateTime()
            is Instant -> value.atZone(ZoneId.systemDefault())
            is Number -> Instant.ofEpochMilli(value.toLong()).atZone(ZoneId.systemDefault())
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number).atZone(ZoneId.systemDefault())
                } else {
                    ZonedDateTime.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalDateTime.")
            }
        }
    }

    private fun getOffsetDateTime(name: String): OffsetDateTime? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is OffsetDateTime -> value
            is LocalDate -> value.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime()
            is LocalDateTime -> value.atZone(ZoneId.systemDefault()).toOffsetDateTime()
            is ZonedDateTime -> value.toOffsetDateTime()
            is Instant -> value.atZone(ZoneId.systemDefault()).toOffsetDateTime()
            is Number -> Instant.ofEpochMilli(value.toLong()).atZone(ZoneId.systemDefault()).toOffsetDateTime()
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number).atZone(ZoneId.systemDefault()).toOffsetDateTime()
                } else {
                    OffsetDateTime.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalDateTime.")
            }
        }
    }

    private fun getInstant(name: String): Instant? {
        return when (val value = getColumnValue(name)) {
            null -> null
            is Instant -> value
            is LocalDate -> value.atStartOfDay(ZoneId.systemDefault()).toInstant()
            is LocalDateTime -> value.atZone(ZoneId.systemDefault()).toInstant()
            is ZonedDateTime -> value.toInstant()
            is OffsetDateTime -> value.toInstant()
            is Number -> Instant.ofEpochMilli(value.toLong())
            is String -> {
                val number = value.toLongOrNull()
                if (number != null) {
                    Instant.ofEpochMilli(number)
                } else {
                    Instant.parse(value)
                }
            }
            else -> {
                throw IllegalArgumentException("Cannot convert ${value.javaClass.name} value to LocalDateTime.")
            }
        }
    }
}
