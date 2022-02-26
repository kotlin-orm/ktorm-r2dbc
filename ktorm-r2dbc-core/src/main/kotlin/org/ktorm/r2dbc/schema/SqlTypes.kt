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

package org.ktorm.r2dbc.schema

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.*
import java.util.*

/**
 * Define a column typed of [BooleanSqlType].
 */
public fun BaseTable<*>.boolean(name: String): Column<Boolean> {
    return registerColumn(name, BooleanSqlType)
}

/**
 * [SqlType] implementation represents `boolean` SQL type.
 */
public object BooleanSqlType : SimpleSqlType<Boolean>(Boolean::class)

/**
 * Define a column typed of [IntSqlType].
 */
public fun BaseTable<*>.int(name: String): Column<Int> {
    return registerColumn(name, IntSqlType)
}

/**
 * [SqlType] implementation represents `int` SQL type.
 */
public object IntSqlType : ConvertibleSqlType<Int>(Int::class) {
    override fun convert(value: Any): Int {
        return when (value) {
            is Number -> value.toInt()
            is String -> value.toInt()
            else -> throw IllegalStateException("Converting type is not supported from value:$value")
        }
    }
}

/**
 * Define a column typed of [ShortSqlType].
 *
 * @since 3.1.0
 */
public fun BaseTable<*>.short(name: String): Column<Short> {
    return registerColumn(name, ShortSqlType)
}

/**
 * [SqlType] implementation represents `smallint` SQL type.
 *
 * @since 3.1.0
 */
public object ShortSqlType : SimpleSqlType<Short>(Short::class)

/**
 * Define a column typed of [LongSqlType].
 */
public fun BaseTable<*>.long(name: String): Column<Long> {
    return registerColumn(name, LongSqlType)
}

/**
 * [SqlType] implementation represents `long` SQL type.
 */
public object LongSqlType : ConvertibleSqlType<Long>(Long::class) {
    override fun convert(value: Any): Long {
        return when (value) {
            is Number -> value.toLong()
            is String -> value.toLong()
            else -> throw IllegalStateException("Converting type is not supported from value:$value")
        }
    }
}

/**
 * Define a column typed of [FloatSqlType].
 */
public fun BaseTable<*>.float(name: String): Column<Float> {
    return registerColumn(name, FloatSqlType)
}

/**
 * [SqlType] implementation represents `float` SQL type.
 */
public object FloatSqlType : ConvertibleSqlType<Float>(Float::class) {
    override fun convert(value: Any): Float {
        return when (value) {
            is Number -> value.toFloat()
            is String -> value.toFloat()
            else -> throw IllegalStateException("Converting type is not supported from value:$value")
        }
    }
}

/**
 * Define a column typed of [DoubleSqlType].
 */
public fun BaseTable<*>.double(name: String): Column<Double> {
    return registerColumn(name, DoubleSqlType)
}

/**
 * [SqlType] implementation represents `double` SQL type.
 */
public object DoubleSqlType : ConvertibleSqlType<Double>(Double::class) {
    override fun convert(value: Any): Double {
        return when (value) {
            is Number -> value.toDouble()
            is String -> value.toDouble()
            else -> throw IllegalStateException("Converting type is not supported from value:$value")
        }
    }
}

/**
 * Define a column typed of [DecimalSqlType].
 */
public fun BaseTable<*>.decimal(name: String): Column<BigDecimal> {
    return registerColumn(name, DecimalSqlType)
}

/**
 * [SqlType] implementation represents `decimal` SQL type.
 */
public object DecimalSqlType : ConvertibleSqlType<BigDecimal>(BigDecimal::class) {
    override fun convert(value: Any): BigDecimal {
        return when (value) {
            is BigDecimal -> value
            is Int -> BigDecimal(value)
            is Long -> BigDecimal(value)
            is Double -> BigDecimal(value)
            is Float -> BigDecimal(value.toDouble())
            is String -> BigDecimal(value)
            else -> throw IllegalStateException("Converting type is not supported from value:$value")
        }
    }

}

/**
 * Define a column typed of [VarcharSqlType].
 */
public fun BaseTable<*>.varchar(name: String): Column<String> {
    return registerColumn(name, VarcharSqlType)
}

/**
 * [SqlType] implementation represents `varchar` SQL type.
 */
public object VarcharSqlType : SimpleSqlType<String>(String::class)

/**
 * Define a column typed of [TextSqlType].
 */
public fun BaseTable<*>.text(name: String): Column<String> {
    return registerColumn(name, TextSqlType)
}

/**
 * [SqlType] implementation represents `text` SQL type.
 */
public object TextSqlType : SimpleSqlType<String>(String::class)

/**
 * Define a column typed of [BlobSqlType].
 */
public fun BaseTable<*>.blob(name: String): Column<ByteArray> {
    return registerColumn(name, BlobSqlType)
}

/**
 * [SqlType] implementation represents `blob` SQL type.
 */
public object BlobSqlType : SimpleSqlType<ByteArray>(ByteArray::class)

/**
 * Define a column typed of [BytesSqlType].
 */
public fun BaseTable<*>.bytes(name: String): Column<ByteArray> {
    return registerColumn(name, BytesSqlType)
}

/**
 * [SqlType] implementation represents `bytes` SQL type.
 */
public object BytesSqlType : SimpleSqlType<ByteArray>(ByteArray::class)

/**
 * Define a column typed of [TimestampSqlType].
 */
public fun BaseTable<*>.jdbcTimestamp(name: String): Column<Timestamp> {
    return registerColumn(name, TimestampSqlType)
}

/**
 * [SqlType] implementation represents `timestamp` SQL type.
 */
public object TimestampSqlType : SimpleSqlType<Timestamp>(Timestamp::class)

/**
 * Define a column typed of [InstantSqlType].
 */
public fun BaseTable<*>.timestamp(name: String): Column<Instant> {
    return registerColumn(name, InstantSqlType)
}

/**
 * [SqlType] implementation represents `timestamp` SQL type.
 */
public object InstantSqlType : SimpleSqlType<Instant>(Instant::class)

/**
 * Define a column typed of [LocalDateTimeSqlType].
 */
public fun BaseTable<*>.datetime(name: String): Column<LocalDateTime> {
    return registerColumn(name, LocalDateTimeSqlType)
}

/**
 * [SqlType] implementation represents `datetime` SQL type.
 */
public object LocalDateTimeSqlType : SimpleSqlType<LocalDateTime>(LocalDateTime::class)

/**
 * Define a column typed of [LocalDateSqlType].
 */
public fun BaseTable<*>.date(name: String): Column<LocalDate> {
    return registerColumn(name, LocalDateSqlType)
}

/**
 * [SqlType] implementation represents `date` SQL type.
 */
public object LocalDateSqlType : SimpleSqlType<LocalDate>(LocalDate::class)

/**
 * Define a column typed of [LocalTimeSqlType].
 */
public fun BaseTable<*>.time(name: String): Column<LocalTime> {
    return registerColumn(name, LocalTimeSqlType)
}

/**
 * [SqlType] implementation represents `time` SQL type.
 */
public object LocalTimeSqlType : SimpleSqlType<LocalTime>(LocalTime::class)

/**
 * Define a column typed of [MonthDaySqlType], instances of [MonthDay] are saved as strings in format `MM-dd`.
 */
public fun BaseTable<*>.monthDay(name: String): Column<MonthDay> {
    return registerColumn(name, MonthDaySqlType)
}

/**
 * [SqlType] implementation used to save [MonthDay] instances, formating them to strings with pattern `MM-dd`.
 */
public object MonthDaySqlType : SimpleSqlType<MonthDay>(MonthDay::class)

/**
 * Define a column typed of [YearMonthSqlType], instances of [YearMonth] are saved as strings in format `yyyy-MM`.
 */
public fun BaseTable<*>.yearMonth(name: String): Column<YearMonth> {
    return registerColumn(name, YearMonthSqlType)
}

/**
 * [SqlType] implementation used to save [YearMonth] instances, formating them to strings with pattern `yyyy-MM`.
 */
@Suppress("MagicNumber")
public object YearMonthSqlType : SimpleSqlType<YearMonth>(YearMonth::class)

/**
 * Define a column typed of [YearSqlType], instances of [Year] are saved as integers.
 */
public fun BaseTable<*>.year(name: String): Column<Year> {
    return registerColumn(name, YearSqlType)
}

/**
 * [SqlType] implementation used to save [Year] instances as integers.
 */
public object YearSqlType : SimpleSqlType<Year>(Year::class)

/**
 * Define a column typed of [UuidSqlType].
 */
public fun BaseTable<*>.uuid(name: String): Column<UUID> {
    return registerColumn(name, UuidSqlType)
}

/**
 * [SqlType] implementation represents `uuid` SQL type.
 */
public object UuidSqlType : SimpleSqlType<UUID>(UUID::class)
