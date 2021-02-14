package org.ktorm.r2dbc.database

import io.r2dbc.spi.Blob
import io.r2dbc.spi.Clob
import kotlinx.coroutines.reactive.collect
import org.reactivestreams.Publisher
import java.nio.ByteBuffer

public suspend fun <T : Any> Publisher<T>.toList(): List<T> {
    val list = ArrayList<T>()
    this.collect { list += it }
    return list
}

public suspend fun Blob.toByteBuffer(): ByteBuffer {
    val buffers = stream().toList()
    val result = ByteBuffer.allocate(buffers.sumOf { it.remaining() })

    for (buffer in buffers) {
        result.put(buffer)
    }

    result.flip()
    return result
}

public suspend fun Blob.toBytes(): ByteArray {
    val buffers = stream().toList()
    val result = ByteArray(buffers.sumOf { it.remaining() })

    var offset = 0
    for (buffer in buffers) {
        val length = buffer.remaining()
        buffer.get(result, offset, length)
        offset += length
    }

    return result
}

public fun ByteBuffer.toBytes(): ByteArray {
    val result = ByteArray(remaining())
    get(result, 0, result.size)
    return result
}

public suspend fun Clob.toStringBuilder(): StringBuilder {
    val buffers = stream().toList()
    val result = StringBuilder(buffers.sumOf { it.length })

    for (buffer in buffers) {
        @Suppress("BlockingMethodInNonBlockingContext")
        result.append(buffer)
    }

    return result
}

public suspend fun Clob.toText(): String {
    return toStringBuilder().toString()
}
