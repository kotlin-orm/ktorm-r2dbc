package org.ktorm.r2dbc.database

import io.r2dbc.spi.Blob
import io.r2dbc.spi.Clob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.sync.Semaphore
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

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

public suspend fun Clob.toText(): String {
    val buffers = stream().toList()
    val result = StringBuilder(buffers.sumOf { it.length })

    for (buffer in buffers) {
        @Suppress("BlockingMethodInNonBlockingContext")
        result.append(buffer)
    }

    return result.toString()
}

internal class CachedPublisher<T : Any>(source: Publisher<T>) : Publisher<T> {
    private val upstreamCachedElements = LinkedBlockingQueue<T>()
    private val upstreamCompletableFuture = CompletableFuture<Unit>()
    private val downstreamSubscribed = AtomicBoolean()
    private val downstreamRequestedCount = AtomicLong()
    private val downstreamStarted = AtomicBoolean()
    private val downstreamFinished = AtomicBoolean()

    init {
        // Pre-fetch the data.
        source.subscribe(object : Subscriber<T> {
            override fun onSubscribe(s: Subscription) {
                s.request(Long.MAX_VALUE)
            }

            override fun onNext(t: T) {
                upstreamCachedElements.offer(t)
            }

            override fun onError(t: Throwable) {
                upstreamCompletableFuture.completeExceptionally(t)
            }

            override fun onComplete() {
                upstreamCompletableFuture.complete(Unit)
            }
        })
    }

    override fun subscribe(s: Subscriber<in T>) {
        if (downstreamSubscribed.getAndSet(true)) {
            s.onSubscribe(EmptySubscription)
            s.onError(IllegalStateException("CachedPublisher can only be subscribed once."))
        } else {
            s.onSubscribe(Downstream(s))
        }
    }

    private inner class Downstream(val subscriber: Subscriber<in T>) : Subscription {

        override fun request(n: Long) {
            if (n > 0) {
                downstreamRequestedCount.addAndGet(n)
                consumeElements()
            }
        }

        private fun consumeElements() {
            if (downstreamStarted.getAndSet(true)) {
                return
            }

            while (!downstreamFinished.get()) {
                try {
                    pollNext()
                    checkCompletion()
                } catch (e: Throwable) {
                    downstreamFinished.set(true)
                    subscriber.onError(e)
                }
            }
        }

        private fun pollNext() {
            if (downstreamRequestedCount.get() > 0) {
                val next = upstreamCachedElements.poll() // busy wait...
                if (next != null) {
                    subscriber.onNext(next)
                    downstreamRequestedCount.decrementAndGet()
                }
            }
        }

        private fun checkCompletion() {
            if (upstreamCompletableFuture.isDone && upstreamCachedElements.isEmpty()) {
                downstreamFinished.set(true)

                try {
                    upstreamCompletableFuture.get()
                    subscriber.onComplete()
                } catch (e: ExecutionException) {
                    subscriber.onError(e.cause)
                }
            }
        }

        override fun cancel() {
            downstreamFinished.set(true)
        }
    }
}

internal class IterableAsPublisher<T : Any>(private val iterable: Iterable<T>) : Publisher<T> {

    constructor() : this(emptyList())

    constructor(element: T) : this(listOf(element))

    constructor(vararg elements: T) : this(elements.asList())

    override fun subscribe(s: Subscriber<in T>) {
        val iterator = iterable.iterator()
        if (iterator.hasNext()) {
            s.onSubscribe(IterableSubscription(s, iterator))
        } else {
            s.onSubscribe(EmptySubscription)
            s.onComplete()
        }
    }

    private class IterableSubscription<T : Any>(
        private val subscriber: Subscriber<in T>,
        private val iterator: Iterator<T>
    ) : Subscription {
        private val requested = AtomicLong()
        private val finished = AtomicBoolean()

        override fun request(n: Long) {
            if (n > 0) {
                if (requested.getAndAdd(n) == 0L) {
                    consumeElements()
                }
            }
        }

        private fun consumeElements() {
            while (!finished.get()) {
                subscriber.onNext(iterator.next())

                if (!iterator.hasNext()) {
                    finished.set(true)
                    subscriber.onComplete()
                }

                if (requested.decrementAndGet() == 0L) {
                    break
                }
            }
        }

        override fun cancel() {
            finished.set(true)
        }
    }
}

internal object EmptySubscription : Subscription {

    override fun request(n: Long) {
        // no-op
    }

    override fun cancel() {
        // no-op
    }
}
