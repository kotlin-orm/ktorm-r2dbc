package org.ktorm.r2dbc.database

import io.r2dbc.spi.Blob
import io.r2dbc.spi.Clob
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

internal class CachedLobImpl<T : Any>(source: Publisher<T>) {
    private val upstreamCachedElements = LinkedBlockingQueue<T>()
    private val upstreamCompletableFuture = CompletableFuture<Unit>()
    private val downstreamSubscribed = AtomicBoolean()
    private val downstreamRequestedCount = AtomicLong()
    private val downstreamStarted = AtomicBoolean()
    private val downstreamFinished = AtomicBoolean()

    init {
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

    fun stream() = Publisher<T> { subscriber ->
        if (downstreamSubscribed.getAndSet(true)) {
            subscriber.onError(IllegalStateException("CachedPublisher can only be subscribed once."))
        } else {
            subscriber.onSubscribe(Downstream(subscriber))
        }
    }

    fun discard() = Publisher<Void> { subscriber ->
        val subscription = object : AtomicBoolean(), Subscription {
            override fun request(n: Long) {
                // no-op
            }

            override fun cancel() {
                set(true)
            }

            fun isCancelled(): Boolean {
                return get()
            }
        }

        subscriber.onSubscribe(subscription)

        if (!subscription.isCancelled()) {
            try {
                downstreamFinished.set(true)
                subscriber.onComplete()
            } catch (e: Throwable) {
                subscriber.onError(e)
            }
        }
    }

    private inner class Downstream(val subscriber: Subscriber<in T>) : Subscription {

        override fun request(n: Long) {
            downstreamRequestedCount.addAndGet(n)
            consumeElements()
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

internal class CachedClob(source: Clob) : Clob {
    private val impl = CachedLobImpl(source.stream())

    override fun stream(): Publisher<CharSequence> {
        return impl.stream()
    }

    override fun discard(): Publisher<Void> {
        return impl.discard()
    }
}

internal class CachedBlob(source: Blob) : Blob {
    private val impl = CachedLobImpl(source.stream())

    override fun stream(): Publisher<ByteBuffer> {
        return impl.stream()
    }

    override fun discard(): Publisher<Void> {
        return impl.discard()
    }
}
