package org.ktorm.r2dbc.database

import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import kotlin.coroutines.coroutineContext

/**
 * Created by vince on Jan 31, 2021.
 */
public class CoroutineLocal<T : Any> {
    private val mutex = Mutex()
    private val locals = IdentityHashMap<Job, Entry<T>>()

    private class Entry<T : Any>(var value: T, val disposable: DisposableHandle) {
        protected fun finalize() {
            disposable.dispose()
        }
    }

    public suspend fun get(): T? {
        mutex.withLock {
            val job = coroutineContext[Job] ?: error("Coroutine Job doesn't exist in the current context.")
            return locals[job]?.value
        }
    }

    public suspend fun set(value: T) {
        mutex.withLock {
            val job = coroutineContext[Job] ?: error("Coroutine Job doesn't exist in the current context.")

            val entry = locals[job]
            if (entry != null) {
                entry.value = value
            } else {
                locals[job] = Entry(value, job.invokeOnCompletion { locals.remove(job) })
            }
        }
    }

    public suspend fun remove() {
        mutex.withLock {
            val job = coroutineContext[Job] ?: error("Coroutine Job doesn't exist in the current context.")
            val existing = locals.remove(job)
            existing?.disposable?.dispose()
        }
    }
}
