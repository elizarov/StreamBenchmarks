package sourceCrossinline

import kotlinx.coroutines.experimental.DefaultDispatcher
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.launch
import kotlin.coroutines.experimental.CoroutineContext

// -------------- Interface definitions

interface SourceCrossinline<out E> {
    suspend fun consume(sink: Sink<E>)
    companion object Factory
}

interface Sink<in E> {
    suspend fun send(item: E)
    fun close(cause: Throwable?)
}

// -------------- Factory (initial/producing) operations

inline fun <E> source(crossinline action: suspend Sink<E>.() -> Unit): SourceCrossinline<E> = object : SourceCrossinline<E> {
    override suspend fun consume(sink: Sink<E>) {
        var cause: Throwable? = null
        try {
            action(sink)
        } catch (e: Throwable) {
            cause = e
        }
        sink.close(cause)
    }
}

fun SourceCrossinline.Factory.range(start: Int, count: Int): SourceCrossinline<Int> = source<Int> {
    for (i in start until (start + count)) {
        send(i)
    }
}

// -------------- Terminal (final/consuming) operations

suspend inline fun <E> SourceCrossinline<E>.consumeEach(crossinline action: suspend (E) -> Unit) {
    consume(object : Sink<E> {
        override suspend fun send(item: E) = action(item)
        override fun close(cause: Throwable?) { cause?.let { throw it } }
    })
}

suspend inline fun <E, R> SourceCrossinline<E>.fold(initial: R, crossinline operation: suspend (acc: R, E) -> R): R {
    var acc = initial
    consumeEach {
        acc = operation(acc, it)
    }
    return acc
}

// -------------- Intermediate (transforming) operations

inline fun <E> SourceCrossinline<E>.filter(crossinline predicate: (E) -> Boolean) = source<E> {
    consumeEach {
        if (predicate(it)) send(it)
    }
}

fun <E> SourceCrossinline<E>.async(context: CoroutineContext = DefaultDispatcher, buffer: Int = 0): SourceCrossinline<E> {
    val channel = Channel<E>(buffer)
    return object : SourceCrossinline<E> {
        override suspend fun consume(sink: Sink<E>) {
            launch(context) {
                channel.consumeEach { sink.send(it) }
            }
            this@async.consumeEach { channel.send(it) }
        }
    }
}
