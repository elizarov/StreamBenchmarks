package benchmark

import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.schedulers.Schedulers
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.filter
import kotlinx.coroutines.experimental.channels.fold
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.reactive.consumeEach
import kotlinx.coroutines.experimental.reactive.publish
import kotlinx.coroutines.experimental.reactor.flux
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.rx2.consumeEach
import kotlinx.coroutines.experimental.rx2.rxFlowable
import kotlinx.coroutines.experimental.rx2.rxObservable
import org.openjdk.jmh.annotations.*
import reactor.core.publisher.Flux
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.stream.Stream
import kotlin.coroutines.experimental.buildSequence

fun Int.isGood() = this % 4 == 0

fun Channel.Factory.range(start: Int, count: Int) = produce<Int> {
    for (i in start until (start + count))
        send(i)
}

fun publishRange(start: Int, count: Int) = publish<Int> {
    for (i in start until (start + count))
        send(i)
}

fun generateSequenceRange(start: Int, count: Int): Sequence<Int> {
    var cur = start
    return generateSequence {
        if (cur > start + count) {
            null
        } else {
            cur++
        }
    }
}

fun buildSequenceRange(start: Int, count: Int) = buildSequence {
    for (i in start until (start + count))
        yield(i)
}

fun <T> Observable<T>.coroFilter(predicate: (T) -> Boolean) = rxObservable {
    consumeEach {
        if (predicate(it)) send(it)
    }
}

fun <T> Flowable<T>.coroFilter(predicate: (T) -> Boolean) = rxFlowable {
    consumeEach {
        if (predicate(it)) send(it)
    }
}

fun <T> Flux<T>.coroFilter(predicate: (T) -> Boolean) = flux {
    consumeEach {
        if (predicate(it)) send(it)
    }
}

data class IntBox(var v: Int)

const val N = 1_000_000

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
open class RangeFilterSumBenchmark {
    @Benchmark
    fun testJavaStream(): Int =
        Stream
            .iterate(1) { it + 1 }
            .limit(N.toLong())
            .filter { it.isGood() }
            .collect(Collectors.summingInt { it })

    @Benchmark
    fun testGenerateSequence(): Int =
        generateSequenceRange(1, N)
            .filter { it.isGood() }
            .fold(0, { a, b -> a + b })

    @Benchmark
    fun testBuildSequence(): Int =
        buildSequenceRange(1, N)
            .filter { it.isGood() }
            .fold(0, { a, b -> a + b })

    @Benchmark
    fun testObservable(): Int =
        Observable
            .range(1, N)
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFlowable(): Int =
        Flowable
            .range(1, N)
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFlux(): Int =
        Flux
            .range(1, N)
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .block()!!.v

    @Benchmark
    fun testObservableThread(): Int =
        Observable
            .range(1, N)
            .observeOn(Schedulers.computation())
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFlowableThread(): Int =
        Flowable
            .range(1, N)
            .observeOn(Schedulers.computation())
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFluxThread(): Int =
        Flux
            .range(1, N)
            .publishOn(reactor.core.scheduler.Schedulers.single())
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .block()!!.v


    @Benchmark
    fun testObservableFromCoroPublish(): Int =
        Observable
            .fromPublisher(publishRange(1, N))
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFlowableFromCoroPublish(): Int =
        Flowable
            .fromPublisher(publishRange(1, N))
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFluxFromCoroPublish(): Int =
        Flux
            .from(publishRange(1, N))
            .filter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .block()!!.v

    @Benchmark
    fun testObservableWithCoroFilter(): Int =
        Observable
            .range(1, N)
            .coroFilter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFlowableWithCoroFilter(): Int =
        Flowable
            .range(1, N)
            .coroFilter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .blockingGet().v

    @Benchmark
    fun testFluxWithCoroFilter(): Int =
        Flux
            .range(1, N)
            .coroFilter { it.isGood() }
            .collect({ IntBox(0) }, { b, x -> b.v += x })
            .block()!!.v

    @Benchmark
    fun testChannelPipeline(): Int = runBlocking {
        Channel
            .range(1, N)
            .filter { it.isGood() }
            .fold(0, { a, b -> a + b })
    }
}

// sanity test that everything work
fun main(args: Array<String>) {
    val b = RangeFilterSumBenchmark()
    for (m in b::class.java.methods) {
        if (m.getAnnotation(Benchmark::class.java) != null) {
            print("${m.name} = ")
            println(m.invoke(b))
        }
    }
}