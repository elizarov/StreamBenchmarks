package benchmark

import org.junit.Test
import org.openjdk.jmh.annotations.Benchmark
import kotlin.test.assertEquals

class RangeFilterSumTest {
    @Test
    fun testConsistentResults() {
        val b = RangeFilterSumBenchmark()
        val expected = b.testBaselineLoop()
        var passed = 0
        var failed = 0
        for (m in b::class.java.methods) {
            if (m.getAnnotation(Benchmark::class.java) != null) {
                print("${m.name} = ")
                val result = m.invoke(b) as Int
                println("$result ${if (result == expected) "[OK]" else "!!!FAILED!!!"}")
                if (result == expected) passed++ else failed++
            }
        }
        println("PASSED: $passed")
        println("FAILED: $failed")
        assertEquals(0, failed)
    }
}