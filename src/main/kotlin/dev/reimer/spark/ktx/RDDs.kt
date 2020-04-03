package dev.reimer.spark.ktx

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.api.java.*
import org.apache.spark.rdd.RDD
import scala.Tuple2
import scala.reflect.ClassTag
import java.io.File

fun List<Double>.toRdd(context: JavaSparkContext, numSlices: Int = context.defaultParallelism): JavaDoubleRDD =
    context.parallelizeDoubles(this, numSlices)

fun Iterable<Double>.toRdd(context: JavaSparkContext, numSlices: Int = context.defaultParallelism): JavaDoubleRDD =
    toList().toRdd(context, numSlices)

fun <T> List<T>.toRdd(context: JavaSparkContext, numSlices: Int = context.defaultParallelism): JavaRDD<T> =
    context.parallelize(this, numSlices)

fun <T> Iterable<T>.toRdd(context: JavaSparkContext, numSlices: Int = context.defaultParallelism): JavaRDD<T> =
    toList().toRdd(context, numSlices)

fun <K, V> List<Tuple2<K, V>>.toRdd(
    context: JavaSparkContext,
    numSlices: Int = context.defaultParallelism
): JavaPairRDD<K, V> =
    context.parallelizePairs(this, numSlices)

fun <K, V> Iterable<Tuple2<K, V>>.toRdd(
    context: JavaSparkContext,
    numSlices: Int = context.defaultParallelism
): JavaPairRDD<K, V> =
    toList().toRdd(context, numSlices)

fun <K, V> Map<K, V>.toRdd(context: JavaSparkContext, numSlices: Int = context.defaultParallelism): JavaPairRDD<K, V> =
    map { it.key tuple it.value }.toRdd(context, numSlices)

fun <T> JavaSparkContext.rddOf(items: Iterable<T>, numSlices: Int = defaultParallelism): JavaRDD<T> =
    parallelize(items.toList(), numSlices)

fun <T> JavaSparkContext.rddOf(vararg items: T, numSlices: Int = defaultParallelism): JavaRDD<T> =
    parallelize(items.toList(), numSlices)

fun JavaSparkContext.rddOf(vararg items: Double, numSlices: Int = defaultParallelism): JavaDoubleRDD =
    parallelizeDoubles(items.toList(), numSlices)

fun <K, V> JavaSparkContext.rddOf(vararg items: Tuple2<K, V>, numSlices: Int = defaultParallelism): JavaPairRDD<K, V> =
    parallelizePairs(items.toList(), numSlices)

fun <K, V> JavaSparkContext.rddOf(vararg items: Pair<K, V>, numSlices: Int = defaultParallelism): JavaPairRDD<K, V> =
    parallelizePairs(items.map { it.toTuple2() }, numSlices)

val <T> JavaRDD<T>.rdd: RDD<T> get() = rdd()

val <T> JavaRDD<T>.classTag: ClassTag<T> get() = classTag()

inline fun <T> JavaRDD<T>.filter(crossinline predicate: (T) -> Boolean): JavaRDD<T> = filter { predicate(it) }

operator fun <T> JavaRDD<T>.plus(other: JavaRDD<T>): JavaRDD<T> = union(other)

operator fun <T> JavaRDD<T>.times(other: JavaRDD<T>): JavaRDD<T> = intersection(other)

operator fun <T> JavaRDD<T>.minus(other: JavaRDD<T>): JavaRDD<T> = subtract(other)

var <T> JavaRDD<T>.name: String
    get() = name()
    set(value) {
        setName(value)
    }

inline fun <T, R : Comparable<R>> JavaRDD<T>.sortBy(
    ascending: Boolean,
    numPartitions: Int,
    crossinline selector: (T) -> R?
): JavaRDD<T> = sortBy({ selector(it) }, ascending, numPartitions)

fun <T, This : JavaRDDLike<T, This>> JavaRDDLike<T, This>.saveAsTextFile(path: File) = saveAsTextFile(path.path)

fun <T, This : JavaRDDLike<T, This>> JavaRDDLike<T, This>.saveAsTextFile(
    path: File,
    codec: Class<out CompressionCodec>
) = saveAsTextFile(path.path, codec)

fun <T, This : JavaRDDLike<T, This>> JavaRDDLike<T, This>.toList(): List<T> = collect()

fun <T, This : JavaRDDLike<T, This>> JavaRDDLike<T, This>.toList(vararg partitions: Int): Array<List<T>> =
    collectPartitions(partitions)

fun <K, V> JavaPairRDD<K, V>.toMap(): Map<K, V> = collectAsMap()

fun <K, V> JavaPairRDD<K, V>.toTupleList(): List<Tuple2<K, V>> = collect()

fun <K, V> JavaPairRDD<K, V>.toList(): List<Pair<K, V>> = toTupleList().map { it.toPair() }

fun <K, V> JavaPairRDD<K, V>.reduceCountByKey(): JavaPairRDD<K, Long> =
    mapValues { 1L }.reduceByKey(Long::plus)

fun <K, VK, VV> JavaPairRDD<K, Tuple2<VK, VV>>.valuesToPair(): JavaPairRDD<VK, VV> =
    mapToPair { it.second }
