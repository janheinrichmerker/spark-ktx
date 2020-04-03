package dev.reimer.spark.ktx

import scala.Tuple2

infix fun <K, V> K.tuple(value: V): Tuple2<K, V> = Tuple2(this, value)

fun <K, V> Pair<K, V>.toTuple2(): Tuple2<K, V> = first tuple second

fun <K, V> Tuple2<K, V>.toPair(): Pair<K, V> = first to second

inline val <T1, T2> Tuple2<T1, T2>.first: T1 get() = _1

inline val <T1, T2> Tuple2<T1, T2>.second: T2 get() = _2

operator fun <T1, T2> Tuple2<T1, T2>.component1(): T1 = first

operator fun <T1, T2> Tuple2<T1, T2>.component2(): T2 = second

fun <T1, T2> Tuple2<T1, T2>.flip(): Tuple2<T2, T1> = second tuple first
