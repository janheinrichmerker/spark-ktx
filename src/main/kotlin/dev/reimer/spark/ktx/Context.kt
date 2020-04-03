package dev.reimer.spark.ktx

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun SparkConf.context(configuration: JavaSparkContext.() -> Unit = {}) =
    JavaSparkContext(this).use(configuration)

val JavaSparkContext.defaultParallelism: Int
    get() = defaultParallelism()

val JavaSparkContext.defaultMinPartitions: Int
    get() = defaultMinPartitions()

val JavaSparkContext.hadoopConfiguration: Configuration
    get() = hadoopConfiguration()

inline fun <reified K, reified V> JavaSparkContext.sequenceFile(
    path: String,
    minPartitions: Int
) =
    sequenceFile(path, K::class.java, V::class.java, minPartitions)

inline fun <reified K, reified V> JavaSparkContext.sequenceFile(
    path: String
) = sequenceFile(path, K::class.java, V::class.java)

inline fun <reified K, reified V, reified F : InputFormat<K, V>> JavaSparkContext.hadoopRDD(
    conf: JobConf,
    minPartitions: Int
) = hadoopRDD(conf, F::class.java, K::class.java, V::class.java, minPartitions)

inline fun <reified K, reified V, reified F : InputFormat<K, V>> JavaSparkContext.hadoopRDD(
    conf: JobConf
) = hadoopRDD(conf, F::class.java, K::class.java, V::class.java)

inline fun <reified K, reified V, reified F : InputFormat<K, V>> JavaSparkContext.hadoopFile(
    path: String,
    minPartitions: Int
) = hadoopFile(path, F::class.java, K::class.java, V::class.java, minPartitions)

inline fun <reified K, reified V, reified F : InputFormat<K, V>> JavaSparkContext.hadoopFile(
    path: String
) = hadoopFile(path, F::class.java, K::class.java, V::class.java)

inline fun <reified K, reified V, reified F : org.apache.hadoop.mapreduce.InputFormat<K, V>> JavaSparkContext.newAPIHadoopFile(
    path: String,
    conf: Configuration = hadoopConfiguration
) = newAPIHadoopFile(path, F::class.java, K::class.java, V::class.java, conf)

inline fun <reified K, reified V, reified F : org.apache.hadoop.mapreduce.InputFormat<K, V>> JavaSparkContext.newAPIHadoopRDD(
    conf: Configuration = hadoopConfiguration
) = newAPIHadoopRDD(conf, F::class.java, K::class.java, V::class.java)
