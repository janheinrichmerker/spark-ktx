package dev.reimer.spark.ktx

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

fun spark(loadDefaults: Boolean = true, configuration: SparkConf.() -> Unit = {}) =
    SparkConf(loadDefaults).apply(configuration)


val Configuration.fileSystem: FileSystem
    get() = FileSystem.get(this)

fun setSparkLogLevel(level: Level) {
    Logger.getLogger("org").level = level
    Logger.getLogger("akka").level = level
    LogManager.getRootLogger().level = level
}

val SparkConf.hasAppName: Boolean
    get() = contains("spark.app.name")

var SparkConf.appName: String
    get() = get("spark.app.name")
    set(value) {
        setAppName(value)
    }

val SparkConf.hasMaster: Boolean
    get() = contains("spark.master")

var SparkConf.master: String
    get() = get("spark.master")
    set(value) {
        setMaster(value)
    }

val SparkConf.hasSparkHome: Boolean
    get() = contains("spark.home")

var SparkConf.sparkHome: String
    get() = get("spark.home")
    set(value) {
        setMaster(value)
    }
