[![GitHub Actions](https://img.shields.io/github/workflow/status/reimersoftware/spark-ktx/CI?style=flat-square)](https://github.com/reimersoftware/spark-ktx/actions?query=workflow%3A"CI")
[![JitPack](https://img.shields.io/jitpack/v/github/reimersoftware/spark-ktx?style=flat-square)](https://jitpack.io/#dev.reimer/spark-ktx)

# 💾 spark-ktx<sup>[α](#status-α)</sup>

Kotlin extensions for [Apache Spark](https://spark.apache.org/) (unofficial).

## Gradle Dependency

This library is available on [**jitpack.io**](https://jitpack.io/#dev.reimer/spark-ktx).  
Add this in your `build.gradle.kts` or `build.gradle` file:

<details open><summary>Kotlin</summary>

```kotlin
repositories {
    maven("https://jitpack.io")
}

dependencies {
    implementation("dev.reimer:spark-ktx:<version>")
}
```

</details>

<details><summary>Groovy</summary>

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'dev.reimer:spark-ktx:<version>'
}
```

</details>

## Status α

⚠️ _Warning:_ This project is in an experimental alpha stage:
- The API may be changed at any time without further notice.
- Development still happens on `master`.
- Pull Requests are highly appreciated!