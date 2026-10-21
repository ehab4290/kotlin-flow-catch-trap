import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
    }
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.junit)
}

application {
    // ./gradlew run --args="android"   (Main.kt and the README at the top of the repo list the other reports)
    mainClass = "catchtrap.MainKt"
}

tasks.test {
    // The checks are timing-sensitive, so always run them and show the report.
    outputs.upToDateWhen { false }
    testLogging {
        showStandardStreams = true
        events("passed", "failed")
    }
}
