plugins {
    alias(libs.plugins.android.application) apply false
    // AGP 9 has Kotlin built in and ships with Kotlin Gradle plugin 2.2.10.
    // Declaring a Kotlin plugin here, without applying it, raises that to the catalog's version.
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.ksp) apply false
    alias(libs.plugins.kotlin.compose) apply false
}
