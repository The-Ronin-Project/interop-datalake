plugins {
    alias(libs.plugins.interop.gradle.junit)
    alias(libs.plugins.interop.gradle.spring)
    alias(libs.plugins.interop.gradle.integration)
    alias(libs.plugins.interop.gradle.publish)
    alias(libs.plugins.interop.version.catalog)
    alias(libs.plugins.interop.gradle.sonarqube)
}

dependencies {
    implementation(libs.interop.common)
    implementation(libs.interop.commonJackson)
    implementation(libs.interop.fhir)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.oci.common)
    implementation(libs.oci.objectstorage)
    implementation(libs.oci.http.client)

    testImplementation(libs.mockk)
    testImplementation("org.springframework:spring-test")

    itImplementation(platform(libs.testcontainers.bom))
    itImplementation("org.testcontainers:junit-jupiter")
    itImplementation(libs.mockserver.client.java)
    itImplementation(libs.oci.common)
    itImplementation(libs.interop.commonJackson)
    itImplementation(libs.oci.objectstorage)
    itImplementation(libs.oci.http.client)
    itImplementation(libs.interop.fhir)
    itImplementation(platform(libs.spring.framework.bom))
    itImplementation("org.springframework:spring-context")
    itImplementation(libs.mockk)
    itImplementation(libs.interop.common)
}
