plugins {
    id("com.projectronin.interop.gradle.junit")
    id("com.projectronin.interop.gradle.spring")
    id("com.projectronin.interop.gradle.integration")
    id("com.projectronin.interop.gradle.publish")
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
