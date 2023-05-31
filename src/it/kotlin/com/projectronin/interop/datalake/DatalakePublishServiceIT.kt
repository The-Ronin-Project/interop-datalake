package com.projectronin.interop.datalake

import com.oracle.bmc.Region
import com.oracle.bmc.http.internal.ParamEncoder
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.projectronin.interop.common.jackson.JacksonManager.Companion.objectMapper
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.resource.Patient
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import org.bouncycastle.util.encoders.Base64
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse
import org.mockserver.model.MediaType
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Optional
import java.util.UUID

@Testcontainers
class DatalakePublishServiceIT {
    companion object {
        @Container
        private val mockserver =
            GenericContainer("docker-proxy.devops.projectronin.io/mockserver/mockserver:5.15.0").withExposedPorts(1080)
                .waitingFor(Wait.forLogMessage(".*started on port: 1080.*", 1))

        fun getPort() = mockserver.getMappedPort(1080)

        fun getMockserverUrl() = "http://localhost:${getPort()}"
    }

    private val client = MockServerClient("localhost", getPort())

    private val namespace = "dataplatform"
    private val infxbucketName = "infx-data"
    private val dlbucketName = "datalake-data"
    private val ociTenantId = "ociTenant"
    private val ociUserId = "ociUser"
    private val fingerPrint = "fingerPrint"
    private val regionId = "us-phoenix-1"
    private val privateKey =
        java.util.Base64.getEncoder().encodeToString(
            this::class.java.getResource("/ExamplePEMPrivateKey.txt")!!.readText().replace("\\r", "")
                .replace("\\n", "")
                .toByteArray()
        )

    // We have to mock the region to force this to our localhost.
    private val region = mockk<Region> {
        every { getEndpoint(ObjectStorageClient.SERVICE) } returns Optional.of(getMockserverUrl())
    }

    private val ociClient =
        OCIClient(ociTenantId, ociUserId, fingerPrint, privateKey, namespace, infxbucketName, dlbucketName, regionId)
    private val taskExecutor = ThreadPoolTaskExecutor().apply {
        val processors = Runtime.getRuntime().availableProcessors()
        corePoolSize = processors / 2
        maxPoolSize = processors
        initialize()
    }
    private val publishService = DatalakePublishService(ociClient, taskExecutor)

    @BeforeEach
    fun setup() {
        mockkStatic(Region::fromRegionId)
        every { Region.fromRegionId("us-phoenix-1") } returns region
        client.reset()
    }

    @Test
    fun `can publish FHIR`() {
        val id = Id(UUID.randomUUID().toString())
        val patient = Patient(
            id = id
        )
        val tenant = "ronin"

        val objectName = getR4Name(tenant, "Patient", id.value!!)
        setPutExpectation(objectName)

        publishService.publishFHIRR4(tenant, listOf(patient))

        val request = getLastPut(objectName)
        request!!
        assertEquals(objectMapper.writeValueAsString(patient), decode(request.bodyAsString))
    }

    private fun getR4Name(tenantId: String, resourceType: String, resourceId: String): String {
        val date = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
        // OCI doesn't encode all characters, use their API so we don't encode '='
        val objectName = "ehr/${resourceType.lowercase()}/fhir_tenant_id=$tenantId/_date=$date/$resourceId.json"
        return ParamEncoder.encodePathParam(objectName)
    }

    private fun setPutExpectation(objectName: String) {
        client.`when`(
            request()
                .withMethod("PUT")
                .withPath(getObjectPath(objectName))
        ).respond(
            HttpResponse.response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON)
        )
    }

    private fun getLastPut(objectName: String): HttpRequest? =
        client.retrieveRecordedRequests(
            request().withPath(getObjectPath(objectName)).withMethod("PUT")
        ).firstOrNull()

    private fun getObjectPath(objectName: String) = "/n/$namespace/b/$dlbucketName/o/$objectName"

    private fun decode(data: String) = String(Base64.decode(data))
}
