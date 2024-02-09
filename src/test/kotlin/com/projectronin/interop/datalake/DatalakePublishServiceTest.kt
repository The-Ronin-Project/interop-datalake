package com.projectronin.interop.datalake

import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.projectronin.interop.common.jackson.JacksonManager
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.datatype.primitive.Code
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.datatype.primitive.asFHIR
import com.projectronin.interop.fhir.r4.resource.Binary
import com.projectronin.interop.fhir.r4.resource.Location
import com.projectronin.interop.fhir.r4.resource.Practitioner
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.time.LocalDate

class DatalakePublishServiceTest {
    private val mockClient = mockk<OCIClient>()
    private val service = DatalakePublishService(mockClient)
    private val tenantId = "mockTenant"

    @Test
    fun `empty FHIR R4 collection is skipped`() {
        service.publishFHIRR4(tenantId, emptyList())
        verify(exactly = 0) { mockClient.uploadToDatalake(any(), any()) }
    }

    @Test
    fun `real FHIR R4 collection processes`() {
        mockkConstructor(LocalDate::class)
        mockkStatic(LocalDate::class)

        val mockkLocalDate =
            mockk<LocalDate> {
                every { format(any()) } returns "1990-01-03"
            }
        every { LocalDate.now() } returns mockkLocalDate // LocalDate.of(1990,1,3)

        val location1 =
            Location(
                id = Id("abc"),
                name = "Location1".asFHIR(),
            )
        val location2 =
            Location(
                id = Id("def"),
                name = "Location2".asFHIR(),
            )
        val practitioner =
            Practitioner(
                id = Id("abc"),
            )
        val filePathString =
            "ehr/__RESOURCETYPE__/fhir_tenant_id=mockTenant/_date=1990-01-03/__FHIRID__.json"
        val locationFilePathString = filePathString.replace("__RESOURCETYPE__", "location")
        val practitionerFilePathString = filePathString.replace("__RESOURCETYPE__", "practitioner")
        val objectMapper = JacksonManager.objectMapper

        val objectStorageClient = mockk<ObjectStorageClient>(relaxed = true)
        every { mockClient.getObjectStorageClient() } returns objectStorageClient
        every {
            mockClient.uploadToDatalake(
                locationFilePathString.replace("__FHIRID__", "abc"),
                objectMapper.writeValueAsString(location1),
                objectStorageClient,
            )
        } returns true
        every {
            mockClient.uploadToDatalake(
                locationFilePathString.replace("__FHIRID__", "def"),
                objectMapper.writeValueAsString(location2),
                objectStorageClient,
            )
        } returns true
        every {
            mockClient.uploadToDatalake(
                practitionerFilePathString.replace("__FHIRID__", "abc"),
                objectMapper.writeValueAsString(practitioner),
                objectStorageClient,
            )
        } returns true
        service.publishFHIRR4(tenantId, listOf(location1, location2, practitioner))
        verify(exactly = 3) { mockClient.uploadToDatalake(any(), any(), objectStorageClient) }
    }

    @Test
    fun `cannot publish a FHIR R4 resource that has no id`() {
        every { mockClient.getObjectStorageClient() } returns mockk()

        val badResource = Location()
        val exception =
            assertThrows<IllegalStateException> {
                service.publishFHIRR4(tenantId, listOf(badResource))
            }
        assertEquals(
            "Did not publish all FHIR resources to datalake for tenant $tenantId: Some resources lacked FHIR IDs. Errors were logged.",
            exception.message,
        )
    }

    @Test
    fun `cannot publish a FHIR R4 resource that has a null id or value`() {
        every { mockClient.getObjectStorageClient() } returns mockk()

        val badResource = Location(id = null)
        val badResource2 = Location(id = Id(value = ""))
        val exception =
            assertThrows<IllegalStateException> {
                service.publishFHIRR4(tenantId, listOf(badResource, badResource2))
            }
        assertEquals(
            "Did not publish all FHIR resources to datalake for tenant $tenantId: Some resources lacked FHIR IDs. Errors were logged.",
            exception.message,
        )
    }

    @Test
    fun `fhir R4 publish fails`() {
        mockkConstructor(LocalDate::class)
        mockkStatic(LocalDate::class)

        val mockkLocalDate =
            mockk<LocalDate> {
                every { format(any()) } returns "1990-01-03"
            }
        every { LocalDate.now() } returns mockkLocalDate // LocalDate.of(1990,1,3)

        val location1 =
            Location(
                id = Id("abc"),
                name = "Location1".asFHIR(),
            )

        val objectMapper = JacksonManager.objectMapper

        val objectStorageClient = mockk<ObjectStorageClient>(relaxed = true)
        every { mockClient.getObjectStorageClient() } returns objectStorageClient
        every {
            mockClient.uploadToDatalake(
                any(),
                objectMapper.writeValueAsString(location1),
                objectStorageClient,
            )
        } returns false

        val exception =
            assertThrows<IllegalStateException> {
                service.publishFHIRR4(tenantId, listOf(location1))
            }
        assertEquals("One or more writes to datalake failed", exception.message)

        verify(exactly = 1) { mockClient.uploadToDatalake(any(), any(), objectStorageClient) }
    }

    @Test
    fun `raw data publish`() {
        every {
            mockClient.uploadToDatalake(
                any(),
                any(),
            )
        } returns true
        every { mockClient.getDatalakeFullURL(any()) } returns "http://objectstorage"
        val response = service.publishRawData(tenantId, "json data", "http://Epic.com")
        assertTrue(response.contains("http://objectstorage"))
    }

    @Test
    fun `raw data publish fails`() {
        every {
            mockClient.uploadToDatalake(
                any(),
                any(),
            )
        } returns false

        val exception =
            assertThrows<IllegalStateException> {
                service.publishRawData(tenantId, "json data", "http://Epic.com")
            }
        assertEquals("Raw data publication failed", exception.message)
    }

    @Test
    fun `binary publish`() {
        val objectStorageClient = mockk<ObjectStorageClient>(relaxed = true)
        every { mockClient.getObjectStorageClient() } returns objectStorageClient
        every {
            mockClient.uploadToDatalake(
                any(),
                any(),
                objectStorageClient,
            )
        } returns true
        val mockBinary = Binary(id = Id("12345"), contentType = Code("1"))
        every { mockClient.getDatalakeFullURL(any()) } returns "http://objectstorage"
        assertDoesNotThrow { service.publishBinaryData(tenantId, listOf(mockBinary)) }
    }

    @Test
    fun `binary publish fails`() {
        val objectStorageClient = mockk<ObjectStorageClient>(relaxed = true)
        every { mockClient.getObjectStorageClient() } returns objectStorageClient
        every {
            mockClient.uploadToDatalake(
                any(),
                any(),
                objectStorageClient,
            )
        } returns false
        val mockBinary = Binary(id = Id("12345"), contentType = Code("1"))

        val exception =
            assertThrows<IllegalStateException> {
                service.publishBinaryData(tenantId, listOf(mockBinary))
            }
        assertEquals("One or more writes to datalake failed", exception.message)
    }

    @Test
    fun `full datalake URL`() {
        every { mockClient.getDatalakeFullURL(any()) } returns "http://objectstorage"
        val response = service.getDatalakeFullURL("filepath!")
        assertTrue(response.contains("http://objectstorage"))
    }

    @AfterEach
    fun unmockk() {
        unmockkAll()
    }
}
