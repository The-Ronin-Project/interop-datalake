package com.projectronin.interop.datalake

import com.projectronin.interop.common.jackson.JacksonManager
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.datatype.primitive.Id
import com.projectronin.interop.fhir.r4.datatype.primitive.asFHIR
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
import org.junit.jupiter.api.assertThrows
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.time.LocalDate
import java.util.concurrent.TimeUnit

class DatalakePublishServiceTest {
    private val mockClient = mockk<OCIClient>()
    private val mockExecutor = mockk<ThreadPoolTaskExecutor> {
        every { submit(any()) } answers {
            val result = firstArg<Runnable>().run()
            mockk {
                every { get(20, TimeUnit.SECONDS) } returns result
            }
        }
    }
    private val service = DatalakePublishService(mockClient, mockExecutor)
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

        val mockkLocalDate = mockk<LocalDate> {
            every { format(any()) } returns "1990-01-03"
        }
        every { LocalDate.now() } returns mockkLocalDate // LocalDate.of(1990,1,3)

        val location1 = Location(
            id = Id("abc"),
            name = "Location1".asFHIR()
        )
        val location2 = Location(
            id = Id("def"),
            name = "Location2".asFHIR()
        )
        val practitioner = Practitioner(
            id = Id("abc")
        )
        val filePathString =
            "ehr/__RESOURCETYPE__/fhir_tenant_id=mockTenant/_date=1990-01-03/__FHIRID__.json"
        val locationFilePathString = filePathString.replace("__RESOURCETYPE__", "location")
        val practitionerFilePathString = filePathString.replace("__RESOURCETYPE__", "practitioner")
        val objectMapper = JacksonManager.objectMapper
        every {
            mockClient.uploadToDatalake(
                locationFilePathString.replace("__FHIRID__", "abc"),
                objectMapper.writeValueAsString(location1)
            )
        } returns true
        every {
            mockClient.uploadToDatalake(
                locationFilePathString.replace("__FHIRID__", "def"),
                objectMapper.writeValueAsString(location2)
            )
        } returns true
        every {
            mockClient.uploadToDatalake(
                practitionerFilePathString.replace("__FHIRID__", "abc"),
                objectMapper.writeValueAsString(practitioner)
            )
        } returns true
        service.publishFHIRR4(tenantId, listOf(location1, location2, practitioner))
        verify(exactly = 3) { mockClient.uploadToDatalake(any(), any()) }
    }

    @Test
    fun `cannot publish a FHIR R4 resource that has no id`() {
        val badResource = Location()
        val exception = assertThrows<IllegalStateException> {
            service.publishFHIRR4(tenantId, listOf(badResource))
        }
        assertEquals(
            "Did not publish all FHIR resources to datalake for tenant $tenantId: Some resources lacked FHIR IDs. Errors were logged.",
            exception.message
        )
    }

    @Test
    fun `cannot publish a FHIR R4 resource that has a null id or value`() {
        val badResource = Location(id = null)
        val badResource2 = Location(id = Id(value = ""))
        val exception = assertThrows<IllegalStateException> {
            service.publishFHIRR4(tenantId, listOf(badResource, badResource2))
        }
        assertEquals(
            "Did not publish all FHIR resources to datalake for tenant $tenantId: Some resources lacked FHIR IDs. Errors were logged.",
            exception.message
        )
    }

    @Test
    fun `raw data publish`() {
        every {
            mockClient.uploadToDatalake(
                any(),
                any()
            )
        } returns true
        every { mockClient.getDatalakeFullURL(any()) } returns "http://objectstorage"
        val response = service.publishRawData(tenantId, "json data", "http://Epic.com")
        assertTrue(response.contains("http://objectstorage"))
    }

    @AfterEach
    fun unmockk() {
        unmockkAll()
    }
}
