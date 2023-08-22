package com.projectronin.interop.datalake

import com.projectronin.interop.common.jackson.JacksonUtil
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.datatype.primitive.Code
import com.projectronin.interop.fhir.r4.resource.Binary
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.URI

class DatalakeRetrieveServiceTest {
    private val mockClient = mockk<OCIClient>()
    private val service = DatalakeRetrieveService(mockClient)
    private val binaryBucket = "bucket"
    private val binaryUrl = "https://objectstorage.region.oraclecloud.com/n/namespace/b/$binaryBucket/o/%s"
    private val binaryFile1 = "file1"
    private val binaryFile2 = "file2"
    private val binaryFile3 = "file3"
    private val binary1 = Binary(contentType = Code("pdf"))
    private val binary2 = Binary(contentType = Code("text/json"))
    private val binary3 = Binary(contentType = Code("mp4"))
    private val tenantId = "tenantId"
    private val resourceId = "resourceId"

    @Test
    fun `empty binary url list is skipped`() {
        service.retrieveBinaryData(emptyList())
        verify(exactly = 0) { mockClient.getObjectBody(ofType(URI::class)) }
    }

    @Test
    fun `single non-existent binary url calls client and returns null`() {
        val binaryUrl1 = URI(String.format(binaryUrl, binaryFile1))

        every {
            mockClient.getObjectBody(binaryUrl1)
        } returns null

        val result = service.retrieveBinaryData(binaryUrl1)
        verify(exactly = 1) { mockClient.getObjectBody(ofType(URI::class)) }
        assertNull(result)
    }

    @Test
    fun `single existent binary url calls client and returns Binary`() {
        val binaryUrl1 = URI(String.format(binaryUrl, binaryFile1))
        val binaryJson = JacksonUtil.writeJsonValue(binary1)

        every {
            mockClient.getObjectBody(binaryUrl1)
        } returns binaryJson

        val result = service.retrieveBinaryData(binaryUrl1)
        verify(exactly = 1) { mockClient.getObjectBody(ofType(URI::class)) }
        assertEquals(binary1, result)
    }

    @Test
    fun `multiple non-existent binary urls calls client and returns no results`() {
        val binaryUrl1 = URI(String.format(binaryUrl, binaryFile1))
        val binaryUrl2 = URI(String.format(binaryUrl, binaryFile2))
        val binaryUrl3 = URI(String.format(binaryUrl, binaryFile3))

        every {
            mockClient.getObjectBody(binaryUrl1)
        } returns null

        every {
            mockClient.getObjectBody(binaryUrl2)
        } returns null

        every {
            mockClient.getObjectBody(binaryUrl3)
        } returns null

        val resultMap = service.retrieveBinaryData(listOf(binaryUrl1, binaryUrl2, binaryUrl3))
        verify(exactly = 3) { mockClient.getObjectBody(ofType(URI::class)) }
        assertEquals(0, resultMap.size)
    }

    @Test
    fun `multiple existent binary urls calls client and returns results`() {
        val binaryUrl1 = URI(String.format(binaryUrl, binaryFile1))
        val binaryUrl2 = URI(String.format(binaryUrl, binaryFile2))
        val binaryUrl3 = URI(String.format(binaryUrl, binaryFile3))
        val binaryJson1 = JacksonUtil.writeJsonValue(binary1)
        val binaryJson2 = JacksonUtil.writeJsonValue(binary2)
        val binaryJson3 = JacksonUtil.writeJsonValue(binary3)

        every {
            mockClient.getObjectBody(binaryUrl1)
        } returns binaryJson1

        every {
            mockClient.getObjectBody(binaryUrl2)
        } returns binaryJson2

        every {
            mockClient.getObjectBody(binaryUrl3)
        } returns binaryJson3

        val resultMap = service.retrieveBinaryData(listOf(binaryUrl1, binaryUrl2, binaryUrl3))
        verify(exactly = 3) { mockClient.getObjectBody(ofType(URI::class)) }
        assertEquals(3, resultMap.size)
        assertTrue(resultMap.keys.contains(binaryUrl1))
        assertTrue(resultMap.keys.contains(binaryUrl2))
        assertTrue(resultMap.keys.contains(binaryUrl3))
        assertEquals(binary1, resultMap[binaryUrl1])
        assertEquals(binary2, resultMap[binaryUrl2])
        assertEquals(binary3, resultMap[binaryUrl3])
    }

    @Test
    fun `mixed existent binary urls calls client and returns correct results`() {
        val binaryUrl1 = URI(String.format(binaryUrl, binaryFile1))
        val binaryUrl2 = URI(String.format(binaryUrl, binaryFile2))
        val binaryUrl3 = URI(String.format(binaryUrl, binaryFile3))
        val binaryJson2 = JacksonUtil.writeJsonValue(binary2)
        val binaryJson3 = JacksonUtil.writeJsonValue(binary3)

        every {
            mockClient.getObjectBody(binaryUrl1)
        } returns null

        every {
            mockClient.getObjectBody(binaryUrl2)
        } returns binaryJson2

        every {
            mockClient.getObjectBody(binaryUrl3)
        } returns binaryJson3

        val resultMap = service.retrieveBinaryData(listOf(binaryUrl1, binaryUrl2, binaryUrl3))
        verify(exactly = 3) { mockClient.getObjectBody(ofType(URI::class)) }
        assertEquals(2, resultMap.size)
        assertTrue(resultMap.keys.contains(binaryUrl2))
        assertTrue(resultMap.keys.contains(binaryUrl3))
        assertEquals(binary2, resultMap[binaryUrl2])
        assertEquals(binary3, resultMap[binaryUrl3])
    }

    @Test
    fun `retrieve by tenantId and resourceId returns Binary`() {
        val binaryJson = JacksonUtil.writeJsonValue(binary1)
        val arg = "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"

        every {
            mockClient.getObjectBody(arg)
        } returns binaryJson

        val result = service.retrieveBinaryData(tenantId, resourceId)
        verify(exactly = 1) { mockClient.getObjectBody(ofType(String::class)) }
        assertEquals(result, binary1)
    }

    @Test
    fun `retrieve by tenantId and resourceId returns null`() {
        val arg = "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"

        every {
            mockClient.getObjectBody(arg)
        } returns null

        val result = service.retrieveBinaryData(tenantId, resourceId)
        verify(exactly = 1) { mockClient.getObjectBody(ofType(String::class)) }
        assertNull(result)
    }

    @Test
    fun `object exists at url returns true`() {
        val objectUrl = URI(String.format(binaryUrl, binaryFile1))

        every {
            mockClient.ObjectExists(objectUrl)
        } returns true

        val result = service.objectExists(objectUrl)
        assertTrue(result)
    }

    @Test
    fun `object exists at url returns false`() {
        val objectUrl = URI(String.format(binaryUrl, binaryFile1))

        every {
            mockClient.ObjectExists(objectUrl)
        } returns false

        val result = service.objectExists(objectUrl)
        assertFalse(result)
    }

    @Test
    fun `binary exists with tenantId and resourceID returns true`() {
        val arg = "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"

        every {
            mockClient.ObjectExists(arg)
        } returns true

        val result = service.binaryExists(tenantId, resourceId)
        assertTrue(result)
    }

    @Test
    fun `binary exists with tenantId and resourceID returns false`() {
        val arg = "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"

        every {
            mockClient.ObjectExists(arg)
        } returns false

        val result = service.binaryExists(tenantId, resourceId)
        assertFalse(result)
    }

    @AfterEach
    fun `clear all mockks`() {
        clearAllMocks()
    }
}
