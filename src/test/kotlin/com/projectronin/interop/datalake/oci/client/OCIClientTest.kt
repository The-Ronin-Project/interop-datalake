package com.projectronin.interop.datalake.oci.client

import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider.SimpleAuthenticationDetailsProviderBuilder
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.requests.GetObjectRequest
import com.oracle.bmc.objectstorage.requests.PutObjectRequest
import com.oracle.bmc.objectstorage.responses.GetObjectResponse
import com.oracle.bmc.objectstorage.responses.PutObjectResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.spyk
import io.mockk.unmockkAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.InputStream
import java.util.Base64

class OCIClientTest {
    private val testClient = OCIClient(
        "tenancy",
        "user",
        "fingerprint",
        "key",
        "namespace",
        "infxbucket",
        "datalakebucket",
        "us-phoenix-1"
    )

    @Test
    fun `getAuthentication - works`() {
        mockkConstructor(SimpleAuthenticationDetailsProviderBuilder::class)
        val privateString = "-----BEGIN PRIVATE KEY-----\n" +
            "-----END PRIVATE KEY-----"
        val credentials = OCIClient(
            tenancyOCID = "ocid1.tenancy.oc1",
            userOCID = "ocid1.user.oc1.",
            fingerPrint = "a1:",
            privateKey = Base64.getEncoder().encodeToString(privateString.toByteArray()),
            namespace = "Namespace",
            infxBucket = "infxbucket",
            datalakeBucket = "dBucket",
            regionId = "us-phoenix-1"
        )
        val auth = credentials.authProvider

        assertNotNull(auth)

        assertNotNull(auth.privateKey)
        unmockkAll()
    }

    @Test
    fun `getContentBody - works`() {
        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("infxbucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<GetObjectResponse> {
            every { inputStream } returns "blag".byteInputStream()
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectFromINFX("test") } answers { callOriginal() }
        assertEquals("blag", client.getObjectFromINFX("test"))
    }

    @Test
    fun `getContentBody - works with null`() {
        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("infxbucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<GetObjectResponse> {
            every { inputStream } returns null
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectFromINFX("test") } answers { callOriginal() }
        assertNull(client.getObjectFromINFX("test"))
    }

    @Test
    fun `put object - works`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<PutObjectResponse> {
            every { __httpStatusCode__ } returns 200
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.uploadToDatalake("test", "content") } answers { callOriginal() }
        assertTrue(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object stream - works`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<PutObjectResponse> {
            every { __httpStatusCode__ } returns 200
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } returns mockResponse
        }

        val mockInputStream = mockk<InputStream> {}

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.upload("datalakebucket", "test", mockInputStream) } answers { callOriginal() }
        assertTrue(client.upload("datalakebucket", "test", mockInputStream))
    }

    @Test
    fun `put object - returns false for failure`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<PutObjectResponse> {
            every { __httpStatusCode__ } returns 404
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.uploadToDatalake("test", "content") } answers { callOriginal() }
        assertFalse(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object retries on failure`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockResponse = mockk<PutObjectResponse> {
            every { __httpStatusCode__ } returns 200
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } throws BmcException(-1, "", "", "") andThen mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.uploadToDatalake("test", "content") } answers { callOriginal() }
        assertTrue(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object retries on failure and then throws error`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } throws BmcException(-1, "", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.uploadToDatalake("test", "content") } answers { callOriginal() }
        assertThrows<BmcException> { (client.uploadToDatalake("test", "content")) }
    }

    @Test
    fun `put object doesn't retry in case that doesn't actually happen`() {
        val mockRequest = mockk<PutObjectRequest> {}
        mockkConstructor(PutObjectRequest.Builder::class)
        val mockBuilder = mockk<PutObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { putObjectBody(any()) } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { putObject(mockRequest) } throws BmcException(200, "", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.uploadToDatalake("test", "content") } answers { callOriginal() }
        assertFalse((client.uploadToDatalake("test", "content")))
    }

    @AfterEach
    fun `unmockk all`() {
        unmockkAll()
    }
}
