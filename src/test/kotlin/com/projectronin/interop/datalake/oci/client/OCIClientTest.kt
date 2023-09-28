package com.projectronin.interop.datalake.oci.client

import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider.SimpleAuthenticationDetailsProviderBuilder
import com.oracle.bmc.http.ClientConfigurator
import com.oracle.bmc.http.client.HttpClientBuilder
import com.oracle.bmc.http.client.jersey.JerseyClientProperties
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.requests.GetObjectRequest
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest
import com.oracle.bmc.objectstorage.requests.PutObjectRequest
import com.oracle.bmc.objectstorage.responses.GetObjectResponse
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse
import com.oracle.bmc.objectstorage.responses.PutObjectResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.spyk
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.InputStream
import java.net.URI
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

    @Test
    fun `client is constructed properly`() {
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

        mockkStatic(ObjectStorageClient::class)
        val clientConfiguratorSlot = slot<ClientConfigurator>()
        val mockClientBuilder = mockk<ObjectStorageClient.Builder>() {
            every { clientConfigurator(capture(clientConfiguratorSlot)) } returns this
            every { isStreamWarningEnabled(any()) } returns this
            every { build(any()) } returns mockObjectStorageClient
        }
        every { ObjectStorageClient.builder() } returns mockClientBuilder

        val mockInputStream = mockk<InputStream> {}

        val client = spyk(testClient)
        every { client.upload("datalakebucket", "test", mockInputStream) } answers { callOriginal() }
        assertTrue(client.upload("datalakebucket", "test", mockInputStream))

        verify(exactly = 1) { mockClientBuilder.isStreamWarningEnabled(false) }

        val httpClientBuilder = mockk<HttpClientBuilder>(relaxed = true)
        val clientConfigurator = clientConfiguratorSlot.captured
        clientConfigurator.customizeClient(httpClientBuilder)

        verify(exactly = 1) { httpClientBuilder.property(JerseyClientProperties.USE_APACHE_CONNECTOR, false) }
    }

    @Test
    fun `getObjectBody by url works`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse = mockk<GetObjectResponse> {
            every { inputStream } returns testFileContents.byteInputStream()
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(testUrl) } answers { callOriginal() }
        assertEquals(testFileContents, client.getObjectBody(testUrl))
    }

    @Test
    fun `getObjectBody by malformed url returns null and doesn't call OCI`() {
        val testUrl1 = URI("")
        val testUrl2 = URI("https://a.b.c.d/efg/hij/klm/nop")
        val testUrl3 = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o")

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(any()) } returns mockBuilder

        val mockResponse = mockk<GetObjectResponse> {
            every { inputStream } returns "".byteInputStream()
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(ofType(URI::class)) } answers { callOriginal() }
        assertNull(client.getObjectBody(testUrl1))
        assertNull(client.getObjectBody(testUrl2))
        assertNull(client.getObjectBody(testUrl3))

        verify(exactly = 0) { mockObjectStorageClient.getObject(any()) }
    }

    @Test
    fun `getObjectBody by fileName works`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        val mockResponse = mockk<GetObjectResponse> {
            every { inputStream } returns testFileContents.byteInputStream()
        }
        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(fileName) } answers { callOriginal() }
        assertEquals(testFileContents, client.getObjectBody(fileName))
    }

    @Test
    fun `getObjectBody by filename returns null when getting 404 exception from OCI`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } throws BmcException(404, "BucketNotFound", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(ofType(URI::class)) } answers { callOriginal() }
        assertNull(client.getObjectBody(fileName))
    }

    @Test
    fun `getObjectBody by filename errors when getting non-404 exception from OCI`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } throws BmcException(500, "BucketFellOffCliff", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(ofType(URI::class)) } answers { callOriginal() }
        assertThrows<BmcException> { client.getObjectBody(fileName) }
    }

    @Test
    fun `getObjectBody by url returns null when getting 404 exception from OCI`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } throws BmcException(404, "BucketNotFound", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(testUrl) } answers { callOriginal() }
        assertNull(client.getObjectBody(testUrl))
    }

    @Test
    fun `getObjectBody by url errors when getting non-404 exception from OCI`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder = mockk<GetObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { getObject(mockRequest) } throws BmcException(500, "BucketFellOffCliff", "", "")
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        every { client.getObjectBody(testUrl) } answers { callOriginal() }
        assertThrows<BmcException> { client.getObjectBody(testUrl) }
    }

    @Test
    fun `Object exists by url works with 200 response`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder = mockk<HeadObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse = mockk<HeadObjectResponse> {
            every { __httpStatusCode__ } returns 200
        }

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { headObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        assertTrue(client.objectExists(testUrl))
    }

    @Test
    fun `Object exists by url works when not 200 response`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder = mockk<HeadObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("bucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse = mockk<HeadObjectResponse> {
            every { __httpStatusCode__ } returns 404
        }

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { headObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        assertFalse(client.objectExists(testUrl))
    }

    @Test
    fun `Object exists by fileName works with 200 response`() {
        val testFile = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder = mockk<HeadObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse = mockk<HeadObjectResponse> {
            every { __httpStatusCode__ } returns 200
        }

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { headObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        assertTrue(client.objectExists(testFile))
    }

    @Test
    fun `Object exists by fileName works when not 200 response`() {
        val testFile = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder = mockk<HeadObjectRequest.Builder> {
            every { namespaceName("namespace") } returns this
            every { bucketName("datalakebucket") } returns this
            every { build() } returns mockRequest
        }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse = mockk<HeadObjectResponse> {
            every { __httpStatusCode__ } returns 404
        }

        val mockObjectStorageClient = mockk<ObjectStorageClient> {
            every { headObject(mockRequest) } returns mockResponse
        }

        val client = spyk(testClient)
        every { client getProperty "client" } returns mockObjectStorageClient
        assertFalse(client.objectExists(testFile))
    }

    @AfterEach
    fun `unmockk all`() {
        unmockkAll()
    }
}
