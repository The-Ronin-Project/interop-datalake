package com.projectronin.interop.datalake.oci.client

import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider.SimpleAuthenticationDetailsProviderBuilder
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.requests.GetObjectRequest
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest
import com.oracle.bmc.objectstorage.requests.PutObjectRequest
import com.oracle.bmc.objectstorage.responses.GetObjectResponse
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse
import com.oracle.bmc.objectstorage.responses.PutObjectResponse
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.InputStream
import java.net.URI
import java.util.Base64

class OCIClientTest {
    private val objectStorageClient = mockk<ObjectStorageClient>()
    private val getObjectRequest = mockk<GetObjectRequest>()
    private val getObjectResponse = mockk<GetObjectResponse>()
    private val putObjectRequest = mockk<PutObjectRequest>()
    private val putObjectResponse = mockk<PutObjectResponse>()

    private lateinit var client: OCIClient

    @BeforeEach
    fun setup() {
        mockkStatic(ObjectStorageClient::class)
        mockkConstructor(GetObjectRequest.Builder::class)
        mockkConstructor(PutObjectRequest.Builder::class)

        val getBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("infxbucket") } returns this
                every { build() } returns getObjectRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName("test") } returns getBuilder

        val putBuilder =
            mockk<PutObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { putObjectBody(any()) } returns this
                every { build() } returns putObjectRequest
            }
        every { anyConstructed<PutObjectRequest.Builder>().objectName("test") } returns putBuilder

        every { objectStorageClient.getObject(getObjectRequest) } returns getObjectResponse
        every { objectStorageClient.putObject(putObjectRequest) } returns putObjectResponse

        every { ObjectStorageClient.builder() } returns
            mockk {
                every { clientConfigurator(any()) } returns this
                every { isStreamWarningEnabled(false) } returns this
                every { build(any()) } returns objectStorageClient
            }

        client =
            OCIClient(
                "tenancy",
                "user",
                "fingerprint",
                "key",
                "namespace",
                "infxbucket",
                "datalakebucket",
                "us-phoenix-1",
            )
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun `getAuthentication - works`() {
        mockkConstructor(SimpleAuthenticationDetailsProviderBuilder::class)
        val privateString =
            "-----BEGIN PRIVATE KEY-----\n" +
                "-----END PRIVATE KEY-----"
        val credentials =
            OCIClient(
                tenancyOCID = "ocid1.tenancy.oc1",
                userOCID = "ocid1.user.oc1.",
                fingerPrint = "a1:",
                privateKey = Base64.getEncoder().encodeToString(privateString.toByteArray()),
                namespace = "Namespace",
                infxBucket = "infxbucket",
                datalakeBucket = "dBucket",
                regionId = "us-phoenix-1",
            )
        val auth = credentials.authProvider

        assertNotNull(auth)

        assertNotNull(auth.privateKey)
        unmockkAll()
    }

    @Test
    fun `getContentBody - works`() {
        every { getObjectResponse.inputStream } returns "blag".byteInputStream()

        assertEquals("blag", client.getObjectFromINFX("test"))
    }

    @Test
    fun `getContentBody - works with null`() {
        every { getObjectResponse.inputStream } returns null

        assertNull(client.getObjectFromINFX("test"))
    }

    @Test
    fun `put object - works`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        assertTrue(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object stream - works`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        val mockInputStream = mockk<InputStream> {}

        assertTrue(client.upload("datalakebucket", "test", mockInputStream))
    }

    @Test
    fun `upload works with shared client`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        val mockInputStream = mockk<InputStream> {}

        assertTrue(client.upload("datalakebucket", "test", mockInputStream))
    }

    @Test
    fun `upload works with non-shared client`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        val newObjectStorageClient =
            mockk<ObjectStorageClient> {
                every { putObject(putObjectRequest) } returns putObjectResponse
            }

        val mockInputStream = mockk<InputStream> {}

        assertTrue(client.upload("datalakebucket", "test", mockInputStream, newObjectStorageClient))

        verify { objectStorageClient wasNot Called }
    }

    @Test
    fun `uploadToDatalake works with shared client`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        assertTrue(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `uploadToDatalake works with non-shared client`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        val newObjectStorageClient =
            mockk<ObjectStorageClient> {
                every { putObject(putObjectRequest) } returns putObjectResponse
            }

        assertTrue(client.uploadToDatalake("test", "content", newObjectStorageClient))

        verify { objectStorageClient wasNot Called }
    }

    @Test
    fun `put object - returns false for failure`() {
        every { putObjectResponse.__httpStatusCode__ } returns 404

        assertFalse(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object retries on failure`() {
        every { putObjectResponse.__httpStatusCode__ } returns 200

        every { objectStorageClient.putObject(putObjectRequest) } throws
            BmcException(-1, "", "", "") andThen putObjectResponse

        assertTrue(client.uploadToDatalake("test", "content"))
    }

    @Test
    fun `put object retries on failure and then throws error`() {
        every { objectStorageClient.putObject(putObjectRequest) } throws BmcException(-1, "", "", "")

        assertThrows<BmcException> { (client.uploadToDatalake("test", "content")) }
    }

    @Test
    fun `put object doesn't retry in case that doesn't actually happen`() {
        every { objectStorageClient.putObject(putObjectRequest) } throws BmcException(200, "", "", "")

        assertFalse((client.uploadToDatalake("test", "content")))
    }

    @Test
    fun `getObjectBody by url works`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("bucket") } returns this
                every { build() } returns getObjectRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        every { getObjectResponse.inputStream } returns testFileContents.byteInputStream()

        assertEquals(testFileContents, client.getObjectBody(testUrl))
    }

    @Test
    fun `getObjectBody by malformed url returns null and doesn't call OCI`() {
        val testUrl1 = URI("")
        val testUrl2 = URI("https://a.b.c.d/efg/hij/klm/nop")
        val testUrl3 = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o")

        assertNull(client.getObjectBody(testUrl1))
        assertNull(client.getObjectBody(testUrl2))
        assertNull(client.getObjectBody(testUrl3))

        verify(exactly = 0) { objectStorageClient.getObject(any()) }
    }

    @Test
    fun `getObjectBody by fileName works`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"
        val testFileContents = "TWV0YW1vcnBob3NpcyBieSBPdmlk"

        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { build() } returns getObjectRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        every { getObjectResponse.inputStream } returns testFileContents.byteInputStream()

        assertEquals(testFileContents, client.getObjectBody(fileName))
    }

    @Test
    fun `getObjectBody by filename returns null when getting 404 exception from OCI`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        every { objectStorageClient.getObject(mockRequest) } throws BmcException(404, "BucketNotFound", "", "")

        assertNull(client.getObjectBody(fileName))
    }

    @Test
    fun `getObjectBody by filename errors when getting non-404 exception from OCI`() {
        val fileName = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(fileName) } returns mockBuilder

        every { objectStorageClient.getObject(mockRequest) } throws BmcException(500, "BucketFellOffCliff", "", "")

        assertThrows<BmcException> { client.getObjectBody(fileName) }
    }

    @Test
    fun `getObjectBody by url returns null when getting 404 exception from OCI`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("bucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        every { objectStorageClient.getObject(mockRequest) } throws BmcException(404, "BucketNotFound", "", "")

        assertNull(client.getObjectBody(testUrl))
    }

    @Test
    fun `getObjectBody by url errors when getting non-404 exception from OCI`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<GetObjectRequest> {}
        mockkConstructor(GetObjectRequest.Builder::class)
        val mockBuilder =
            mockk<GetObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("bucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<GetObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        every { objectStorageClient.getObject(mockRequest) } throws BmcException(500, "BucketFellOffCliff", "", "")

        assertThrows<BmcException> { client.getObjectBody(testUrl) }
    }

    @Test
    fun `Object exists by url works with 200 response`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder =
            mockk<HeadObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("bucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse =
            mockk<HeadObjectResponse> {
                every { __httpStatusCode__ } returns 200
            }

        every { objectStorageClient.headObject(mockRequest) } returns mockResponse

        assertTrue(client.objectExists(testUrl))
    }

    @Test
    fun `Object exists by url works when not 200 response`() {
        val testFile = "testFile"
        val testUrl = URI("https://objectstorage.region.oraclecloud.com/n/namespace/b/bucket/o/$testFile")

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder =
            mockk<HeadObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("bucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse =
            mockk<HeadObjectResponse> {
                every { __httpStatusCode__ } returns 404
            }

        every { objectStorageClient.headObject(mockRequest) } returns mockResponse

        assertFalse(client.objectExists(testUrl))
    }

    @Test
    fun `Object exists by fileName works with 200 response`() {
        val testFile = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder =
            mockk<HeadObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse =
            mockk<HeadObjectResponse> {
                every { __httpStatusCode__ } returns 200
            }

        every { objectStorageClient.headObject(mockRequest) } returns mockResponse
        assertTrue(client.objectExists(testFile))
    }

    @Test
    fun `Object exists by fileName works when not 200 response`() {
        val testFile = "ehr/Binary/fhir_tenant_id=tenantId/resourceId.json"

        val mockRequest = mockk<HeadObjectRequest> {}
        mockkConstructor(HeadObjectRequest.Builder::class)
        val mockBuilder =
            mockk<HeadObjectRequest.Builder> {
                every { namespaceName("namespace") } returns this
                every { bucketName("datalakebucket") } returns this
                every { build() } returns mockRequest
            }
        every { anyConstructed<HeadObjectRequest.Builder>().objectName(testFile) } returns mockBuilder

        val mockResponse =
            mockk<HeadObjectResponse> {
                every { __httpStatusCode__ } returns 404
            }

        every { objectStorageClient.headObject(mockRequest) } returns mockResponse

        assertFalse(client.objectExists(testFile))
    }
}
