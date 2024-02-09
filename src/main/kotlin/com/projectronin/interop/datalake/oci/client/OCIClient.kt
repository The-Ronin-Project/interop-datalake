package com.projectronin.interop.datalake.oci.client

import com.oracle.bmc.Region
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider
import com.oracle.bmc.http.client.jersey3.Jersey3ClientProperties
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.requests.GetObjectRequest
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest
import com.oracle.bmc.objectstorage.requests.PutObjectRequest
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.net.URI
import java.util.Base64
import java.util.function.Supplier

/***
 * Client for interactive with OCI's object store
 */
@Component
class OCIClient(
    @Value("\${oci.tenancy.ocid}")
    private val tenancyOCID: String,
    @Value("\${oci.user.ocid}")
    private val userOCID: String,
    @Value("\${oci.fingerPrint}")
    private val fingerPrint: String,
    @Value("\${oci.private.key.base64}")
    private val privateKey: String,
    @Value("\${oci.namespace}")
    private val namespace: String,
    @Value("\${oci.conceptmap.bucket.name:infx-shared}")
    private val infxBucket: String,
    @Value("\${oci.publish.bucket.name}")
    private val datalakeBucket: String,
    @Value("\${oci.region:us-phoenix-1}")
    private val regionId: String,
) {
    private val logger = KotlinLogging.logger { }

    private val privateKeySupplier: Supplier<InputStream> =
        Supplier<InputStream> { Base64.getDecoder().decode(privateKey).inputStream() }
    val authProvider: SimpleAuthenticationDetailsProvider by lazy {
        SimpleAuthenticationDetailsProvider.builder()
            .tenantId(tenancyOCID)
            .userId(userOCID)
            .fingerprint(fingerPrint)
            .region(Region.fromRegionId(regionId))
            .privateKeySupplier(privateKeySupplier)
            .build()
    }
    private val sharedClient by lazy { getObjectStorageClient() }

    /**
     * Gets the [ObjectStorageClient] to use for a specific call.
     */
    internal fun getObjectStorageClient(): ObjectStorageClient =
        ObjectStorageClient.builder()
            .clientConfigurator { // Disables Apache Connector
                it.property(Jersey3ClientProperties.USE_APACHE_CONNECTOR, false)
            }
            // Disables stream warning presented about Apache Connector
            .isStreamWarningEnabled(false)
            .build(authProvider)

    /**
     * Retrieves the contents of the object found at [fileName] in the infx-shared bucket. If [fileName] is null,
     * retrieves the contents of the most recent DataNormalizationRegistry JSON in the infx-shared bucket.
     * The DataNormalizationRegistry is Informatics' manifest of the most recent ValueSets and ConceptMaps in OCI.
     */
    fun getObjectFromINFX(fileName: String): String? {
        return getObjectBody(infxBucket, fileName)
    }

    /**
     * Upload the string found in [data] to [fileName] to the datalake bucket
     *  Returns true if it was successful
     */
    fun uploadToDatalake(
        fileName: String,
        data: String,
        client: ObjectStorageClient = sharedClient,
    ): Boolean {
        return upload(datalakeBucket, fileName, data, client)
    }

    fun getDatalakeFullURL(fileName: String): String =
        "https://objectstorage.$regionId.oraclecloud.com/n/$namespace/b/$datalakeBucket/o/$fileName"

    /**
     * Upload the string found in [data] to [fileName]
     * Returns true if it was successful
     */
    fun upload(
        bucket: String,
        fileName: String,
        data: String,
        client: ObjectStorageClient = sharedClient,
    ): Boolean =
        uploadObjectRequest(
            PutObjectRequest.builder()
                .objectName(fileName)
                .putObjectBody(ByteArrayInputStream(data.toByteArray()))
                .namespaceName(namespace)
                .bucketName(bucket)
                .build(),
            client,
        )

    /**
     * Upload the stream found in [stream] to [fileName].  Useful when we need to upload a large file.  If the file is
     * a resource, you can use `this.javaClass.getResourceAsStream("name")` to load it.  If the file is in the local
     * system, use `FileInputStream("name")`.
     * Returns true if it was successful.
     */
    fun upload(
        bucket: String,
        fileName: String,
        stream: InputStream,
        client: ObjectStorageClient = sharedClient,
    ): Boolean =
        uploadObjectRequest(
            PutObjectRequest.builder()
                .objectName(fileName)
                .putObjectBody(stream)
                .namespaceName(namespace)
                .bucketName(bucket)
                .build(),
            client,
        )

    private fun uploadObjectRequest(
        putObjectRequest: PutObjectRequest,
        client: ObjectStorageClient,
    ): Boolean {
        // OCI JDK natively supports retrying, but errors occasionally when something unexpected happens
        // https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/javasdkconcepts.htm

        val responseStatusCode =
            try {
                client.putObject(putObjectRequest).__httpStatusCode__
                // client side exception in the OCI JDK
            } catch (bmcException: BmcException) {
                if (bmcException.statusCode == -1) {
                    runBlocking { delay(5000) }
                    client.putObject(putObjectRequest).__httpStatusCode__
                } else {
                    logger.error(bmcException) { "Error while uploading to OCI" }
                    null
                }
            }
        return responseStatusCode in (200..202)
    }

    /**
     * Retrieves the contents of the object found at [fileName]
     */
    fun getObjectBody(
        bucket: String,
        fileName: String,
    ): String? {
        val getObjectRequest =
            GetObjectRequest.builder()
                .objectName(fileName)
                .namespaceName(namespace)
                .bucketName(bucket)
                .build()

        val result =
            try {
                val inputStream = sharedClient.getObject(getObjectRequest).inputStream
                inputStream?.bufferedReader().use { it?.readText() }
            } catch (exception: BmcException) {
                when (exception.statusCode) {
                    404 -> null
                    else -> throw exception
                }
            }

        return result
    }

    /**
     * Retrieves the contents of the object found at [url].
     * Expects url with path of form: /n/namespace/b/bucket/o/filename
     */
    fun getObjectBody(url: URI): String? {
        val (bucket, fileName) =
            runCatching {
                getBucketAndFilenameFromURI(url)
            }.getOrDefault(Pair("", ""))

        return if (bucket.isBlank() || fileName.isBlank()) null else getObjectBody(bucket, fileName)
    }

    /**
     * Retrieves the contents of object from the data lake with name [fileName]
     */
    fun getObjectBody(fileName: String): String? = getObjectBody(datalakeBucket, fileName)

    /**
     * Retrieves the meta data of the object found at [fileName].
     */
    fun getObjectDetails(
        bucket: String,
        fileName: String,
    ): HeadObjectResponse {
        val headObjectRequest =
            HeadObjectRequest.builder()
                .objectName(fileName)
                .namespaceName(namespace)
                .bucketName(bucket)
                .build()

        return sharedClient.headObject(headObjectRequest)
    }

    /**
     * Retrieves whether the object exists at the given [url].
     * Expects url with path of form: /n/namespace/b/bucket/o/filename
     */
    fun objectExists(url: URI): Boolean {
        val (bucket, fileName) = getBucketAndFilenameFromURI(url)
        return getObjectDetails(bucket, fileName).let {
            it.__httpStatusCode__ == 200
        }
    }

    /**
     * Retrieves whether the object exists by [fileName]
     */
    fun objectExists(fileName: String): Boolean =
        getObjectDetails(
            datalakeBucket,
            fileName,
        ).let {
            it.__httpStatusCode__ == 200
        }

    /**
     * Returns [Pair] of bucket and fileName from an OCI object url
     */
    private fun getBucketAndFilenameFromURI(url: URI): Pair<String, String> =
        url.rawPath
            .removePrefix("/")
            .split('/')
            .slice(setOf(3, 5)).let { Pair(it.first(), it.last()) }
}
