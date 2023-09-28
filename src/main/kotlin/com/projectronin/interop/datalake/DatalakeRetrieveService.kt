package com.projectronin.interop.datalake

import com.projectronin.interop.common.collection.associateWithNonNull
import com.projectronin.interop.common.jackson.JacksonUtil
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.resource.Binary
import org.springframework.stereotype.Service
import java.net.URI

/**
 * Service allowing access to retrieve data from the datalake
 */
@Service
class DatalakeRetrieveService(private val ociClient: OCIClient) {

    /**
     * Retrieves a map of [URI] to [Binary] object for each of the binary [urls]
     */
    fun retrieveBinaryData(urls: List<URI>): Map<URI, Binary> = urls.associateWithNonNull {
        ociClient.getObjectBody(it)?.let { binary ->
            JacksonUtil.readJsonObject(binary, Binary::class)
        }
    }

    /**
     * Retrieves a [Binary] object using its [url]
     */
    fun retrieveBinaryData(url: URI): Binary? = retrieveBinaryData(listOf(url))[url]

    /**
     * Retrieves a [Binary] object by [tenantId] and [resourceId]
     */
    fun retrieveBinaryData(tenantId: String, resourceId: String): Binary? =
        ociClient.getObjectBody(tenantAndResourceToFilePath(tenantId, resourceId))?.let {
            JacksonUtil.readJsonObject(it, Binary::class)
        }

    /**
     * Returns whether an object exists at [url]
     */
    fun objectExists(url: URI): Boolean = ociClient.objectExists(url)

    /**
     * Returns whether an [Binary] object exists with [tenantId] and [resourceId]
     */
    fun binaryExists(tenantId: String, resourceId: String): Boolean =
        ociClient.objectExists(tenantAndResourceToFilePath(tenantId, resourceId))

    private fun tenantAndResourceToFilePath(tenantId: String, resourceId: String): String =
        "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"
}
