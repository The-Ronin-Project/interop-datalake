package com.projectronin.interop.datalake

import com.projectronin.interop.common.jackson.JacksonManager
import com.projectronin.interop.common.jackson.JacksonUtil
import com.projectronin.interop.datalake.oci.client.OCIClient
import com.projectronin.interop.fhir.r4.resource.Binary
import com.projectronin.interop.fhir.r4.resource.Resource
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Service allowing access to push data updates to the datalake
 */
@Service
class DatalakePublishService(private val ociClient: OCIClient, private val taskExecutor: ThreadPoolTaskExecutor) {
    private val logger = KotlinLogging.logger { }

    /**
     * Publishes serialized FHIR R4 resources to the OCI datalake.
     *
     * Gets the resourceType and resourceId from the FHIR R4 resource content.
     * A resourceType is a name defined by the FHIR spec. Examples: Patient, Practitioner, Condition, Observation, etc.
     * A resourceId could be any valid FHIR id.
     *
     * Each resource in [resources] is published to a distinct file in the datalake container.
     * The file path supports Data Platform needs for code optimization and bronze directory layout, root: ehr
     *
     * @param tenantId The tenant mnemonic for the specific resource
     * @param resources List of FHIR resources to publish. May be a mixed List with different resourceTypes,
     *                  but expects all of them to have a defined ID
     * @throws IllegalStateException if any of the resources lacked FHIR id values so were not published.
     */
    fun publishFHIRR4(tenantId: String, resources: List<Resource<*>>) {
        val root = "ehr"
        logger.info { "Publishing Ronin clinical data to datalake at $root" }
        if (resources.isEmpty()) {
            logger.debug { "Publishing nothing to datalake because the supplied data is empty" }
            return
        }
        val date = LocalDate.now()
        val resourcesToWrite = resources.filter { it.id?.value?.isNotEmpty() ?: false }

        val context = taskExecutor.asCoroutineDispatcher()
        runBlocking {
            val jobs = resourcesToWrite.map {
                async(context) {
                    val resourceType = it.resourceType
                    val resourceId = it.id?.value!!
                    val filePathString =
                        "$root/${resourceType.lowercase()}/fhir_tenant_id=$tenantId/_date=${
                        date.format(
                            DateTimeFormatter.ISO_LOCAL_DATE
                        )
                        }/$resourceId.json"
                    logger.debug { "Publishing Ronin clinical data to $filePathString" }
                    val serialized = JacksonManager.objectMapper.writeValueAsString(it)
                    ociClient.uploadToDatalake(filePathString, serialized)
                }
            }

            // Force completion of each job
            jobs.awaitAll()
        }

        if (resourcesToWrite.size < resources.size) {
            throw IllegalStateException(
                "Did not publish all FHIR resources to datalake for tenant $tenantId: Some resources lacked FHIR IDs. Errors were logged."
            )
        }
    }

    /**
     * Publishes raw data to the data lake for a specific tenant.
     *
     * @param tenantId The unique identifier for the tenant.
     * @param binaryList the Binary objects to store in OCI
     * @return The URL of the uploaded data.
     */
    fun publishBinaryData(tenantId: String, binaryList: List<Binary>) {
        val jobs = binaryList.map { binary ->
            val filePathString = getBinaryFilepath(tenantId, binary.id!!.value!!)
            logger.debug { "Publishing Binary data to $filePathString" }
            taskExecutor.submit {
                ociClient.uploadToDatalake(
                    filePathString,
                    JacksonUtil.writeJsonValue(binary)
                )
            }
        }
        jobs.forEach { it.get(20, TimeUnit.SECONDS) }
    }

    fun getBinaryFilepath(
        tenantId: String,
        resourceId: String
    ): String {
        return "ehr/Binary/fhir_tenant_id=$tenantId/$resourceId.json"
    }

    // give callers access to OCI filepath
    fun getDatalakeFullURL(filePathString: String): String {
        return ociClient.getDatalakeFullURL(filePathString)
    }

    /**
     * Publishes raw data to the data lake for a specific tenant.
     *
     * @param tenantId The unique identifier for the tenant.
     * @param data The raw data to be uploaded to the data lake.
     * @param url The URL of the original API call
     * @return The URL of the uploaded data.
     */
    fun publishRawData(tenantId: String, data: String, url: String): String {
        val transactionID = UUID.randomUUID().toString()
        val root = "raw_data_response"
        logger.info { "Publishing Ronin clinical data to datalake at $root" }
        val filePathString = "$root/tenant_id=$tenantId/transaction_id/$transactionID"
        logger.debug { "Publishing Ronin clinical data to $filePathString" }
        taskExecutor.submit {
            ociClient.uploadToDatalake(
                filePathString,
                JacksonUtil.writeJsonValue(
                    RawDataWrapper(
                        url,
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        data
                    )
                )
            )
        }
        return getDatalakeFullURL(filePathString)
    }

    private data class RawDataWrapper(val url: String, val time: String, val body: String)
}
