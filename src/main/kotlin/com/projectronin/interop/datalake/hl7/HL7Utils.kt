package com.projectronin.interop.datalake.hl7

import mu.KotlinLogging

private val logger = KotlinLogging.logger { }

/**
 * To publish to the datalake, we need MSH.9.
 * @return a list with the messageType (like MDM) in position 0 and messageEvent (like T02) in position 1.
 */
fun getMSH9(tenantId: String, message: String): List<String> {
    // In every HL7v2 message and version, separators, and fields MSH.1 through MSH.9, are on line 1 in the same
    // positions. Message parsers like hapi Parser and Terser exist, but add complexity and overhead without value.

    val emptyStructure = emptyList<String>()
    val mshStart = message.indexOf("MSH", 0)
    if (mshStart < 0) {
        logger.error { "Did not publish HL7v2 message to datalake for tenant $tenantId: Message header is missing" }
        return emptyStructure
    }
    val separator1 = message.elementAtOrNull(mshStart + 3)
    val separator2 = message.elementAtOrNull(mshStart + 4)
    if ((separator1 == null) || (separator2 == null)) {
        logger.error { "Did not publish HL7v2 message to datalake for tenant $tenantId: Message header is invalid" }
        return emptyStructure
    }
    val fields: List<String> = message.split(separator1, limit = 10)
    if (fields.size < 10) {
        logger.error { "Did not publish HL7v2 message to datalake for tenant $tenantId: Message header is incomplete" }
        return emptyStructure
    }
    val structure = fields[8]
    if ((structure.length == 7) && (structure.indexOf(separator2) == 3)) {
        return structure.split(separator2)
    }
    return emptyStructure
}
