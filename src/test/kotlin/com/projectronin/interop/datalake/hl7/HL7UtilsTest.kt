package com.projectronin.interop.datalake.hl7

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HL7UtilsTest {
    @Test
    fun `HL7v2 missing message header`() {
        val messageMDMT02 = """
           
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 missing separator1`() {
        val messageMDMT02 = """
            MSH
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 missing separator2`() {
        val messageMDMT02 = """
            MSH|
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 short header`() {
        val messageMDMT02 = """
            MSH|^~\&|Brocade|MDACC|Epic|MDACC|20220412161126
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 empty MSH-9`() {
        val messageMDMT02 = """
            MSH|^~\&|Brocade|MDACC|Epic|MDACC|20220412161126|||121160|T|2.7
            EVN|T02|20220412161126
            PID|||2008661^^^EPI^MR||RADONCTEST^GISELLE||19671028|F||||||
            PV1||I
            TXA||101||20220412000000|13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|20220412161126|20220412161126||13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|||^^dpg121160_637853766870444105|||||IP||UN|||||||
            OBX|1|TX|||Date of Service: Tuesday, April 12, 2022
            OBX|2|TX|||
            OBX|3|TX|||Diagnosis: Malignant neoplasm of cervix uteri, unspecified
            OBX|4|TX|||Histopathology: 8070/3 Squamous cell carcinoma, NOS
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 MSH-9 does not have 7 characters`() {
        val messageMDMT02 = """
            MSH|^~\&|Brocade|MDACC|Epic|MDACC|20220412161126||MDMD^T02|121160|T|2.7
            EVN|T02|20220412161126
            PID|||2008661^^^EPI^MR||RADONCTEST^GISELLE||19671028|F||||||
            PV1||I
            TXA||101||20220412000000|13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|20220412161126|20220412161126||13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|||^^dpg121160_637853766870444105|||||IP||UN|||||||
            OBX|1|TX|||Date of Service: Tuesday, April 12, 2022
            OBX|2|TX|||
            OBX|3|TX|||Diagnosis: Malignant neoplasm of cervix uteri, unspecified
            OBX|4|TX|||Histopathology: 8070/3 Squamous cell carcinoma, NOS
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 MSH-9 correct separator not at index 3`() {
        val messageMDMT02 = """
            MSH|^~\&|Brocade|MDACC|Epic|MDACC|20220412161126||MDMDT02|121160|T|2.7
            EVN|T02|20220412161126
            PID|||2008661^^^EPI^MR||RADONCTEST^GISELLE||19671028|F||||||
            PV1||I
            TXA||101||20220412000000|13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|20220412161126|20220412161126||13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|||^^dpg121160_637853766870444105|||||IP||UN|||||||
            OBX|1|TX|||Date of Service: Tuesday, April 12, 2022
            OBX|2|TX|||
            OBX|3|TX|||Diagnosis: Malignant neoplasm of cervix uteri, unspecified
            OBX|4|TX|||Histopathology: 8070/3 Squamous cell carcinoma, NOS
        """.trimIndent()
        assertTrue(getMSH9("mockTenant", messageMDMT02).isEmpty())
    }

    @Test
    fun `HL7v2 good message header`() {
        val messageMDMT02 = """
            MSH|^~\&|Brocade|MDACC|Epic|MDACC|20220412161126||MDM^T02|121160|T|2.7
            EVN|T02|20220412161126
            PID|||2008661^^^EPI^MR||RADONCTEST^GISELLE||19671028|F||||||
            PV1||I
            TXA||101||20220412000000|13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|20220412161126|20220412161126||13765^CHUNG^CAROLINE^^^^^^PROVID^^^^PROVID|||^^dpg121160_637853766870444105|||||IP||UN|||||||
            OBX|1|TX|||Date of Service: Tuesday, April 12, 2022
            OBX|2|TX|||
            OBX|3|TX|||Diagnosis: Malignant neoplasm of cervix uteri, unspecified
            OBX|4|TX|||Histopathology: 8070/3 Squamous cell carcinoma, NOS
        """.trimIndent()
        assertTrue(listOf("MDM", "T02").equals(getMSH9("mockTenant", messageMDMT02)))
    }
}
