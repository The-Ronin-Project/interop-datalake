package com.projectronin.interop.datalake.spring

import com.projectronin.interop.datalake.DatalakePublishService
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.getBean
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension

@ExtendWith(SpringExtension::class)
@ContextConfiguration(classes = [DatalakeSpringConfig::class, TestConfig::class])
class DatalakeSpringConfigTest {
    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Test
    fun `loads DatalakePublishService`() {
        val service = applicationContext.getBean<DatalakePublishService>()
        assertNotNull(service)
        assertInstanceOf(DatalakePublishService::class.java, service)
    }
}

@Configuration
class TestConfig {
    @Bean
    fun threadPoolTaskExecutor() = mockk<ThreadPoolTaskExecutor>(relaxed = true)
}
