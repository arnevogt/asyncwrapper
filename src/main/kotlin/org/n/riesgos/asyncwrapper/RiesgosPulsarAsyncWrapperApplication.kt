package org.n.riesgos.asyncwrapper

import org.n.riesgos.asyncwrapper.config.PulsarConfiguration
import org.n.riesgos.asyncwrapper.config.WPSConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(PulsarConfiguration::class, WPSConfiguration::class)
class RiesgosPulsarAsyncWrapperApplication

fun main(args: Array<String>) {
	runApplication<RiesgosPulsarAsyncWrapperApplication>(*args)
}
