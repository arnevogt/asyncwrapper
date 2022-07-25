package org.n.riesgos.asyncwrapper.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration


@ConfigurationProperties(prefix = "pulsar")
@ConstructorBinding
data class PulsarConfiguration(var pulsarURL: String, var inputTopic: String, var outputTopic: String)
