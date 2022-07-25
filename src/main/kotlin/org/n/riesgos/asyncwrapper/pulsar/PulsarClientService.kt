package org.n.riesgos.asyncwrapper.pulsar

import org.apache.pulsar.client.api.PulsarClient
import org.springframework.stereotype.Service

@Service
class PulsarClientService {

    val pulsarURL : String = "pulsar://localhost:6650"

    fun createPulsarConnection() : PulsarClient{
        println("connect to pulsar at $pulsarURL")
        val pulsarClient = PulsarClient.builder()
            .serviceUrl(pulsarURL)
            .build() //build already establishs connection
        return pulsarClient
    }

}