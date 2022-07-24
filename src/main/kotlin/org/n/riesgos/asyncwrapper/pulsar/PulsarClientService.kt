package org.n.riesgos.asyncwrapper.pulsar

import org.apache.pulsar.client.api.PulsarClient
import org.springframework.stereotype.Service

@Service
class PulsarClientService {

    val pulsarURL : String = "pulsar://localhost:6650"
    val pulsarClient : PulsarClient

    init{
        println("create pulsar client")
        this.pulsarClient = PulsarClient.builder()
            .serviceUrl(pulsarURL)
            .build()
    }

}