package org.n.riesgos.asyncwrapper.process


import org.n.riesgos.asyncwrapper.config.WPSConfiguration
import org.n.riesgos.asyncwrapper.events.MessageEvent
import org.n.riesgos.asyncwrapper.process.wps.WPSClientService
import org.n.riesgos.asyncwrapper.process.wps.WPSProcess
import org.n.riesgos.asyncwrapper.pulsar.PulsarClientService
import org.n.riesgos.asyncwrapper.pulsar.PulsarPublisher
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component
import java.sql.Ref

@Component
class ProcessHandler(var publisher: PulsarPublisher, val clientService : WPSClientService,  val config : WPSConfiguration) : ApplicationListener<MessageEvent> {

    override fun onApplicationEvent(event: MessageEvent) {
        println("received message: ${event.msg}")
        val wpsClient = clientService.establishWPSConnection()
        val process = WPSProcess(wpsClient, config.wpsURL, config.process, config.version)
        val inputParam = InlineParameter("literalInput", event.msg, "text/xml");
        val input = ProcessInput("", mapOf("literalInput" to inputParam), HashMap<String, ReferenceParameter>())
        val output = process.runProcess(input)
        println("publish process output: $output")
        publisher.publishMessage(output.toString())
    }
}