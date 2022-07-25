package org.n.riesgos.asyncwrapper.process.wps


import org.n.riesgos.asyncwrapper.events.MessageEvent
import org.n.riesgos.asyncwrapper.pulsar.PulsarPublisher
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component

@Component
class ProcessHandler(var publisher: PulsarPublisher) : ApplicationListener<MessageEvent> {

    override fun onApplicationEvent(event: MessageEvent) {
        println("received message: ${event.msg}")
        val process = DummyProcess()
        val output = process.runProcess(event.msg)
        publisher.publishMessage(output)
    }
}