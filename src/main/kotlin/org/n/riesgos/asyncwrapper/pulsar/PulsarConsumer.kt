package org.n.riesgos.asyncwrapper.pulsar

import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.SubscriptionType
import org.n.riesgos.asyncwrapper.events.MessageEvent
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class PulsarConsumer (private val clientService: PulsarClientService, private val msgEventPublisher : ApplicationEventPublisher): Runnable{

    val topic = "input-topic"


    private val subscription : String = this.topic + "_subscription"

    override fun run() {
        val consumer = createConsumer()
        if (consumer != null) {
            receiveMessages(consumer)
        }
    }

     private fun receiveMessages(consumer : Consumer<ByteArray>){
        while (true) {
            // Wait for a message
            val msg: Message<ByteArray> = consumer.receive()
            try {
                this.msgEventPublisher.publishEvent(MessageEvent(this, String(msg.value)))
                //acknowledge message
                consumer.acknowledge(msg)
            } catch (e: Exception) {
                println(e)
            }
        }
    }

    private fun createConsumer  () : Consumer<ByteArray>? {
        val consumer = this.clientService.createPulsarConnection().newConsumer()
            .topic(this.topic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName(this.subscription)
            .subscribe()
        return consumer
    }

}