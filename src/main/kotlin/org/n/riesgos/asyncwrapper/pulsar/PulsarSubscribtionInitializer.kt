package org.n.riesgos.asyncwrapper.pulsar

import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
class PulsarSubscriptionInitializer(var consumer: PulsarConsumer) {

    @PostConstruct
    fun initSubscriptions(){
        println("start consumer in new thread")
        var consumerThread = Thread(consumer)
        consumerThread.start()
    }
}