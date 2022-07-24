package org.n.riesgos.asyncwrapper.events

import org.springframework.context.ApplicationEvent

data class MessageEvent(@get:JvmName("getEventSource") val source: Any , val msg: String) : ApplicationEvent(source)