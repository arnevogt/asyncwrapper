package org.n.riesgos.asyncwrapper

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RiesgosPulsarAsyncWrapperApplication

fun main(args: Array<String>) {
	runApplication<RiesgosPulsarAsyncWrapperApplication>(*args)
}
