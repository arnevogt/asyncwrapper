package org.n.riesgos.asyncwrapper.process

import org.n.riesgos.asyncwrapper.process.Process

class DummyProcess : Process {

    override fun runProcess(input: String): String {
        println("dummy process: $input")
        return input;
    }
}