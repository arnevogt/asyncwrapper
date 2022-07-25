package org.n.riesgos.asyncwrapper.process.wps

class DummyProcess : org.n.riesgos.asyncwrapper.process.wps.Process {

    override fun runProcess(input: String): String {
        println("dummy process: $input")
        return input;
    }
}