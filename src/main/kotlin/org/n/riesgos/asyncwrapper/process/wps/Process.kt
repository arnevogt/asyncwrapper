package org.n.riesgos.asyncwrapper.process.wps

interface Process {
    fun runProcess(input: String) : String
}