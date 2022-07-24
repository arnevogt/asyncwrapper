package org.n.riesgos.asyncwrapper.process.wps

import org.n52.geoprocessing.wps.client.ExecuteRequestBuilder
import org.n52.geoprocessing.wps.client.WPSClientException
import org.n52.geoprocessing.wps.client.WPSClientSession
import org.n52.geoprocessing.wps.client.model.StatusInfo
import org.n52.geoprocessing.wps.client.model.execution.Data
import java.io.IOException


class WPSProcess : Process {

    private val url: String = "http://geoprocessing.demo.52north.org:8080/wps/WebProcessingService"
    private val version: String = "1.1.0"
    private val processID: String = "org.n52.wps.server.algorithm.test.EchoProcess"

    override fun runProcess(input: String): String {
        // connect session
        val wpsClient = WPSClientSession.getInstance()
        val connected = wpsClient.connect(url, version)

        // take a look at the process description
        val processDescription = wpsClient.getProcessDescription(url, processID, version)

        // create the request, add literal input
        val executeBuilder = ExecuteRequestBuilder(processDescription)
        val parameterIn = "literalInput"
        executeBuilder.addLiteralData(parameterIn, input, version, "", "")
        val parameterOut = "literalOutput"
        executeBuilder.setResponseDocument(parameterOut, null, null, null)

        // build and send the request document

        // build and send the request document
        val executeRequest = executeBuilder.execute

        val output = wpsClient.execute(url, executeRequest, version)

        var result: org.n52.geoprocessing.wps.client.model.Result =
            if (output is org.n52.geoprocessing.wps.client.model.Result) {
                output
            } else {
                (output as StatusInfo).result
            }

        val outputs: List<Data> = result.outputs
        val stringOutput = outputs[0].asLiteralData()
        return stringOutput.value.toString()
    }
}