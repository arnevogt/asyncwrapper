package org.n.riesgos.asyncwrapper.process.wps

import org.n.riesgos.asyncwrapper.process.Process
import org.n52.geoprocessing.wps.client.ExecuteRequestBuilder
import org.n52.geoprocessing.wps.client.WPSClientSession
import org.n52.geoprocessing.wps.client.model.StatusInfo
import org.n52.geoprocessing.wps.client.model.execution.Data


class WPSProcess(private val wpsClient : WPSClientSession, private val url: String, private val processID: String, private val wpsVersion: String) : Process {

    override fun runProcess(input: String): String {

        // take a look at the process description
        val processDescription = wpsClient.getProcessDescription(url, processID, wpsVersion)

        // create the request, add literal input
        val executeBuilder = ExecuteRequestBuilder(processDescription)
        val parameterIn = "literalInput"
        executeBuilder.addLiteralData(parameterIn, input, wpsVersion, "", "text/xml")
        val parameterOut = "literalOutput"
        executeBuilder.setResponseDocument(parameterOut, null, null, "text/xml")

        // build and send the request document

        // build and send the request document
        val executeRequest = executeBuilder.execute

        val output = wpsClient.execute(url, executeRequest, wpsVersion)

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