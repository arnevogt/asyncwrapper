package org.n.riesgos.asyncwrapper.dummy

import org.json.JSONObject
import org.n.riesgos.asyncwrapper.datamanagement.DatamanagementRepo
import org.n.riesgos.asyncwrapper.datamanagement.models.ComplexInputConstraint
import org.n.riesgos.asyncwrapper.datamanagement.utils.NamedInput
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.math.BigInteger
import java.util.*

@RestController
class DummyController (val jdbcTemplate: JdbcTemplate, val datamanagementRepo: DatamanagementRepo) {



    @GetMapping("/ordersConstraints/{orderId}")
    fun showOrderConstraints (@PathVariable(value="orderId") orderId: BigInteger): String {
        val jsonObject = datamanagementRepo.orderConstraints(orderId)
        if (jsonObject == null) {
            return "not found"
        }
        return jsonObject.toString()
    }

    @GetMapping("/run/shakyground/{orderId}")
    fun runShakyground (@PathVariable(value="orderId") orderId: BigInteger): String {

        val jsonObject = datamanagementRepo.orderConstraints(orderId)
        if (jsonObject == null) {
            return "no order found"
        }

        if (!jsonObject.has("shakyground")) {
            return "no order constraints found for the process"
        }

        val shakygroundConstraints = jsonObject.getJSONObject("shakyground")


        /*
        The constraints could look like this:

        {"shakyground": {"complex_inputs": {"quakeMLFile": [{"link": "https://bla", "encoding": "UTF-8", "mime_type": "application/json", "xmlschema": ""}]}, "literal_inputs": {"gmpe": ["Abrahamson", "Montalval"], "vsgrid": ["usgs", "micro"]}}}
        
         */
        if (shakygroundConstraints.has("job_id")) {
            val jobId = shakygroundConstraints.getBigInteger("job_id")

            // now we have the job id
            // all that we want to do now, is that we associate our new
            // order with the old job

            if (! datamanagementRepo.hasJob(jobId)) {
                return "job does not exists"
            }

            datamanagementRepo.addJobToOrder(jobId, orderId)

            return "success"
        }
        // extract the values from the constraints
        var gmpeConstraints = Arrays.asList("Abrahamson", "Montalva")
        var vsgridConstraints = Arrays.asList("usgs", "microzonation")

        if (shakygroundConstraints.has("literal_inputs")) {
            val literalInputConstraints = shakygroundConstraints.getJSONObject("literal_inputs")
            if (literalInputConstraints.has("gmpe")) {
                gmpeConstraints = ArrayList<String>()
                val gmpeConstraintsArray = literalInputConstraints.getJSONArray("gmpe")
                for (gmpeConstraintObject in gmpeConstraintsArray) {
                    gmpeConstraints.add(gmpeConstraintObject as String)
                }
            }
            if (literalInputConstraints.has("vsgrid")) {
                vsgridConstraints = ArrayList<String>()
                val vsGridConstaintsArray = literalInputConstraints.getJSONArray("vsgrid")
                for (vsGridConstraintObject in vsGridConstaintsArray) {
                    vsgridConstraints.add(vsGridConstraintObject as String)
                }
            }
        }

        // Ok now we have the literal constraints.
        // But we still want to check for a constraint for our quakeml file

        var quakeMlConstraints = ArrayList<ComplexInputConstraint>()

        if (shakygroundConstraints.has("complex_inputs")) {
            val complexInputConstraints = shakygroundConstraints.getJSONObject("complex_inputs")
            if (complexInputConstraints.has("quakeMLFile")) {
                quakeMlConstraints = ArrayList<ComplexInputConstraint>()
                val quakeMlFileContraintsArray = complexInputConstraints.getJSONArray("quakeMLFile")
                for (complexInputConstraintRawObject in quakeMlFileContraintsArray) {
                    val complexInputConstraintObject = complexInputConstraintRawObject as JSONObject
                    quakeMlConstraints.add(
                            ComplexInputConstraint(
                                    complexInputConstraintObject.getString("link"), // or null
                                    complexInputConstraintObject.getString("input_value"), // or null
                                    complexInputConstraintObject.getString("mime_type"),
                                    complexInputConstraintObject.getString("xmlschema"), // or empty
                                    complexInputConstraintObject.getString("encoding")
                            )
                    )
                }
            }
        }

        if (quakeMlConstraints.isEmpty()) {
            // now we need to search in the database for inputs that we could use
            var existingQuakeMLOutputs = datamanagementRepo.complexOutputs(orderId, "Quakeledger", "quakeMLFile")
            for (existingQuakeMLOutput in existingQuakeMLOutputs) {
                quakeMlConstraints.add(ComplexInputConstraint(
                        existingQuakeMLOutput.link,
                        null,
                        existingQuakeMLOutput.mimeType,
                        existingQuakeMLOutput.xmlschema,
                        existingQuakeMLOutput.encoding
                ))
            }
        }

        // now we have all the constraints.
        // now it the point to make all possible combinations out of it.
        for (gmpeConstraint in gmpeConstraints) {
            for (vsgridConstraint in vsgridConstraints) {
                for (quakeMLConstaint in quakeMlConstraints) {
                    // if one of the lists is empty, we are not going to process anything
                    //
                    // test if we already used this combo for one job.
                    // if so, just send the success & test the next
                    // if not, start the processing
                    if (datamanagementRepo.hasAlreadyProcessed(
                                    "Shakyground",
                                    Arrays.asList(
                                            NamedInput("quakeMLFile", quakeMLConstaint)
                                    ),
                                    Arrays.asList(
                                            NamedInput("gmpe", gmpeConstraint),
                                            NamedInput("vsgrid", vsgridConstraint)
                                    )
                            )
                    ) {
                        // send success
                        // no need to recalculate it
                    } else {
                        // now we have all the data that we need
                        // and we now that we need to process it ourselves.
                        // We have to
                        // 1. Check if we need to put the process in the database.
                        // 2. Add a job to the database
                        // 3. Add the inputs
                        // 4. Add the reference to the order
                        // 5. Run the wps client with our input data
                        // 6. Update the status of the job to running
                        // 7. Wait the process to end
                        // 8. Update the status of the job either to successful or failed (lookup the WPS wording)
                        // 9. Set the outputs in the database.
                        // 10. send a success
                    }
                }
            }
        }


        return gmpeConstraints.toString()
    }
}

