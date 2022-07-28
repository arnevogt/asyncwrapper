package org.n.riesgos.asyncwrapper.datamanagement

import org.json.JSONObject
import org.n.riesgos.asyncwrapper.datamanagement.mapper.*
import org.n.riesgos.asyncwrapper.datamanagement.models.ComplexInputConstraint
import org.n.riesgos.asyncwrapper.datamanagement.models.ComplexOutput
import org.n.riesgos.asyncwrapper.datamanagement.utils.NamedInput
import org.n52.javaps.algorithm.annotation.ComplexInput
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.math.BigInteger
import java.util.*
import java.util.stream.Collectors


@Component
class DatamanagementRepo (val jdbcTemplate: JdbcTemplate) {
    fun orderConstraints(orderId: BigInteger): JSONObject? {
        val sql = """
            select order_constraints
            from orders
            where id=?
        """.trimIndent()

        try {
            return jdbcTemplate.queryForObject(sql, OrderConstraintsRowMapper(), orderId)
        } catch (e: EmptyResultDataAccessException) {
            return null
        }
    }

    fun hasJob(jobId: BigInteger) : Boolean {
        val sql = """
            select id
            from jobs
            where id=?
        """.trimIndent()
        try {
            jdbcTemplate.queryForObject(sql, Int::class.javaObjectType, jobId)
            return true;
        } catch (e: EmptyResultDataAccessException) {
            return false
        }
    }

    fun addJobToOrder(jobId: BigInteger, orderId: BigInteger) {
        val sql = """
            insert into order_job_refs (job_id, order_id) values (?, ?)
        """.trimIndent()

        jdbcTemplate.update(sql, jobId, orderId)
    }

    fun complexOutputs(orderId: BigInteger, processWpsIdentifier: String, outputWpsIdentifier: String): List<ComplexOutput> {
        val sql = """
            select distinct complex_outputs.*
            from complex_outputs
            join jobs on jobs.id = complex_outputs.job_id
            join order_job_refs on order_job_refs.job_id = jobs.id
            join processes on processes.id = jobs.process_id
            where order_job_refs.order_id = ?
            and processes.wps_identifier = ?
            and complex_outputs.wps_identifier = ?
        """.trimIndent()
        return jdbcTemplate.query(sql, ComplexOutputRowMapper(), orderId, processWpsIdentifier, outputWpsIdentifier)
    }

    fun hasAlreadyProcessed(
            processIdentifier: String,
            complexInputs: List<NamedInput<ComplexInputConstraint>>,
            literalInputs: List<NamedInput<String>>
    ): Boolean {
        // first search for the complex inputs that were there
        val sqlComplexInputs = """
            select complex_inputs.*
            from complex_inputs
            join jobs on jobs.id = complex_inputs.job_id
            join processes on processes.id = jobs.process_id
            where process.wps_identifier = ?
            and complex_inputs.wps_identifier = ?
            and complex_inputs.link = ?
            and complex_inputs.mime_type = ?
            and complex_inputs.xmlschema = ?
            and complex_inputs.encoding = ?
        """.trimIndent()

        val sqlComplexInputsAsValues = """
            select complex_inputs_as_values.*
            from complex_inputs_as_values
            join jobs on jobs.id = complex_inputs_as_values.job_id
            join processes on processes.id = jobs.process_id
            where process.wps_identifier = ?
            and complex_inputs_as_values.wps_identifier = ?
            and complex_inputs_as_values.input_value = ?
            and complex_inputs_as_values.mime_type = ?
            and complex_inputs_as_values.xmlschema = ?
            and complex_inputs_as_values.encoding = ?
        """.trimIndent()

       val sqlLiteralInputs = """
           select literal_inputs.*
            from literal_inputs
            join jobs on jobs.id = literal_inputs.job_id
            join processes on processes.id = jobs.process_id
            where process.wps_identifier = ?
            and literal_inputs.wps_identifier = ?
            and literal_inputs.input_value = ?
       """.trimIndent()

        var jobIdSet = HashSet<Long>()
        var jobIdSetNotSetYet = true
        for (complexInput in complexInputs) {
            val searchForThisComplexInput =
                jdbcTemplate.query(
                        sqlComplexInputs,
                        ComplexInputRowMapper(),
                        processIdentifier,
                        complexInput.name,
                        complexInput.input.link,
                        complexInput.input.mimeType,
                        complexInput.input.xmlschema,
                        complexInput.input.encoding
                )

            val searchForThisComplexInputAsValue =
                    jdbcTemplate.query(
                            sqlComplexInputsAsValues,
                            ComplexInputAsValueRowMapper(),
                            processIdentifier,
                            complexInput.name,
                            complexInput.input.inputValue,
                            complexInput.input.mimeType,
                            complexInput.input.xmlschema,
                            complexInput.input.encoding
                    )
            if (searchForThisComplexInput.isEmpty() && searchForThisComplexInputAsValue.isEmpty()) {
                return false
            }
            val referenceJobId = searchForThisComplexInput.stream().map({ x -> x.jobId}).distinct().collect(Collectors.toSet())
            val valueJobIds = searchForThisComplexInputAsValue.stream().map({ x -> x.jobId}).distinct().collect(Collectors.toSet())
            // for the complex inputs we don't want to differ, either one is as good as the other
            referenceJobId.addAll(valueJobIds)
            if (jobIdSetNotSetYet) {
                jobIdSet.addAll(referenceJobId)
                jobIdSetNotSetYet = false
            } else {
                jobIdSet.retainAll(referenceJobId)
            }

            if (jobIdSet.isEmpty()) {
                return false
            }
        }
        for (literalInput in literalInputs) {
            val searchForThisLiteralInput =
                    jdbcTemplate.query(
                            sqlLiteralInputs,
                            LiteralInputRowMapper(),
                            processIdentifier,
                            literalInput.name,
                            literalInput.input
                    )

            if (searchForThisLiteralInput.isEmpty()) {
                return false
            }
            val literalInputJobIds = searchForThisLiteralInput.stream().map({ x -> x.jobId}).distinct().collect(Collectors.toSet())
            if (jobIdSetNotSetYet) {
                jobIdSet.addAll(literalInputJobIds)
                jobIdSetNotSetYet = false
            } else {
                jobIdSet.retainAll(literalInputJobIds)
            }
            if (jobIdSet.isEmpty()) {
                return false
            }
        }

        // ok we found one or more job ids that have all the parameters
        // now it is the task to check if there were additional parameters that we don't checked yet.
        // TODO: This here doesn't consider the count of arguments. However for the moment we don't need that in RIESGOS.
        val sqlJobWpsIdentifier = """
            with cte_job as (
                select id
                from jobs
                where id = ?
            ),
            cte_input_identifier as (
                select complex_inputs.wps_identifier
                join cte_job on cte_job.id = complex_inputs.job_id
                
                union all
                
                select complex_inputs_as_values.wps_identifier
                join cte_job on cte_job.id = complex_inputs_as_values.job_id
                
                union all
                
                select literal_inputs.wps_identifier
                join cte_job on cte_job.id = literal_inputs.job_id
                
                union all
                
                select bbox_inputs.wps_identifier
                join cte_job on cte_job.id = bbox_inputs.job_id
            )
            select distinct wps_identifier
            from cte_input_identifier
        """.trimIndent()

        for (jobId in jobIdSet) {
            val usedWpsIdentifiersInThatJob = HashSet(jdbcTemplate.query(sqlJobWpsIdentifier, StringRowMapper("wps_identifier"), jobId) as List<String>)
            for (complexInput in complexInputs) {
                usedWpsIdentifiersInThatJob.remove(complexInput.name)
            }
            for (literalInput in literalInputs) {
                usedWpsIdentifiersInThatJob.remove(literalInput.name)
            }
            if (usedWpsIdentifiersInThatJob.isEmpty()) {
                // Now we have found a job id that had the same input parameters (and no others!)
                return true
            }
        }

        // either we haven't found an job that has all the identifiers, or the job had more input parameters
        return false
    }
}