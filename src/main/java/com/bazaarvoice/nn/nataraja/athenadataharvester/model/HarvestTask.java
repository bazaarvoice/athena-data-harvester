package com.bazaarvoice.nn.nataraja.athenadataharvester.model;

import java.time.OffsetDateTime;
import java.util.List;

public record HarvestTask(
        String requestId,
        String taskId ,
        String clientName,
        String emailId,
        TaskType taskType,
        String parentTaskId,
        OffsetDateTime submissionTime,
        List<String> identifiersList
) {
}
