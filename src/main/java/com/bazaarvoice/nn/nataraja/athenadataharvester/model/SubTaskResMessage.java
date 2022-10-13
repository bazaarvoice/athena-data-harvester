package com.bazaarvoice.nn.nataraja.athenadataharvester.model;

import java.util.List;
import java.util.Map;

public record SubTaskResMessage (
            String requestId,
            String taskId ,
            String clientName,
            String emailId,
            TaskType taskType,
            boolean isFinished,
            Map<String , List<String> > collectedIds,
            String comment,
            String taskName
) {
    public SubTaskResMessage(HarvestTask cdMessage, boolean dataFound,
                             Map<String , List<String> > collectedIds,
                             String comment,
                             String taskName) {
            this(cdMessage.requestId(),cdMessage.taskId(),cdMessage.clientName(),cdMessage.emailId(), cdMessage.taskType() ,dataFound,collectedIds,comment,taskName);
        }

    public SubTaskResMessage(HarvestTask cdMessage, boolean isFinished, String taskName) {
            this(cdMessage.requestId(),cdMessage.taskId(),cdMessage.clientName(),cdMessage.emailId(), cdMessage.taskType() ,isFinished,null,null,taskName);
        }

    public SubTaskResMessage(HarvestTask cdMessage, boolean isFinished, String comment, String taskName) {
            this(cdMessage.requestId(),cdMessage.taskId(),cdMessage.clientName(),cdMessage.emailId(), cdMessage.taskType() ,isFinished,null,comment,taskName);
        }

    }
