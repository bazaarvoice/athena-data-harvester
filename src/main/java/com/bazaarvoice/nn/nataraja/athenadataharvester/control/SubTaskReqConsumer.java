package com.bazaarvoice.nn.nataraja.athenadataharvester.control;

import com.amazonaws.services.sqs.model.Message;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.HarvestTask;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.SubTaskResMessage;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.TaskType;
import com.bazaarvoice.nn.nataraja.athenadataharvester.service.EventService;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.FileManager;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.TaskFileCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping ("/sqs")
public class SubTaskReqConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubTaskReqConsumer.class);

    @Value("${custom.req-queue-name}")
    private String reqQueueName;

    @Value("${custom.res-queue-name}")
    private String respQueueName ;

    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;

    @Autowired
    private EventService eventService;

    @Value("${custom.taskName}")
    private String taskName;

    private FileManager fileManager = new FileManager("/target");

    @SqsListener(value = "${custom.req-queue-name}" , deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void queueListener(Message message) {
        LOGGER.info("Received SQS message in 2 {}", message.toString());
        ObjectMapper objectMapper = new ObjectMapper();
        HarvestTask task = null;

        try {
            task = objectMapper.readValue(message.getBody(), HarvestTask.class);
        } catch (JsonProcessingException e) {
            LOGGER.error("not able to parse the msg"+ message.getBody());
        }

        SubTaskResMessage response = null;
        TaskFileCreator taskFileCreator = new TaskFileCreator(objectMapper, task, fileManager);
        if(task.taskType() == TaskType.COLLECT_DATA){
            response =  eventService.fetchEvents(task, taskFileCreator);
            response = new SubTaskResMessage(task,true,"COMPLETED", taskName) ;
        }else{
            // no-op for removeData
            // link between email address and messageId will be broken in RolodexCorrespondenceHarvester and LetterpressRequestHarvester
            response = new SubTaskResMessage(task,true,"COMPLETED", taskName) ;
        }
        this.queueMessagingTemplate.convertAndSend(respQueueName, response);

    }

    @RequestMapping(value = "/message-processing-queue", method = RequestMethod.POST)
    @ResponseStatus (HttpStatus.OK)
    public void sendMessageToMessageProcessingQueue(@RequestBody HarvestTask message) {
        LOGGER.debug("Going to send message {} over SQS", message);
        this.queueMessagingTemplate.convertAndSend(reqQueueName, message);
    }
}