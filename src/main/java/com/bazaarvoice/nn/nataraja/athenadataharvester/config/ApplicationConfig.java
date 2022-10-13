package com.bazaarvoice.nn.nataraja.athenadataharvester.config;

import io.awspring.cloud.messaging.listener.SimpleMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@Component
@Configuration
@EnableScheduling
public class ApplicationConfig implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private SimpleMessageListenerContainer simpleMessageListenerContainer;

    private static final Logger log = LoggerFactory.getLogger(ApplicationConfig.class);

    @Value ("${custom.req-queue-name}")
    private String queueName;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.applicationContext = applicationContext;
        this.simpleMessageListenerContainer = simpleMessageListenerContainer = applicationContext.getBean(SimpleMessageListenerContainer.class);

    }

    //@Scheduled(fixedDelay = 120000,initialDelay =  120000)
    public void scheduleFixedDelayTask() {
        log.info(
                "Fixed delay task - " + System.currentTimeMillis() / 1000);
        if (simpleMessageListenerContainer.isRunning(queueName)) {
            log.info(queueName + "  is getting stopped as it was running");
            simpleMessageListenerContainer.stop(queueName);
        } else {
            log.info(queueName + "  is getting start as it was stopped");
            simpleMessageListenerContainer.start(queueName);
        }

        log.info(" sqs listener status  " + simpleMessageListenerContainer.isRunning());
        log.info("my-sqs listener status  " + simpleMessageListenerContainer.isRunning(queueName));
    }
}
