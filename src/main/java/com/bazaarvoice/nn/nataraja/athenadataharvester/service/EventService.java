package com.bazaarvoice.nn.nataraja.athenadataharvester.service;


import com.bazaarvoice.nn.nataraja.athenadataharvester.model.HarvestTask;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.SubTaskResMessage;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.TaskFileCreator;
import lombok.SneakyThrows;

public interface EventService {

    @SneakyThrows
    SubTaskResMessage fetchEvents(HarvestTask harvestTask, TaskFileCreator fileCreator);
}