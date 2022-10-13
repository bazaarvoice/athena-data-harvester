package com.bazaarvoice.nn.nataraja.athenadataharvester.service;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryFormatter {

    private static final String QUERY_TEMPLATE_PREFIX = "/analytics";
    private static final String QUERY_TEMPLATE_SUFFIX = ".sql";
    private static final String PARTITION_TEMPLATE = "Partition";
    private static final String MESSAGE_LIFECYCLE_TEMPLATE = "MessageLifecycle";
    private static final String IMPRESSION_TEMPLATE = "Impression";
    private static final String FEATURE_TEMPLATE = "Feature";

    private Template _partitionTemplate;
    private Template _messageLifecycleTemplate;
    private Template _impressionTemplate;
    private Template _featureTemplate;

    public QueryFormatter(){
        TemplateLoader templateLoader = new ClassPathTemplateLoader();
        templateLoader.setSuffix(QUERY_TEMPLATE_SUFFIX);
        templateLoader.setPrefix(QUERY_TEMPLATE_PREFIX);
        Handlebars handlebars = new Handlebars(templateLoader);

        try {
            _partitionTemplate = handlebars.compile(PARTITION_TEMPLATE);
            _messageLifecycleTemplate = handlebars.compile(MESSAGE_LIFECYCLE_TEMPLATE);
            _impressionTemplate = handlebars.compile(IMPRESSION_TEMPLATE);
            _featureTemplate = handlebars.compile(FEATURE_TEMPLATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public List<String> getFormattedDataQueries(List<String> messageIds) {
        final List<AnalyticsQueryMessageId> analyticsQueryMessageIds = messageIds
                .stream()
                .map(AnalyticsQueryMessageId::new)
                .collect(Collectors.toList());

        List<String> queries = new ArrayList<>();

        try {
            queries.add(_messageLifecycleTemplate.apply(analyticsQueryMessageIds));
            queries.add(_impressionTemplate.apply(analyticsQueryMessageIds));
            queries.add(_featureTemplate.apply(analyticsQueryMessageIds));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load query template resource", e);
        }

        return queries;
    }

    public String getFormattedPartitionQuery(String table) {
        try {
            return _partitionTemplate.apply(table);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load partition template resource", e);
        }
    }
}
