package com.bazaarvoice.nn.nataraja.athenadataharvester.service;

import com.bazaarvoice.nn.nataraja.athenadataharvester.exception.TaskFailureException;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.HarvestTask;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.SubTaskResMessage;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.CollectedDataAndIds;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.TaskFileCreator;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.TokenPager;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.WrappedSequenceWriter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public class EventServiceImpl implements EventService {
    private static final Logger log = LoggerFactory.getLogger(EventServiceImpl.class);
    private static final int MESSAGE_IDS_PER_BATCH_QUERY = 50;
    private static final String MESSAGE_LIFECYCLE_TABLE_NAME = "messagelifecycle";
    private static final String FEATURE_TABLE_NAME = "feature";
    private static final String IMPRESSION_TABLE_NAME = "impression";

    // NN-4937 and AR-1886: For an unknown reason, Athena will sometimes create partition names without padding with "0" prefix
    // For example, Athena will report "year=2018/month=6/day=29/hour=0" instead of "year=2018/month=06/day=29/hour=00".
    // To account for this, we will check both patterns
    private static final DateTimeFormatter ZERO_PADDED_PARTITION_NAME_FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral("year=").appendValue(YEAR, 4)
            .appendLiteral("/month=").appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral("/day=").appendValue(DAY_OF_MONTH, 2)
            .appendLiteral("/hour=").appendValue(HOUR_OF_DAY, 2)
            .toFormatter();
    private static final DateTimeFormatter PARTITION_NAME_FORMATTER = new DateTimeFormatterBuilder()
            .appendLiteral("year=").appendValue(YEAR)
            .appendLiteral("/month=").appendValue(MONTH_OF_YEAR)
            .appendLiteral("/day=").appendValue(DAY_OF_MONTH)
            .appendLiteral("/hour=").appendValue(HOUR_OF_DAY)
            .toFormatter();

    // Analytics & Reporting team will guarantee all Magpie raw logs after this date will be converted to parquet format (and so, are queryable through Athena)
    // Some magpie raw logs prior to this date may have never have been converted to parquet format. Those raw logs will not be queryable through Athena
    // But, all magpie raw logs will be deleted after 6 months (in production, or 1 month in staging) so there will be no data to report for those missing parquet files
    @Value("${athena.magpieRetentionStartDate}")
    private OffsetDateTime magpieRetentionStartDate;
    @Value("${athena.databaseName}")
    private String athenaDatabaseName;
    @Autowired
    private AthenaService athenaService;

    @Value("${custom.res-queue-name}")
    private   String resQueueName;

    @Value("${custom.taskFileName}")
    private String taskFileName;

    @Autowired
    private AmazonS3Service amazonS3Service;

    @Autowired
    private QueryFormatter queryFormatter;

    @Override
    @SneakyThrows
    public SubTaskResMessage fetchEvents(HarvestTask harvestTask, TaskFileCreator fileCreator){
//            List<Event> result =  this.eventRepo.getEventsByUserEmail(cdMessage.clientName(),cdMessage.emailId());
//            log.debug("About to send res from fetchEvents ,  count {}", result.size() );
//            if ( result.size() > 0 ){
//                InputStream targetStream = new ByteArrayInputStream(mapper.writeValueAsString(result).getBytes());
//                this.amazonS3Service.writeFile(s3BucketName, generateFileName(cdMessage),
//                        targetStream);
//            }
//            SubTaskResMessage res = new SubTaskResMessage(cdMessage,true,"Completed",emailHasTaskName) ;
//            log.debug(" Result :"+ res);
//            this.queueMessagingTemplate.convertAndSend(resQueueName, res);
//            return res;


        ensureAllPartitionsAvailable(MESSAGE_LIFECYCLE_TABLE_NAME, magpieRetentionStartDate, harvestTask.submissionTime());
        ensureAllPartitionsAvailable(FEATURE_TABLE_NAME, magpieRetentionStartDate, harvestTask.submissionTime());
        ensureAllPartitionsAvailable(IMPRESSION_TABLE_NAME, magpieRetentionStartDate, harvestTask.submissionTime());

        try (WrappedSequenceWriter writer = fileCreator.openFileForArray()) {
            // batch message ids to prevent any single query from being too long or running for too long
            // Athena will kill queries running over 30 minutes
            for(List<String> identifierBatch : Lists.partition(harvestTask.identifiersList(), MESSAGE_IDS_PER_BATCH_QUERY)) {
                List<String> queries = queryFormatter.getFormattedDataQueries(identifierBatch);

                Stopwatch timer = Stopwatch.createStarted();
                List<String> completedAthenaQueryIds = athenaService.executeQueriesParallel(athenaDatabaseName, queries);
                log.info("Batch query group completed, took: " + timer.stop());

                for(String completedAthenaQueryId : completedAthenaQueryIds) {
                    Iterable<List<Map<String, String>>> pages =
                            new TokenPager<String, Map<String, String>>((id) -> athenaService.getPagedResults(completedAthenaQueryId, id.orElse(null)));
                    for(List<Map<String, String>> page : pages) {
                        writer.writeAll(page);
                    }
                }
            }
        } catch (IOException ioe) {
            throw new TaskFailureException("Failed to write query results to disk", ioe);
        }

        CollectedDataAndIds collectedDataAndIds = CollectedDataAndIds.dataOnly(fileCreator.getFiles());
        for (Path path : collectedDataAndIds.getCollectedData()) {
            amazonS3Service.uploadFile(harvestTask.clientName(), harvestTask.taskId(), taskFileName + ".json", path);
        }

        return null;
    }



    /**
     * Verify all hourly partitions available for the table
     */
    public void ensureAllPartitionsAvailable(String table, OffsetDateTime fromDateTime, OffsetDateTime toDateTime)
            throws Exception {
        String partitionQuery = queryFormatter.getFormattedPartitionQuery(table);
        String completedAthenaQueryId = athenaService.executeQueriesParallel(athenaDatabaseName, Collections.singletonList(partitionQuery)).get(0);

        Iterable<List<Map<String, String>>> pages = new TokenPager<String, Map<String, String>>(nextToken -> athenaService.getPagedResults(completedAthenaQueryId, nextToken.orElse(null)));

        Set<String> availablePartitions = new HashSet<>();
        for(List<Map<String, String>> page: pages) {
            page.stream()
                    .map(partitionRow -> partitionRow.get("partition"))
                    .forEach(availablePartitions::add);
        }

        OffsetDateTime checkDate = fromDateTime;
        List<String> missingPartitions = new ArrayList<>();
        while(checkDate.isBefore(toDateTime) || checkDate.equals(toDateTime)) {
            // Either of these partition formats needs to be in `availablePartitions`
            String zeroPaddedPartitionName = ZERO_PADDED_PARTITION_NAME_FORMATTER.format(checkDate);
            String partitionName = PARTITION_NAME_FORMATTER.format(checkDate);
            if(!availablePartitions.contains(zeroPaddedPartitionName) && !availablePartitions.contains(partitionName)) {
                missingPartitions.add(zeroPaddedPartitionName);
            }
            checkDate = checkDate.plusHours(1);
        }

        if(missingPartitions.size() > 0) {
            throw new Exception(String.format("Athena database: %s, table: %s is missing %d partitions: %s", athenaDatabaseName, table, missingPartitions.size(), missingPartitions.toString()));
        }
    }

}
