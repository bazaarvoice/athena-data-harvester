package com.bazaarvoice.nn.nataraja.athenadataharvester.service;

import com.amazonaws.services.athena.model.ColumnInfo;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.QueryExecutionState;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.AthenaSupplier;
import com.bazaarvoice.nn.nataraja.athenadataharvester.util.TokenPagedData;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AthenaService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AthenaService.class);

    private static final int QUERY_POLL_MS = 5000;
    private static final int BATCH_MAX_RESULTS = 1000;
    @Value ("${athena.queryOutputLocation}")
    private String athenaQueryOutputLocation;
    @Autowired
    private AthenaSupplier amazonAthenaSupplier;


    // Submits all queries to athena and waits until all queries have finished running
    public List<String> executeQueriesParallel(String database, List<String> athenaQueries) {
        List<String> athenaQueryIds = athenaQueries.stream()
                .map(athenaQuery -> submitAthenaQuery(database, athenaQuery))
                .collect(Collectors.toList());

        for (String athenaQueryId : athenaQueryIds) {
            try {
                waitForQueryToComplete(athenaQueryId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // reset the interrupted flag
                throw new RuntimeException(e);
            }
        }
        return athenaQueryIds;
    }

    public TokenPagedData<String, Map<String, String>> getPagedResults(String queryExecutionId, @Nullable String nextToken) {
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withMaxResults(BATCH_MAX_RESULTS)
                .withQueryExecutionId(queryExecutionId)
                .withNextToken(nextToken);

        GetQueryResultsResult getQueryResultsResult = amazonAthenaSupplier.get().getQueryResults(getQueryResultsRequest);
        List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();

        Iterable<Row> resultsRows = getQueryResultsResult.getResultSet().getRows();
        resultsRows = nextToken == null ? Iterables.skip(resultsRows, 1) : resultsRows; // the first row of the first page holds the column names so skip it
        List<Map<String, String>> processedResultsRows = StreamSupport.stream(resultsRows.spliterator(), false)
                .map(row -> processRow(row, columnInfoList))
                .collect(Collectors.toList());

        return new TokenPagedData<>(processedResultsRows, getQueryResultsResult.getNextToken());
    }

    // For each row, create a map of "columnName" => "columnValue"
    private Map<String, String> processRow(Row row, List<ColumnInfo> columnInfoList) {
        Map<String, String> valuesByColumnName = new HashMap<>();
        for (int i = 0; i < columnInfoList.size(); ++i) {
            valuesByColumnName.put(columnInfoList.get(i).getName(), row.getData().get(i).getVarCharValue());
        }
        return valuesByColumnName;
    }

    // The code below was copied from https://docs.aws.amazon.com/athena/latest/ug/code-samples.html
    private String submitAthenaQuery(String database, String query) {

        // The QueryExecutionContext allows us to set the Database.
        QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(database);

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(athenaQueryOutputLocation);

        // Create the StartQueryExecutionRequest to send to Athena which will start the query.
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(query)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        StartQueryExecutionResult startQueryExecutionResult = amazonAthenaSupplier.get().startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResult.getQueryExecutionId();
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */
    private void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId);

        GetQueryExecutionResult getQueryExecutionResult = null;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResult = amazonAthenaSupplier.get().getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            }
            else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            }
            else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
                LOGGER.info("Athena query id {} scanned {}", getQueryExecutionResult.getQueryExecution().getQueryExecutionId(), FileUtils.byteCountToDisplaySize(getQueryExecutionResult.getQueryExecution().getStatistics().getDataScannedInBytes()));
            }
            else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(QUERY_POLL_MS);
            }
        }
    }
}
