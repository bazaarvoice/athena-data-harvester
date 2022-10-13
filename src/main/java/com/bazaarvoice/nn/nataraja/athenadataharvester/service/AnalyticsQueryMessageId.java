package com.bazaarvoice.nn.nataraja.athenadataharvester.service;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Getter
@Accessors (prefix = "_")
public class AnalyticsQueryMessageId {
    private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    private String _messageId;
    private OffsetDateTime _sentDateTime;
    private OffsetDateTime _queryRangeStart;
    private OffsetDateTime _queryRangeEnd;

    public AnalyticsQueryMessageId(String messageId) {
        _messageId = messageId;
        _sentDateTime = getTimeFromUUID(messageId);
        _queryRangeStart = _sentDateTime.minusHours(1);
        _queryRangeEnd = _sentDateTime.plusHours(1);
    }

    private static OffsetDateTime getTimeFromUUID(String uuidString) {
        UUID uuid = UUID.fromString(uuidString);
        if (uuid.version() != 1) { // verify that the UUID is time based
            throw new IllegalArgumentException("UUID is not time-based");
        }
        //Taken from https://support.datastax.com/hc/en-us/articles/204226019-Converting-TimeUUID-Strings-to-Dates
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli((uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000), ZoneOffset.UTC);
    }

}
