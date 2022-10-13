{{!-- Analytics data contains a `dt` field which is an ISO 8601 timestamp, we don't use this field to filter because it is a `varchar`, not `timestamp` --}}
{{!-- Queries that use `dt` field scan much more data and can potentially fail. Instead, we use `year`, `month`, `day`, `hour` fields because these fields form the partitions --}}
{{!-- Athena will be able to exclude partitions that fall outside the range --}}
{{!-- This partial returns out a Presto statement that converts `year`, `month`... to a format that can be used in a date ranged query --}}

    from_iso8601_timestamp(concat(cast(year AS varchar), '-', cast(month AS varchar),'-', cast(day AS varchar),'T', cast(hour AS varchar), ':00:00.000Z'))