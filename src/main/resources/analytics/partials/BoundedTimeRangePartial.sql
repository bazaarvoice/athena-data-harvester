{{!-- Use this partial when the item you are searching for can be bounded by a lower and upper DateTime --}}
{{!-- queryRangeStart and queryRangeEnd are inclusive --}}

{{#each this}}
  (
    {{identifierName}} = '{{messageId}}'
    AND
    {{> partials/TimeFieldsToISOPartial }} BETWEEN from_iso8601_timestamp('{{queryRangeStart}}') AND from_iso8601_timestamp('{{queryRangeEnd}}')
  )
  {{#unless @last}} OR {{/unless}}
{{/each}}