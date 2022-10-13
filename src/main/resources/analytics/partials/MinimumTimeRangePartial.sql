{{!-- Use this partial when the item you are searching for can only be bounded by a lower DateTime bound --}}
{{!-- This ensures that we don't scan over any more data than we need to --}}

{{#each this}}
  (
    {{identifierName}} = '{{messageId}}'
    AND
    {{> partials/TimeFieldsToISOPartial }} > from_iso8601_timestamp('{{queryRangeStart}}')
  )
  {{#unless @last}} OR {{/unless}}
{{/each}}