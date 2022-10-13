SELECT *
FROM
  messagelifecycle
WHERE
(
  (
    LOWER(type) = 'sent'
    OR LOWER(type) = 'delivered'
    OR LOWER(type) = 'bounced'
    OR LOWER(type) = 'complaint'
    OR LOWER(type) = 'opened'
  )
  AND ( {{> partials/MinimumTimeRangePartial identifierName='messageid'}} )
)