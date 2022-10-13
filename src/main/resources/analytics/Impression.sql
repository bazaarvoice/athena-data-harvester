SELECT *
FROM
  impression
WHERE
(
  ( LOWER(type) = 'pgc' )
  AND ( {{> partials/BoundedTimeRangePartial identifierName='messageid'}} )
)