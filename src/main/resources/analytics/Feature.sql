SELECT *
FROM
  feature
WHERE
(
  (
    (
      LOWER(type) = 'used' -- finds Click events and Unsubscribe click events
      AND LOWER(name) = 'linkclick'
    ) OR (
      LOWER(type) = 'shown' -- finds Form Open and Unsubscribe Open events
    ) OR (
      LOWER(type) = 'used' -- finds  Submission events
      AND LOWER(name) = 'submission'
      AND LOWER(detail1) = 'submissionformsubmit'
    )
  )
  AND ( {{> partials/MinimumTimeRangePartial identifierName='notificationid'}} )
)