SELECT *
  FROM places
  JOIN events USING (place_id)
  JOIN counts USING (event_id)
  JOIN taxa USING (taxon_id)
 WHERE year = 2014
   AND day  BETWEEN  100 AND  110
   AND lng  BETWEEN  -73 AND  -72
   AND lat  BETWEEN   40 AND   41
   AND target = 't';


SELECT *
  FROM places
  JOIN events USING (place_id)
  JOIN counts USING (event_id)
  JOIN taxa USING (taxon_id)
 WHERE year = 2014
   AND day  BETWEEN  100 AND  110
   AND lng  BETWEEN  -73 AND  -72
   AND lat  BETWEEN   40 AND   41
   AND class  = 'lepidoptera'
   AND target = 't';


SELECT lng, lat, year, day, count, sci_name,
       event_json ->> 'GROUP_IDENTIFIER' AS group_identifier
  FROM places
  JOIN events USING (place_id)
  JOIN counts USING (event_id)
  JOIN taxa USING (taxon_id)
 WHERE dataset_id = 'ebird'
   AND year = 2014
   AND day  BETWEEN  100 AND  110
   AND lng  BETWEEN  -73 AND  -72
   AND lat  BETWEEN   40 AND   41
   AND target = 't'
   AND event_json -> 'GROUP_IDENTIFIER' IS NOT NULL;


WITH checklists AS (
SELECT event_json ->> 'SAMPLING_EVENT_IDENTIFIER' AS sample_id
  FROM places
  JOIN events USING (place_id)
 WHERE places.dataset_id = 'ebird'
   AND year >= 2010
   AND day  BETWEEN  30 AND  150
   AND lng  BETWEEN  -80.430375 AND  -80.124475
   AND lat  BETWEEN   25.956546 AND   25.974140
   AND event_json -> 'SAMPLING_EVENT_IDENTIFIER' IS NOT NULL)
SELECT COUNT(*) AS n
  FROM checklists;
