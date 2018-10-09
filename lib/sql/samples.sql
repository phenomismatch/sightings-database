SELECT *
  FROM places
  JOIN events USING (place_id)
  JOIN counts USING (event_id)
  JOIN taxons USING (taxon_id)
 WHERE year BETWEEN 2010 AND 2014
   AND day  BETWEEN  100 AND  200
   AND lng  BETWEEN  -73 AND  -72
   AND lat  BETWEEN   40 AND   44
   AND target = 't'
 LIMIT 100;
