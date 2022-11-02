WITH cte_tag AS(
		 SELECT i.test_id,
		    CAST('[' || string_agg(DISTINCT '"' || t.title || '"', ', ') || ']' AS jsonb) AS tags_info
		 FROM tag_to_test i
		 LEFT JOIN tag t ON i.tag_id=t.id		 
		 GROUP BY i.test_id )
SELECT to_jsonb(t.id) as id,to_jsonb(t.title) as name,
       to_jsonb(i.id) as temp_id
	   ,c.tags_info as tags
	   ,to_jsonb(s.sample_id) as material_id,(t.meta->'metadata'->'Results' ) as results
FROM test t join instrument i on i.id=t.instrument_id
JOIN sample_to_test s on t.id=s.test_id
LEFT JOIN cte_tag c on c.test_id=t.id
		
		