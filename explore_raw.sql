-- from right-click in duckdb ui
from tca_data.raw.dynamo_items_raw
select source_file,
	record_index,
	file_modified_at,
	pk,
	sk,
	payload_json,
	_dlt_load_id,
	_dlt_id
limit 100;
-- first manual explore query
from raw.dynamo_items_raw
select sk,
	count(*),
	group by sk
order by 2 desc,
	1;
-- with ai helping to get game names, hmm, basis for games dimension, hmm...
select split_part(sk, '#', 1) as sk_left,
	count(*) as cnt
from raw.dynamo_items_raw
group by sk_left
order by cnt desc,
	sk_left;
-- trying to find where notebooks stored, blah...
SELECT *
FROM duckdb_settings()
WHERE 0 = 0 -- and name = 'ui_workspace_directory';
	and name like '%directory%';