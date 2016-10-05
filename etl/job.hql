use wellbook;

set hive.execution.engine=tez;

add jar /home/cluster-admin/wellbook/SequenceFileKeyValueInputFormat/target/SequenceFileKeyValueInputFormat-0.1.0-SNAPSHOT.jar;
add file /home/cluster-admin/wellbook/etl/lib/recordhelper.py;
add file /home/cluster-admin/wellbook/etl/lib/las.py;
add file /home/cluster-admin/wellbook/etl/hive/las_readings.py;
add file /home/cluster-admin/wellbook/etl/hive/las_metadata.py;

drop table if exists stage;
create external table stage(filename string, text string)
stored as inputformat 'com.github.randerzander.SequenceFileKeyValueInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '${hiveconf:SOURCE}';

create table if not exists ${hiveconf:TARGET}_errors (error string) stored as orc;

from (
  select transform(filename, text) using '${hiveconf:SCRIPT}'
    as error,${hiveconf:COLUMNS}
  from stage
) source
insert overwrite table ${hiveconf:TARGET} select ${hiveconf:COLUMNS} where error = ''
insert overwrite table ${hiveconf:TARGET}_errors select error where error != '';

