CREATE TABLE source_table (
  f0 INT,
  f1 INT,
  f2 STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second'='5'
  );


CREATE TABLE print_table (
  f0 INT,
  f1 INT,
  f2 STRING
) WITH (
'connector' = 'print'
)


insert into print_table select f0,f1,f2 from source_table;