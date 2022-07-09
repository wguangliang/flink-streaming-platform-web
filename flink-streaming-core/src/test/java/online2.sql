CREATE TABLE source_table ( f0 INT ) WITH ('connector' = 'datagen', 'rows-per-second'='5');  insert into print_table select f0,f1,f2 from source_table;

