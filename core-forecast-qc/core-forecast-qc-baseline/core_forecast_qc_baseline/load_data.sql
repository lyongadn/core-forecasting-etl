LOAD DATA FROM S3 prefix '__s3_prefix__'
INTO TABLE __database_name__.__table__
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 lines;


