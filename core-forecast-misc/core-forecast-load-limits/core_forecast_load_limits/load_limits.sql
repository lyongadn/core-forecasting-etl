LOAD DATA FROM S3 prefix '__s3_path__'
INTO TABLE __database__.__table_name__
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES;