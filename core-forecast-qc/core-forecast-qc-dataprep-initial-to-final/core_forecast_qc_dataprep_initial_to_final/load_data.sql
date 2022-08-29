LOAD DATA FROM S3 prefix 's3-us-east-1://__prod_bucket__/__upload_path__/'
                						INTO TABLE __database_name__.__table__
                						FIELDS TERMINATED BY ','
                							ENCLOSED BY '"'
                								LINES TERMINATED BY '\n'
                									IGNORE 1 LINES;