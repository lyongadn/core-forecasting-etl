python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-truncate Prod-Aurora-Truncate-SlackNofication conf
python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-dl-initial Prod-Aurora-Datalake-Initial-QC-SlackNofication conf
python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-initial-to-final-dataprep-qc Prod-Aurora-Initial-Final-Dataprep-QC-SlackNofication conf
python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-inference-qc Prod-Aurora-Forecast-QC-SlackNofication conf
python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-limits-redundancy Prod-Redundancy-Slack-Alerts conf
python get-functions.py core-forecast-slack-alerts core-forecast-slack-alert-ratio-qc Prod-Ratio-QC-Slack-Notification conf
python get-functions.py core-forecast-qc core-forecast-qc-dl-initial Prod-Aurora-Datalake-Initial-QC conf
python get-functions.py core-forecast-qc core-forecast-qc-dataprep-initial-to-final Prod-Aurora-Dataprep-Initial-Final-QC conf
python get-functions.py core-forecast-qc core-forecast-qc-datalayer-staging-final Prod-DataLayer-Staging-Final-QC conf
python get-functions.py core-forecast-qc core-forecast-qc-inference Prod-Aurora-Forecast-QC conf
python get-functions.py core-forecast-qc core-forecast-qc-redundancy-inference Prod-Aurora-Redundancy-QC conf
python get-functions.py core-forecast-qc core-forecast-qc-ratio Prod-Ratio-QC-execute-lambda conf
python get-functions.py core-forecast-dataprep core-forecast-dataprep-incremental-itemcount Prod-Aurora-Incremental-Itemcount conf
python get-functions.py core-forecast-dataprep core-forecast-dataprep-incremental-dollarsandtrans Prod-Aurora-Incremental-DollarSalesAndTranscount conf
python get-functions.py core-forecast-dataprep core-forecast-inference-input-dollarsandtrans Prod-Aurora-Inference-DollarsAndTranscount-Input conf
python get-functions.py core-forecast-dataprep core-forecast-inference-input-itemcount Prod-Aurora-Inference-ItemCount-Input conf
python get-functions.py core-forecast-inference-daily core-forecast-inference-daily-sales Aurora-Inference-Daily-Sales-Prod conf
python get-functions.py core-forecast-inference-daily core-forecast-inference-daily-transcount Aurora-Inference-Transcount-Daily-Prod conf
python get-functions.py core-forecast-inference-daily core-forecast-inference-daily-itemcount Aurora-Inference-Itemcount-Prod conf
python get-functions.py core-forecast-inference-daily core-forecast-naive-daily-itemcount Aurora-Inference-Itemcount-Naive-Prod conf
python get-functions.py core-forecast-inference-daily core-forecast-inference-daily-load Aurora-Inference-Itemcount-Aurora-Load-Prod conf
python get-functions.py core-forecast-15min-breakdown core-forecast-15min-breakdown-itemcount Prod-Aurora-Forecast-ItemCount-15min conf
python get-functions.py core-forecast-15min-breakdown core-forecast-15min-breakdown-ingredie Prod-Aurora-Forecast-Ingredient-15min conf
python get-functions.py core-forecast-15min-breakdown core-forecast-15min-breakdown-dollarsandtrans Prod-Aurora-Forecast-DollarAndTranscount-15min conf
python get-functions.py core-forecast-15min-breakdown core-forecast-15min-breakdown-redundancy-dollarsandtrans Prod-Aurora-Redundancy-Forecast-Dollars conf
python get-functions.py core-forecast-15min-breakdown core-forecast-15min-breakdown-redundancy-itemanding Prod-Aurora-Redundancy-Forecast ItemIngre conf
python get-functions.py core-forecast-csvtojson core-forecast-csvtojson-2weekout Prod-Aurora-CSVtoJSON-10thday conf
python get-functions.py core-forecast-csvtojson core-forecast-csvtojson-3weekout Prod-Aurora-CSVtoJSON-30thday conf
python get-functions.py core-forecast-csvtojson core-forecast-csvtojson-redundancy Prod-Aurora-CSVtoJSON-Redundancy conf
python get-functions.py core-forecast-csvtojson core-forecast-csvtojson-fallback prod-aws-forecast-fallback-csvtojson-execute-json-generation conf