DBX_TOKEN=`cat ~/dbx_token.txt`
	# --git-path https://github.com/wcbeard/python_mozetl.git \
echo `which python`
python bin/mozetl-databricks.py \
	--git-path https://github.com/wcbeard/bgbb_airflow.git \
	--git-branch read_params \
	--python 3 \
	--num-workers 5 \
	--token $DBX_TOKEN  \
	--module-name bgbb_airflow \
	bgbb_fit \
		--submission-date "2019-03-01" \
        --start-params '[0.825,0.68,0.0876,1.385]' \
		--sample-fraction 0.05


# logs at https://pastebin.com/pRzW5eUH


Failed pred: job-1588-run-1
https://dbc-caf9527b-e073.cloud.databricks.com/#/setting/clusters/0315-194910-gusto390/configuration

<Fix>
New fit: job-1589-run-1
https://dbc-caf9527b-e073.cloud.databricks.com/#/setting/clusters/0315-204700-wire392/driverLogs

https://pastebin.com/6PRUpWag


New preds: job-1591-run-1
https://pastebin.com/756MGhPT
