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



# Fit  https://pastebin.com/BvNsVYBg
# Pred https://pastebin.com/euE69Q52
