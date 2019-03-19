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
	bgbb_pred \
		--submission-date "2019-02-15" \
        --model-win 120 \
        --sample-ids "[0]"
        # --model-params '[0.825,0.68,0.0876,1.385]' \

# Example logs at https://pastebin.com/3wRT4SWT
