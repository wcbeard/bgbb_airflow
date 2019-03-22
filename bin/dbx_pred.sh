DBX_TOKEN=`cat ~/dbx_token.txt`
	# --git-path https://github.com/wcbeard/python_mozetl.git \
echo `which python`
python bin/mozetl-databricks.py \
	--git-path https://github.com/wcbeard/bgbb_airflow.git \
	--git-branch samp_id \
	--python 3 \
	--num-workers 5 \
	--token $DBX_TOKEN  \
	--module-name bgbb_airflow \
	bgbb_pred \
		--submission-date "2019-02-15" \
        --model-win 20 \
        --sample-ids "[42]" \
		--pred-bucket "s3://net-mozaws-prod-us-west-2-pipeline-analysis" \
		--pred-prefix "wbeard/bgbb_preds" \
		--param-bucket "s3://net-mozaws-prod-us-west-2-pipeline-analysis" \
		--param-prefix "wbeard/bgbb_params"
        # --model-params '[0.825,0.68,0.0876,1.385]' \

# Example logs at https://pastebin.com/3wRT4SWT
