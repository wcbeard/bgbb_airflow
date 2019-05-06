set -xeuo pipefail
echo "Running for submission-date==$1"
 # todo: mar 25 on to apr
DBX_TOKEN=`cat ~/dbx_token.txt`
	# --git-path https://github.com/wcbeard/python_mozetl.git \
echo `which python`
python bin/mozetl-databricks.py \
	--git-path https://github.com/wcbeard/bgbb_airflow.git \
	--git-branch pmau \
	--python 3 \
	--num-workers 5 \
	--token $DBX_TOKEN  \
	--module-name bgbb_airflow \
	bgbb_pred \
		--submission-date "$1" \
        --model-win 90 \
        --sample-ids "[42]" \
		--pred-bucket "s3://net-mozaws-prod-us-west-2-pipeline-analysis" \
		--pred-prefix "wbeard/active_profiles" \
		--param-bucket "s3://net-mozaws-prod-us-west-2-pipeline-analysis" \
		--param-prefix "wbeard/bgbb_params"
        # --model-params '[0.825,0.68,0.0876,1.385]' \

# Example logs at https://pastebin.com/3wRT4SWT
