#!/usr/bin/env bash
# Run the in-house baseline on a remote host (an EC2 box with a vdl-tools
# checkout, its venv, Playwright Chromium and ~/config.ini), then pull the
# results back. The remote checkout only needs this package and the sample
# parquet; nothing is committed or pushed there.
#
#   remote_baseline.sh push   user@host [~/vdl/vdl-tools] [~/vdl-tools-312/bin/python]
#   remote_baseline.sh smoke  user@host ...     # 3 URLs
#   remote_baseline.sh run    user@host ...     # full sample, nohup + nice, logs to .cache/vendor_pilot/inhouse_ec2_run.log
#   remote_baseline.sh status user@host ...
#   remote_baseline.sh pull   user@host ...     # copies inhouse_ec2_* back into .cache/vendor_pilot/
#
# SSH options come from $SSH_OPTS (e.g. SSH_OPTS="-i ~/.ssh/key.pem").
set -euo pipefail

cmd=${1:?push|smoke|run|status|pull}
host=${2:?user@host}
remote_repo=${3:-'~/vdl/vdl-tools'}
remote_py=${4:-'~/vdl-tools-312/bin/python'}
pkg=vdl_tools/scrape_enrich/scraper/vendor_pilot
cache=.cache/vendor_pilot
ssh_opts=${SSH_OPTS:-}

remote() { ssh $ssh_opts -o BatchMode=yes "$host" "cd $remote_repo && export VDL_GLOBAL_CONFIG_PATH=\$HOME/config.ini && $*"; }

case $cmd in
  push)
    ssh $ssh_opts -o BatchMode=yes "$host" "mkdir -p $remote_repo/$pkg $remote_repo/$cache"
    scp $ssh_opts -q $pkg/*.py "$host:$remote_repo/$pkg/"
    scp $ssh_opts -q $cache/sample_urls.parquet "$host:$remote_repo/$cache/"
    echo "pushed $pkg and $cache/sample_urls.parquet to $host:$remote_repo"
    ;;
  smoke)
    remote "nice -n 10 $remote_py -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse --label ec2 --limit 3 2>&1 | grep -v ' - DEBUG - ' | tail -8"
    ;;
  run)
    remote "nohup nice -n 10 $remote_py -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse --label ec2 > $cache/inhouse_ec2_run.log 2>&1 &"
    echo "started; check with: $0 status $host"
    ;;
  status)
    remote "grep -v ' - DEBUG - ' $cache/inhouse_ec2_run.log | grep -E 'done,|wrote|Error|Traceback' | tail -5; pgrep -f run_inhouse >/dev/null && echo RUNNING || echo FINISHED"
    ;;
  pull)
    mkdir -p $cache
    scp $ssh_opts -q "$host:$remote_repo/$cache/inhouse_ec2_*" $cache/
    ls -la $cache/inhouse_ec2_*
    ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
