#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Import common code
from sysadmws_common import *
import gitlab
import glob
import textwrap
import subprocess
import pytz
from datetime import datetime
from datetime import time
import psycopg2
import base64

# Constants and envs

LOGO="Jobs"
WORK_DIR = os.environ.get("ACC_WORKDIR", "/opt/sysadmws/accounting")
LOG_DIR = os.environ.get("ACC_LOGDIR", "/opt/sysadmws/accounting/log")
LOG_FILE = "jobs.log"
TARIFFS_SUBDIR = "tariffs"
CLIENTS_SUBDIR = "clients"
YAML_GLOB = "*.yaml"
YAML_EXT = "yaml"
ACC_YAML = "accounting.yaml"
LOCK_TIMEOUT = 600 # Supposed to be run each 10 minutes, so lock for 10 minutes
MINUTES_JITTER = 10 # Jobs are run on some minute between 00 and 10 minutes each 10 minutes

# Main

if __name__ == "__main__":

    # Set parser and parse args
    parser = argparse.ArgumentParser(description='{LOGO} functions.'.format(LOGO=LOGO))

    parser.add_argument("--debug", dest="debug", help="enable debug", action="store_true")
    parser.add_argument("--ignore-jobs-disabled",
                          dest="ignore_jobs_disabled",
                          help="ignore jobs_disabled if set in yaml",
                          action="store_true")
    parser.add_argument("--dry-run-pipeline", dest="dry_run_pipeline", help="do not execute pipeline script", action="store_true")
    parser.add_argument("--at-date", dest="at_date", help="use DATETIME instead of now for tariff", nargs=1, metavar=("DATETIME"))

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-job", dest="run_job", help="run specific job id JOB for asset ASSET (use ALL for all assets) via GitLab pipelines for CLIENT (use ALL for all clients)", nargs=3, metavar=("CLIENT", "ASSET", "JOB"))
    group.add_argument("--run-jobs", dest="run_jobs", help="run jobs for asset ASSET (use ALL for all assets) via GitLab pipelines for CLIENT (use ALL for all clients)", nargs=2, metavar=("CLIENT", "ASSET"))
    group.add_argument("--force-run-job", dest="force_run_job", help="force run (omit time conditions) specific job id JOB for asset ASSET (use ALL for all assets) via GitLab pipelines for CLIENT (use ALL for all clients)", nargs=3, metavar=("CLIENT", "ASSET", "JOB"))
    group.add_argument("--force-run-jobs", dest="force_run_jobs", help="force run all jobs (omit time conditions) for asset ASSET (use ALL for all assets) via GitLab pipelines for CLIENT (use ALL for all clients)", nargs=2, metavar=("CLIENT", "ASSET"))
    # This is deprecated but kept for history
    group.add_argument("--prune-run-tags", dest="prune_run_tags", help="prune all run_* tags older than AGE via GitLab API for CLIENT (use ALL for all clients)", nargs=2, metavar=("CLIENT", "AGE"))

    if len(sys.argv) > 1:
        args = parser.parse_args()
    else:
        parser.print_help()
        sys.exit(1)

    # Set logger and console debug
    if args.debug:
        logger = set_logger(logging.DEBUG, LOG_DIR, LOG_FILE)
    else:
        logger = set_logger(logging.ERROR, LOG_DIR, LOG_FILE)

    GL_ADMIN_PRIVATE_TOKEN = os.environ.get("GL_ADMIN_PRIVATE_TOKEN")
    if GL_ADMIN_PRIVATE_TOKEN is None:
        raise Exception("Env var GL_ADMIN_PRIVATE_TOKEN missing")
    
    errors = False

    # Catch exception to logger

    try:

        logger.info("Starting {LOGO}".format(LOGO=LOGO))

        # Chdir to work dir
        os.chdir(WORK_DIR)

        # Read ACC_YAML
        acc_yaml_dict = load_yaml("{0}/{1}".format(WORK_DIR, ACC_YAML), logger)
        if acc_yaml_dict is None:
            raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, ACC_YAML))
        
        # Do tasks

        if args.run_jobs or args.run_job or args.force_run_job or args.force_run_jobs:

            # Check db vars
            PG_DB_HOST = os.environ.get("PG_DB_HOST")
            if PG_DB_HOST is None:
                raise Exception("Env var PG_DB_HOST missing")

            PG_DB_PORT = os.environ.get("PG_DB_PORT")
            if PG_DB_PORT is None:
                raise Exception("Env var PG_DB_PORT missing")

            PG_DB_NAME = os.environ.get("PG_DB_NAME")
            if PG_DB_NAME is None:
                raise Exception("Env var PG_DB_NAME missing")

            PG_DB_USER = os.environ.get("PG_DB_USER")
            if PG_DB_USER is None:
                raise Exception("Env var PG_DB_USER missing")

            PG_DB_PASS = os.environ.get("PG_DB_PASS")
            if PG_DB_PASS is None:
                raise Exception("Env var PG_DB_PASS missing")

            # Connect to PG
            dsn = "host={host} port={port} dbname={dbname} user={user} password={password}".format(host=PG_DB_HOST, port=PG_DB_PORT, dbname=PG_DB_NAME, user=PG_DB_USER, password=PG_DB_PASS)
            conn = psycopg2.connect(dsn)
            cur = conn.cursor()

            # Save now once in UTC
            # We cannot take now() within run jobs loops - each job run takes ~5 secs and thats why now drifts many minutes forward
            saved_now = datetime.now(pytz.timezone("UTC"))
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                # Client file errors should not stop other clients
                try:
                
                    logger.info("Found client file: {0}".format(client_file))

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                    
                    # Skip other clients
                    if args.run_jobs:
                        run_client, run_asset = args.run_jobs
                    if args.run_job:
                        run_client, run_asset, run_job = args.run_job
                    if args.force_run_job:
                        run_client, run_asset, run_job = args.force_run_job
                    if args.force_run_jobs:
                        run_client, run_asset = args.force_run_jobs

                    if run_client != "ALL" and client_dict["name"].lower() != run_client:
                        continue

                    # Skip disabled clients
                    if not client_dict["active"]:
                        continue

                    # Skip clients without salt_project
                    if "salt_project" not in client_dict["gitlab"]:
                        logger.info("Salt project not defined for client {client}, skipping".format(client=client_dict["name"]))
                        continue
                    
                    # Skip clients with jobs disabled
                    if "jobs_disabled" in client_dict and client_dict["jobs_disabled"] and not args.ignore_jobs_disabled:
                        logger.info("Jos disabled for client {client}, skipping".format(client=client_dict["name"]))
                        continue

                    # Get GitLab project for client
                    project = gl.projects.get(client_dict["gitlab"]["salt_project"]["path"])
                    logger.info("Salt project {project} for client {client} ssh_url_to_repo: {ssh_url_to_repo}, path_with_namespace: {path_with_namespace}".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"], path_with_namespace=project.path_with_namespace, ssh_url_to_repo=project.ssh_url_to_repo))

                    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                    # For each asset
                    for asset in asset_list:

                        # Asset errors should not stop other assets
                        try:

                            # Skip assets if needed
                            if run_asset != "ALL" and asset["fqdn"] != run_asset:
                                continue

                            # Skip non-server assets
                            if asset["kind"] != "server":
                                continue
                            
                            # Skip assets with jobs disabled
                            if "jobs_disabled" in asset and asset["jobs_disabled"] and not args.ignore_jobs_disabled:
                                logger.info("Jos disabled for asset {asset}, skipping".format(asset=asset["fqdn"]))
                                continue
                            
                            # Skip not active assets
                            if "active" in asset and not asset["active"]:
                                logger.info("Asset {asset} is not active, skipping".format(asset=asset["fqdn"]))
                                continue
                            
                            # Build job list
                            job_list = []

                            # Add global jobs from accounting yaml
                            if "jobs" in acc_yaml_dict:
                                
                                for job_id, job_params in acc_yaml_dict["jobs"].items():
                                    
                                    # Do not add if the same job exists in client jobs or asset jobs
                                    if not (("jobs" in client_dict and job_id in client_dict["jobs"]) or ("jobs" in asset and job_id in asset["jobs"])):
                                        job_params["id"] = job_id
                                        job_params["level"] = "GLOBAL"
                                        job_list.append(job_params)

                            # Add client jobs from client yaml
                            if "jobs" in client_dict:
                                
                                for job_id, job_params in client_dict["jobs"].items():
                                    
                                    # Do not add if the same job exists in asset jobs
                                    if not ("jobs" in asset and job_id in asset["jobs"]):
                                        job_params["id"] = job_id
                                        job_params["level"] = "CLIENT"
                                        job_list.append(job_params)

                            # Add asset jobs from asset def in client yaml
                            if "jobs" in asset:
                                
                                for job_id, job_params in asset["jobs"].items():
                                    job_params["id"] = job_id
                                    job_params["level"] = "ASSET"
                                    job_list.append(job_params)

                            # Run jobs from job list

                            logger.info("Job list for asset {asset}:".format(asset=asset["fqdn"]))
                            logger.info(json.dumps(job_list, indent=4, sort_keys=True))

                            for job in job_list:

                                # Check os include
                                if "os" in job and "include" in job["os"]:
                                    if asset["os"] not in job["os"]["include"]:
                                        logger.info("Job {asset}/{job} skipped because os {os} is not in job os include list".format(asset=asset["fqdn"], job=job["id"], os=asset["os"]))
                                        continue

                                # Check os exclude
                                if "os" in job and "exclude" in job["os"]:
                                    if asset["os"] in job["os"]["exclude"]:
                                        logger.info("Job {asset}/{job} skipped because os {os} is in job os exclude list".format(asset=asset["fqdn"], job=job["id"], os=asset["os"]))
                                        continue

                                # Check job is disabled
                                if "disabled" in job and job["disabled"]:
                                    logger.info("Job {asset}/{job} skipped because it is disabled".format(asset=asset["fqdn"], job=job["id"]))
                                    continue

                                # Check licenses
                                if "licenses" in job:
                                    
                                    logger.info("Job {asset}/{job} requires license list {lic_list_job}, loading licenses in tariffs".format(asset=asset["fqdn"], job=job["id"], lic_list_job=job["licenses"]))

                                    # Load tariffs

                                    # Take the first (upper and current) tariff
                                    all_tar_lic_list = []
                                    for asset_tariff in activated_tariff(asset["tariffs"], datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now(), logger)["tariffs"]:

                                        # If tariff has file key - load it
                                        if "file" in asset_tariff:
                                            
                                            tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                                            if tariff_dict is None:
                                                
                                                raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                                            # Add tariff plan licenses to all tariffs lic list if exist
                                            if "licenses" in tariff_dict:
                                                all_tar_lic_list.extend(tariff_dict["licenses"])

                                        # Also take inline plan and service
                                        else:

                                            # Add tariff plan licenses to all tariffs lic list if exist
                                            if "licenses" in asset_tariff:
                                                all_tar_lic_list.extend(asset_tariff["licenses"])

                                    # Search for all needed licenses in tariff licenses and skip if not found
                                    if not all(lic in all_tar_lic_list for lic in job["licenses"]):
                                        logger.info("Job {asset}/{job} skipped because required license list {lic_list_job} is not found in joined licenses {lic_list_tar} of all of asset tariffs".format(asset=asset["fqdn"], job=job["id"], lic_list_job=job["licenses"], lic_list_tar=all_tar_lic_list))
                                        continue
                                    else:
                                        logger.info("Job {asset}/{job} required license list {lic_list_job} is found in joined licenses {lic_list_tar} of all of asset tariffs".format(asset=asset["fqdn"], job=job["id"], lic_list_job=job["licenses"], lic_list_tar=all_tar_lic_list))

                                # Check run_job
                                if args.run_job:
                                    if job["id"] != run_job:
                                        logger.info("Job {asset}/{job} skipped because it is not needed job".format(asset=asset["fqdn"], job=job["id"]))
                                        continue

                                # Job error should not stop other jobs
                                try:

                                    # Make now from saved_now in job timezone
                                    now = saved_now.astimezone(pytz.timezone(job["tz"]))
                                    logger.info("Job {asset}/{job} now() in job TZ is {now}".format(asset=asset["fqdn"], job=job["id"], now=datetime.strftime(now, "%Y-%m-%d %H:%M:%S %z %Z")))

                                    # Load last job run from jobs_log table
                                    sql = """
                                    SELECT
                                            jobs_script_run_at
                                    ,       job_tz
                                    FROM
                                            jobs_log
                                    WHERE
                                            client = '{client}'
                                    AND
                                            asset_fqdn = '{asset_fqdn}'
                                    AND
                                            job_id = '{job_id}'
                                    ORDER BY
                                            id DESC
                                    LIMIT 1
                                    ;
                                    """.format(client=client_dict["name"], asset_fqdn=asset["fqdn"], job_id=job["id"])
                                    logger.info("Query:")
                                    logger.info(sql)

                                    cur.execute(sql)

                                    # Get job last run
                                    if cur.rowcount > 0:
                                        row = cur.fetchone()
                                        row_jobs_script_run_at = row[0]
                                        row_job_tz = row[1]
                                        row_offset = datetime.now(pytz.timezone(row_job_tz)).strftime("%z") # now is just for an object
                                        job_last_run_text = datetime.strftime(row_jobs_script_run_at, "%Y-%m-%d %H:%M:%S") + " " + row_offset
                                        job_last_run = datetime.strptime(job_last_run_text, "%Y-%m-%d %H:%M:%S %z")
                                    else:
                                        job_last_run = datetime.strptime("1970-01-01 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                                    logger.info("Job {asset}/{job} last run: {time}".format(asset=asset["fqdn"], job=job["id"], time=datetime.strftime(job_last_run, "%Y-%m-%d %H:%M:%S %z %Z")))
                                    
                                    # Check force run one job

                                    if args.force_run_job:

                                        if job["id"] != run_job:
                                            logger.info("Job {asset}/{job} skipped because job id didn't match force run parameter".format(asset=asset["fqdn"], job=job["id"]))
                                            continue
                                        logger.info("Job {asset}/{job} force run - time conditions omitted".format(asset=asset["fqdn"], job=job["id"]))

                                    # Check force run all jobs
                                    elif args.force_run_jobs:

                                        logger.info("Job {asset}/{job} force run - time conditions omitted".format(asset=asset["fqdn"], job=job["id"]))

                                    else:

                                        # Decide if needed to run

                                        if "each" in job:
                                            seconds_between_now_and_job_last_run = (now - job_last_run).total_seconds()
                                            logger.info("Job {asset}/{job} seconds between now and job last run: {secs}".format(asset=asset["fqdn"], job=job["id"], secs=seconds_between_now_and_job_last_run))
                                            seconds_needed_to_wait = 0-2*MINUTES_JITTER*60
                                            if "years" in job["each"]:
                                                seconds_needed_to_wait += 60*60*24*365*job["each"]["years"]
                                            if "months" in job["each"]:
                                                seconds_needed_to_wait += 60*60*24*31*job["each"]["month"]
                                            if "weeks" in job["each"]:
                                                seconds_needed_to_wait += 60*60*24*7*job["each"]["weeks"]
                                            if "days" in job["each"]:
                                                seconds_needed_to_wait += 60*60*24*job["each"]["days"]
                                            if "hours" in job["each"]:
                                                seconds_needed_to_wait += 60*60*job["each"]["hours"]
                                            if "minutes" in job["each"]:
                                                seconds_needed_to_wait += 60*job["each"]["minutes"]
                                            logger.info("Job {asset}/{job} seconds needed to wait from \"each\" key: {secs}".format(asset=asset["fqdn"], job=job["id"], secs=seconds_needed_to_wait))
                                            if seconds_between_now_and_job_last_run < seconds_needed_to_wait:
                                                logger.info("Job {asset}/{job} skipped because: {secs1} < {secs2}".format(asset=asset["fqdn"], job=job["id"], secs1=seconds_between_now_and_job_last_run, secs2=seconds_needed_to_wait))
                                                continue

                                        if "minutes" in job:
                                            minutes_rewrited = []
                                            for minutes in job["minutes"]:
                                                if len(str(minutes).split("-")) > 1:
                                                    for m in range(int(str(minutes).split("-")[0]), int(str(minutes).split("-")[1])+1):
                                                        minutes_rewrited.append(m)
                                                else:
                                                    # Apply MINUTES_JITTER
                                                    for m in range(minutes, minutes + MINUTES_JITTER):
                                                        minutes_rewrited.append(m)
                                            logger.info("Job {asset}/{job} should be run on minutes: {mins}".format(asset=asset["fqdn"], job=job["id"], mins=minutes_rewrited))
                                            now_minute = int(datetime.strftime(now, "%M"))
                                            logger.info("Job {asset}/{job} now minute is: {minute}".format(asset=asset["fqdn"], job=job["id"], minute=now_minute))
                                            if now_minute not in minutes_rewrited:
                                                logger.info("Job {asset}/{job} skipped because now minute is not in run minutes list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue

                                        if "hours" in job:
                                            hours_rewrited = []
                                            for hours in job["hours"]:
                                                if len(str(hours).split("-")) > 1:
                                                    for h in range(int(str(hours).split("-")[0]), int(str(hours).split("-")[1])+1):
                                                        hours_rewrited.append(h)
                                                else:
                                                    hours_rewrited.append(hours)
                                            logger.info("Job {asset}/{job} should be run on hours: {hours}".format(asset=asset["fqdn"], job=job["id"], hours=hours_rewrited))
                                            now_hour = int(datetime.strftime(now, "%H"))
                                            logger.info("Job {asset}/{job} now hour is: {hour}".format(asset=asset["fqdn"], job=job["id"], hour=now_hour))
                                            if now_hour not in hours_rewrited:
                                                logger.info("Job {asset}/{job} skipped because now hour is not in run hours list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue
                                        
                                        if "days" in job:
                                            days_rewrited = []
                                            for days in job["days"]:
                                                if len(str(days).split("-")) > 1:
                                                    for d in range(int(str(days).split("-")[0]), int(str(days).split("-")[1])+1):
                                                        days_rewrited.append(d)
                                                else:
                                                    days_rewrited.append(days)
                                            logger.info("Job {asset}/{job} should be run on days: {days}".format(asset=asset["fqdn"], job=job["id"], days=days_rewrited))
                                            now_day = int(datetime.strftime(now, "%d"))
                                            logger.info("Job {asset}/{job} now day is: {day}".format(asset=asset["fqdn"], job=job["id"], day=now_day))
                                            if now_day not in days_rewrited:
                                                logger.info("Job {asset}/{job} skipped because now day is not in run days list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue
                                        
                                        if "months" in job:
                                            months_rewrited = []
                                            for months in job["months"]:
                                                if len(str(months).split("-")) > 1:
                                                    for m in range(int(str(months).split("-")[0]), int(str(months).split("-")[1])+1):
                                                        months_rewrited.append(m)
                                                else:
                                                    months_rewrited.append(months)
                                            logger.info("Job {asset}/{job} should be run on months: {months}".format(asset=asset["fqdn"], job=job["id"], months=months_rewrited))
                                            now_month = int(datetime.strftime(now, "%m"))
                                            logger.info("Job {asset}/{job} now month is: {month}".format(asset=asset["fqdn"], job=job["id"], month=now_month))
                                            if now_month not in months_rewrited:
                                                logger.info("Job {asset}/{job} skipped because now month is not in run months list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue
                                        
                                        if "years" in job:
                                            years_rewrited = []
                                            for years in job["years"]:
                                                if len(str(years).split("-")) > 1:
                                                    for y in range(int(str(years).split("-")[0]), int(str(years).split("-")[1])+1):
                                                        years_rewrited.append(y)
                                                else:
                                                    years_rewrited.append(years)
                                            logger.info("Job {asset}/{job} should be run on years: {years}".format(asset=asset["fqdn"], job=job["id"], years=years_rewrited))
                                            now_year = int(datetime.strftime(now, "%Y"))
                                            logger.info("Job {asset}/{job} now year is: {year}".format(asset=asset["fqdn"], job=job["id"], year=now_year))
                                            if now_year not in years_rewrited:
                                                logger.info("Job {asset}/{job} skipped because now year is not in run years list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue
                                        
                                        if "weekdays" in job:
                                            logger.info("Job {asset}/{job} should be run on weekdays: {weekdays}".format(asset=asset["fqdn"], job=job["id"], weekdays=job["weekdays"]))
                                            now_weekday = datetime.strftime(now, "%a")
                                            logger.info("Job {asset}/{job} now weekday is: {weekday}".format(asset=asset["fqdn"], job=job["id"], weekday=now_weekday))
                                            if now_weekday not in job["weekdays"]:
                                                logger.info("Job {asset}/{job} skipped because now weekday is not in run weekdays list".format(asset=asset["fqdn"], job=job["id"]))
                                                continue

                                    # Run job

                                    if job["type"] == "salt_cmd":

                                        if "severity_override" in job:
                                            severity_override_part = "SEVERITY_OVERRIDE={severity_override}".format(severity_override=job["severity_override"])
                                        else:
                                            severity_override_part = ""

                                        if "salt-ssh" in job and job["salt-ssh"]:
                                            salt_ssh_in_salt_part = "SALT_SSH_IN_SALT=true"
                                        else:
                                            salt_ssh_in_salt_part = ""

                                        script = textwrap.dedent(
                                            """
                                            .gitlab-server-job/pipeline_salt_cmd.sh nowait {salt_project} {timeout} {asset} "{job_cmd}" {severity_override_part} {salt_ssh_in_salt_part}
                                            """
                                        ).format(
                                            salt_project=client_dict["gitlab"]["salt_project"]["path"],
                                            timeout=job["timeout"],
                                            asset=asset["fqdn"],
                                            job_cmd=job["cmd"],
                                            severity_override_part=severity_override_part,
                                            salt_ssh_in_salt_part=salt_ssh_in_salt_part
                                        )

                                        logger.info("Running bash script:")
                                        logger.info(script)
                                        if not args.dry_run_pipeline:
                                            subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                                    elif job["type"] == "rsnapshot_backup_ssh":
                                        
                                        # Decide which connect host:port to use
                                        if "ssh" in asset:

                                            if "host" in asset["ssh"]:
                                                ssh_host = asset["ssh"]["host"]
                                            else:
                                                ssh_host = asset["fqdn"]

                                            if "port" in asset["ssh"]:
                                                ssh_port = asset["ssh"]["port"]
                                            else:
                                                ssh_port = "22"

                                        else:

                                            ssh_host = asset["fqdn"]
                                            ssh_port = "22"

                                        # Decide ssh jump
                                        if "ssh" in asset and "jump" in asset["ssh"]:
                                            ssh_jump = "SSH_JUMP={host}:{port}".format(host=asset["ssh"]["jump"]["host"], port=asset["ssh"]["jump"]["port"] if "port" in asset["ssh"]["jump"] else "22")
                                        else:
                                            ssh_jump = ""

                                        if "salt-ssh" in job and job["salt-ssh"]:
                                            salt_ssh_in_salt_part = "SALT_SSH_IN_SALT=true"
                                        else:
                                            salt_ssh_in_salt_part = ""

                                        script = textwrap.dedent(
                                            """
                                            .gitlab-server-job/pipeline_rsnapshot_backup.sh nowait {salt_project} 0 {asset} SSH SSH_HOST={ssh_host} SSH_PORT={ssh_port} {ssh_jump} {salt_ssh_in_salt_part}
                                            """
                                        ).format(
                                            salt_project=client_dict["gitlab"]["salt_project"]["path"],
                                            asset=asset["fqdn"],
                                            ssh_host=ssh_host,
                                            ssh_port=ssh_port,
                                            ssh_jump=ssh_jump,
                                            salt_ssh_in_salt_part=salt_ssh_in_salt_part
                                        )
                                        logger.info("Running bash script:")
                                        logger.info(script)
                                        if not args.dry_run_pipeline:
                                            subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                                    elif job["type"] == "rsnapshot_backup_salt":
                                        script = textwrap.dedent(
                                            """
                                            .gitlab-server-job/pipeline_rsnapshot_backup.sh nowait {salt_project} {timeout} {asset} SALT {salt_ssh_in_salt_part}
                                            """
                                        ).format(
                                            salt_project=client_dict["gitlab"]["salt_project"]["path"],
                                            timeout=job["timeout"],
                                            asset=asset["fqdn"],
                                            salt_ssh_in_salt_part=salt_ssh_in_salt_part
                                        )
                                        logger.info("Running bash script:")
                                        logger.info(script)
                                        if not args.dry_run_pipeline:
                                            subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                                    else:
                                        raise Exception("Unknown job type: {jtype}".format(jtype=job["type"]))

                                    # Print job details
                                    print(
                                        "Job: {client} {asset_fqdn} {job_id} {job_level} {job_type} {job_cmd} {job_timeout}".format(
                                            client=client_dict["name"],
                                            asset_fqdn=asset["fqdn"],
                                            job_id=job["id"],
                                            job_level=job["level"],
                                            job_type=job["type"],
                                            job_cmd=job["cmd"].rstrip() if "cmd" in job else "",
                                            job_timeout=job["timeout"] if "timeout" in job else ""
                                        )
                                    )

                                    # Save job log
                                    sql = """
                                    INSERT INTO
                                            jobs_log
                                            (
                                                    jobs_script_run_at
                                            ,       client
                                            ,       asset_fqdn
                                            ,       job_id
                                            ,       job_level
                                            ,       job_type
                                            ,       job_cmd
                                            ,       job_timeout
                                            ,       job_tz
                                            )
                                    VALUES
                                            (
                                                    '{jobs_script_run_at}'
                                            ,       '{client}'
                                            ,       '{asset_fqdn}'
                                            ,       '{job_id}'
                                            ,       '{job_level}'
                                            ,       '{job_type}'
                                            ,       TRIM(e'\t\n\r\ ' FROM CONVERT_FROM(DECODE('{job_cmd_base64}', 'BASE64'), 'UTF-8'))
                                            ,       '{job_timeout}'
                                            ,       '{job_tz}'
                                            )
                                    ;
                                    """.format(
                                        jobs_script_run_at=datetime.strftime(now, "%Y-%m-%d %H:%M:%S"),
                                        client=client_dict["name"],
                                        asset_fqdn=asset["fqdn"],
                                        job_id=job["id"],
                                        job_level=job["level"],
                                        job_type=job["type"],
                                        job_cmd_base64=base64.b64encode(job["cmd"].encode("ascii")).decode("ascii") if "cmd" in job else "",
                                        job_timeout=job["timeout"] if "timeout" in job else "",
                                        job_tz=job["tz"]
                                    )
                                    logger.info("Query:")
                                    logger.info(sql)
                                    try:
                                        cur.execute(sql)
                                        logger.info("Query execution status:")
                                        logger.info(cur.statusmessage)
                                        conn.commit()
                                    except Exception as e:
                                        raise Exception("Caught exception on query execution")
                                
                                except Exception as e:
                                    logger.error("Caught exception, but not interrupting")
                                    logger.exception(e)
                                    errors = True
                
                        except Exception as e:
                            logger.error("Caught exception, but not interrupting")
                            logger.exception(e)
                            errors = True

                except Exception as e:
                    logger.error("Caught exception, but not interrupting")
                    logger.exception(e)
                    errors = True

            # Close connection
            cur.close()
            conn.close()

            # Exit with error if there were errors
            if errors:
                raise Exception("There were errors")

        if args.prune_run_tags:
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                # Client file errors should not stop other clients
                try:
                
                    logger.info("Found client file: {0}".format(client_file))

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                    
                    # Skip other clients
                    prune_client, prune_age = args.prune_run_tags
                    if prune_client != "ALL" and client_dict["name"].lower() != prune_client:
                        continue

                    # Skip disabled clients
                    if not client_dict["active"]:
                        continue

                    # Skip clients without salt_project
                    if "salt_project" not in client_dict["gitlab"]:
                        continue
                    
                    # Skip clients with jobs disabled
                    if "jobs_disabled" in client_dict and client_dict["jobs_disabled"] and not args.ignore_jobs_disabled:
                        continue

                    # Get GitLab project for client
                    project = gl.projects.get(client_dict["gitlab"]["salt_project"]["path"])
                    logger.info("Salt project {project} for client {client} ssh_url_to_repo: {ssh_url_to_repo}, path_with_namespace: {path_with_namespace}".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"], path_with_namespace=project.path_with_namespace, ssh_url_to_repo=project.ssh_url_to_repo))

                    # Decide run_tag_create_access_level
                    if "run_tag_create_access_level" in acc_yaml_dict["gitlab"]["salt_project"]:
                        run_tag_create_access_level = acc_yaml_dict["gitlab"]["salt_project"]["run_tag_create_access_level"]
                    else:
                        run_tag_create_access_level = 40

                    try:
                        # Prune
                        script = textwrap.dedent(
                            """
                            .gitlab-server-job/prune_run_tags.sh {salt_project} {age} git {level}
                            """
                        ).format(salt_project=client_dict["gitlab"]["salt_project"]["path"], age=prune_age, level=run_tag_create_access_level)
                        logger.info("Running bash script:")
                        logger.info(script)
                        subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                    except KeyboardInterrupt:
                        # Remove lock coz trap doesn't work if run inside python
                        script = textwrap.dedent(
                            """
                            rm -rf .locks/prune_run_tags.lock
                            """
                        )
                        logger.info("Running bash script:")
                        logger.info(script)
                        subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                        raise

                except Exception as e:
                    logger.error("Caught exception, but not interrupting")
                    logger.exception(e)
                    errors = True
                
            # Exit with error if there were errors
            if errors:
                raise Exception("There were errors")

    # Reroute catched exception to log
    except Exception as e:
        logger.exception(e)
        logger.info("Finished {LOGO} with errors".format(LOGO=LOGO))
        sys.exit(1)

    logger.info("Finished {LOGO}".format(LOGO=LOGO))
