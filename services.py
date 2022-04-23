#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Import common code
from sysadmws_common import *
import gitlab
import glob
import textwrap
import subprocess
import paramiko
import sys
import json
from io import BytesIO
import threading
import re
import time

# Constants and envs

LOGO="Services"
WORK_DIR = os.environ.get("ACC_WORKDIR", "/opt/sysadmws/accounting")
LOG_DIR = os.environ.get("ACC_LOGDIR", "/opt/sysadmws/accounting/log")
LOG_FILE = "services.log"
CLIENTS_SUBDIR = "clients"
TARIFFS_SUBDIR = "tariffs"
YAML_GLOB = "*.yaml"
YAML_EXT = "yaml"
ACC_YAML = "accounting.yaml"

# Main

if __name__ == "__main__":

    # Set parser and parse args
    parser = argparse.ArgumentParser(description='{LOGO} functions.'.format(LOGO=LOGO))
    parser.add_argument("--debug",
                          dest="debug",
                          help="enable debug",
                          action="store_true")

    parser.add_argument("--ignore-jobs-disabled",
                          dest="ignore_jobs_disabled",
                          help="ignore jobs_disabled if set in yaml",
                          action="store_true")

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--exclude-clients",
                          dest="exclude_clients",
                          help="exclude clients defined by JSON_LIST from all-clients operations",
                          nargs=1, metavar=("JSON_LIST"))

    group.add_argument("--include-clients",
                          dest="include_clients",
                          help="include only clients defined by JSON_LIST for all-clients operations",
                          nargs=1, metavar=("JSON_LIST"))

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pipeline-salt-cmd-for-asset-for-client",
                          dest="pipeline_salt_cmd_for_asset_for_client",
                          help="pipeline salt CMD for one ASSET for client CLIENT",
                          nargs=3, metavar=("CLIENT", "ASSET", "CMD"))

    group.add_argument("--pipeline-salt-cmd-for-all-assets-for-client",
                          dest="pipeline_salt_cmd_for_all_assets_for_client",
                          help="pipeline salt CMD for all assets for client CLIENT",
                          nargs=2, metavar=("CLIENT", "CMD"))

    group.add_argument("--pipeline-salt-cmd-for-all-assets-for-all-clients",
                          dest="pipeline_salt_cmd_for_all_assets_for_all_clients",
                          help="pipeline salt CMD for all assets for all clients excluding --exclude-clients or only for --include-clients",
                          nargs=1, metavar=("CMD"))

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

        if args.exclude_clients is not None:
            json_str, = args.exclude_clients
            exclude_clients_list = json.loads(json_str)
        else:
            exclude_clients_list = []

        if args.include_clients is not None:
            json_str, = args.include_clients
            include_clients_list = json.loads(json_str)
        else:
            include_clients_list = []

        if args.pipeline_salt_cmd_for_asset_for_client or args.pipeline_salt_cmd_for_all_assets_for_client or args.pipeline_salt_cmd_for_all_assets_for_all_clients:
            
            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Unpack oarams and select client if needed
                needed_asset = None
                if args.pipeline_salt_cmd_for_asset_for_client:
                    client, needed_asset, cmd = args.pipeline_salt_cmd_for_asset_for_client
                    if client_dict["name"].lower() != client:
                        continue
                if args.pipeline_salt_cmd_for_all_assets_for_client:
                    client, cmd = args.pipeline_salt_cmd_for_all_assets_for_client
                    if client_dict["name"].lower() != client:
                        continue
                if args.pipeline_salt_cmd_for_all_assets_for_all_clients:
                    cmd, = args.pipeline_salt_cmd_for_all_assets_for_all_clients

                # Check client active, inclusions, exclusions and other reqs
                if (
                        client_dict["active"] and "salt_project" in client_dict["gitlab"] and client_dict["configuration_management"]["type"] in ["salt", "salt-ssh"]
                        and
                        (
                            (
                                args.exclude_clients is not None
                                and
                                client_dict["name"].lower() not in exclude_clients_list
                            )
                            or
                            (
                                args.include_clients is not None
                                and
                                client_dict["name"].lower() in include_clients_list
                            )
                            or
                            (
                                args.exclude_clients is None
                                and
                                args.include_clients is None
                            )
                        )
                    ):

                    # Skip clients with global jobs disabled
                    if not args.ignore_jobs_disabled and "jobs_disabled" in client_dict and client_dict["jobs_disabled"]:
                        continue
            
                    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger)

                    # Threaded function
                    def pipeline_salt_cmd(salt_project, asset, cmd):
                        script = textwrap.dedent(
                            """
                            .gitlab-server-job/pipeline_salt_cmd.sh wait {salt_project} 300 {asset} "{cmd}"
                            """
                        ).format(salt_project=salt_project, asset=asset, cmd=cmd)
                        logger.info("Running bash script in thread:")
                        logger.info(script)
                        run_result = subprocess.run(script, shell=True, universal_newlines=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        json_result = json.loads(run_result.stdout.rstrip())
                        # Take last line as error
                        result_error=run_result.stderr.rstrip().split("\n")[-1]
                        result_pipeline_status=json_result.get("pipeline_status", "")
                        result_project=json_result.get("project", "")
                        result_target=json_result.get("target", "")
                        result_url=json_result.get("pipeline_url", "")
                        print("{status}\t{project}\t{target}\t{url}\t{error}".format(
                            status=result_pipeline_status,
                            project=result_project,
                            target=result_target,
                            url=result_url,
                            error=result_error if result_pipeline_status != "success" else ""
                        ))

                    # For each asset
                    for asset in asset_list:
                        
                        # Pipelines are only for servers
                        if asset["kind"] == "server":

                            # Skip assets with disabled jobs
                            if "jobs_disabled" in asset and asset["jobs_disabled"]:
                                continue

                            # Skip oher assets if specific asset is set
                            if needed_asset is not None:
                                if needed_asset != asset["fqdn"]:
                                    continue

                            # Run pipeline
                            thread = threading.Thread(target=pipeline_salt_cmd, args=[client_dict["gitlab"]["salt_project"]["path"], asset["fqdn"], cmd])
                            thread.start()
                            # Give gitlab time to create tag and pipeline, otherwise it will be overloaded
                            time.sleep(4)

    # Reroute catched exception to log
    except Exception as e:
        logger.exception(e)
        logger.info("Finished {LOGO} with errors".format(LOGO=LOGO))
        sys.exit(1)

    logger.info("Finished {LOGO}".format(LOGO=LOGO))
