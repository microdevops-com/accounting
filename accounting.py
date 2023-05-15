#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Add parent dir to path to fix imports from inside submodules
import os
import sys
import inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, currentdir)
# Import common code
from sysadmws_common import *
from gsuite_scripts import *
import gitlab
import glob
import psycopg2
import textwrap
from datetime import datetime
from datetime import timedelta
from datetime import time
from dateutil.relativedelta import relativedelta
from num2words import num2words
import re
from zipfile import ZipFile
import subprocess
import woocommerce
import paramiko

# Constants and envs

LOGO="Accounting"
WORK_DIR = os.environ.get("ACC_WORKDIR", "/opt/sysadmws/accounting")
LOG_DIR = os.environ.get("ACC_LOGDIR", "/opt/sysadmws/accounting/log")
LOG_FILE = "accounting.log"
TARIFFS_SUBDIR = "tariffs"
CLIENTS_SUBDIR = "clients"
YAML_GLOB = "*.yaml"
YAML_EXT = "yaml"
DB_STRUCTURE_FILE = "accounting_db_structure.sql"
ACC_YAML = "accounting.yaml"
INVOICE_TYPES = ["Hourly", "Monthly", "Storage"]

# Functions

def calculate_range_size(range_id):
    num = 0
    for c in range_id.split(":")[0]:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    left_column = num

    num = 0
    for c in range_id.split(":")[1]:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    right_column = num

    range_size = right_column - left_column + 1

    return range_size

def download_pdf_package(acc_yaml_dict, client_dict, client_folder_files, invoice_type, invoice_number, invoice_needed, act_needed, pack_to_archive, double_act):

    try:
        
        if not invoice_needed and not act_needed:
            raise LoadError("Invoice not needed and act not needed, do not know what to do")
        
        found_invoice = False
        found_details = False
        found_act = False
        pdf_list = []
        
        # Construct name prefix we are searching
        search_prefix_invoice = (
            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][0] +
            " " +
            invoice_number +
            " " +
            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][1]
        )
        search_prefix_details = (
            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["filename"][0] +
            " " +
            invoice_number +
            " " +
            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["filename"][1]
        )
        if act_needed:
            search_prefix_act = (
                acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["filename"][0] +
                " " +
                invoice_number +
                " " +
                acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["filename"][1]
            )
        
        for item in client_folder_files:
            
            # Invoice
            if invoice_needed and item["name"].startswith(search_prefix_invoice) and item["mimeType"] == "application/pdf":
                
                # Remove local pdf
                if os.path.exists(item["name"]):
                    os.remove(item["name"])
                
                # Download pdf
                try:
                    response = drive_download(SA_SECRETS_FILE, item["id"], item["name"], acc_yaml_dict["gsuite"]["drive_user"])
                    logger.info("Doc drive_download response: {0}".format(response))
                except:
                    raise

                # Add pdf to archive list
                pdf_list.append(item["name"])

                # Set found
                logger.info("Invoice PDF for invoice_number = {0} found: {1}".format(invoice_number, item["name"]))
                found_invoice = True
            
            # Details
            if invoice_needed and item["name"].startswith(search_prefix_details) and item["mimeType"] == "application/pdf":
                
                # Remove local pdf
                if os.path.exists(item["name"]):
                    os.remove(item["name"])
                
                # Download pdf
                try:
                    response = drive_download(SA_SECRETS_FILE, item["id"], item["name"], acc_yaml_dict["gsuite"]["drive_user"])
                    logger.info("Doc drive_download response: {0}".format(response))
                except:
                    raise

                # Add pdf to archive list
                pdf_list.append(item["name"])

                # Set found
                logger.info("Details PDF for invoice_number = {0} found: {1}".format(invoice_number, item["name"]))
                found_details = True
            
            # Act
            if act_needed and item["name"].startswith(search_prefix_act) and item["mimeType"] == "application/pdf":
                
                # Remove local pdf
                if os.path.exists(item["name"]):
                    os.remove(item["name"])
                
                # Download pdf
                try:
                    response = drive_download(SA_SECRETS_FILE, item["id"], item["name"], acc_yaml_dict["gsuite"]["drive_user"])
                    logger.info("Doc drive_download response: {0}".format(response))
                except:
                    raise

                # Add pdf to archive list
                pdf_list.append(item["name"])
                
                # Add second act to list if needed
                if double_act:
                    pdf_list.append(item["name"])

                # Set found
                logger.info("Act PDF for invoice_number = {0} found: {1}".format(invoice_number, item["name"]))
                found_act = True
                                
        # Check if needed pdfs found
        if invoice_needed and not found_invoice:
            raise LoadError("Invoice PDF for invoice_number = {0} not found".format(invoice_number))
        if invoice_needed and not found_details:
            raise LoadError("Details PDF for invoice_number = {0} not found".format(invoice_number))
        if act_needed and not found_act:
            raise LoadError("Act PDF for invoice_number = {0} not found".format(invoice_number))

        # Create zip archive if needed and return archive filename
        if pack_to_archive:

            if os.path.exists(invoice_number + '.zip'):
                os.remove(invoice_number + '.zip')
            with ZipFile(invoice_number + '.zip','w') as zip:
                for file in pdf_list:
                    zip.write(file)

            # Remove pdfs
            for pdf_file in pdf_list:
                if os.path.exists(pdf_file):
                    os.remove(pdf_file)

            return [invoice_number + '.zip']

        # Else return pdf filenames
        else:

            return pdf_list

    except:
        # Remove pdfs
        for pdf_file in pdf_list:
            if os.path.exists(pdf_file):
                os.remove(pdf_file)
        raise

# Main

if __name__ == "__main__":

    # Set parser and parse args
    parser = argparse.ArgumentParser(description='{LOGO} functions.'.format(LOGO=LOGO))
    parser.add_argument("--debug", dest="debug", help="enable debug", action="store_true")
    parser.add_argument("--no-exceptions-on-label-errors", dest="no_exceptions_on_label_errors", help="use with dry-runs to bulk check label errors", action="store_true")
    parser.add_argument("--dry-run-db", dest="dry_run_db", help="do not commit to database", action="store_true")
    parser.add_argument("--dry-run-gitlab", dest="dry_run_gitlab", help="no new objects created in gitlab", action="store_true")
    parser.add_argument("--dry-run-gsuite", dest="dry_run_gsuite", help="no new objects created in gsuite", action="store_true")
    parser.add_argument("--dry-run-print", dest="dry_run_print", help="no print commands executed", action="store_true")
    parser.add_argument("--dry-run-woocommerce", dest="dry_run_woocommerce", help="no woocommerce api commands executed", action="store_true")
    parser.add_argument("--timelogs-spent-before-date", dest="timelogs_spent_before_date", help="select unchecked timelogs for hourly invoices spent before date DATE", nargs=1, metavar=("DATE"))
    parser.add_argument("--at-date", dest="at_date", help="use DATETIME instead of now for tariff", nargs=1, metavar=("DATETIME"))
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--exclude-clients", dest="exclude_clients", help="exclude clients defined by JSON_LIST from all-clients operations", nargs=1, metavar=("JSON_LIST"))
    group.add_argument("--include-clients", dest="include_clients", help="include only clients defined by JSON_LIST for all-clients operations", nargs=1, metavar=("JSON_LIST"))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--db-structure", dest="db_structure", help="create database structure", action="store_true")
    group.add_argument("--yaml-check", dest="yaml_check", help="check yaml structure", action="store_true")
    group.add_argument("--asset-labels", dest="asset_labels", help="sync asset labels", action="store_true")
    group.add_argument("--issues-check", dest="issues_check", help="report issue activities as new issue in accounting project", action="store_true")
    group.add_argument("--merge-requests-check", dest="merge_requests_check", help="report MR activities as new issue in accounting project", action="store_true")
    group.add_argument("--storage-usage", dest="storage_usage", help="save all clients billable storage usage to database, excluding --exclude-clients or only for --include-clients", action="store_true")
    group.add_argument("--report-hourly-employee-timelogs", dest="report_hourly_employee_timelogs", help="check new timelogs for EMPLOYEE_EMAIL and report them as new issue", nargs=1, metavar=("EMPLOYEE_EMAIL"))
    group.add_argument("--update-envelopes-for-client", dest="update_envelopes_for_client", help="update envelope pdfs in envelopes folder for client CLIENT", nargs=1, metavar=("CLIENT"))
    group.add_argument("--update-envelopes-for-all-clients", dest="update_envelopes_for_all_clients", help="update envelope pdfs in envelopes folder for all clients excluding --exclude-clients or only for --include-clients", action="store_true")
    group.add_argument("--make-pdfs-for-client", dest="make_pdfs_for_client", help="make pdfs for client CLIENT folder for docs that do not have pdf copy yet", nargs=1, metavar=("CLIENT"))
    group.add_argument("--make-pdfs-for-all-clients", dest="make_pdfs_for_all_clients", help="make pdfs for every client folder for docs that do not have pdf copy yet", action="store_true")
    group.add_argument("--make-gmail-drafts-for-client", dest="make_gmail_drafts_for_client", help="make GMail drafts with PDFs of invoices with status Prepared/Sent for CLIENT", nargs=1, metavar=("CLIENT"))
    group.add_argument("--make-gmail-drafts-for-all-clients", dest="make_gmail_drafts_for_all_clients", help="make GMail drafts with PDFs of invoices with status Prepared/Sent for all clients", action="store_true")
    group.add_argument("--print-papers-for-client", dest="print_papers_for_client", help="print invoice papers with status not Printed for CLIENT", nargs=1, metavar=("CLIENT"))
    group.add_argument("--print-papers-for-all-clients", dest="print_papers_for_all_clients", help="print invoice papers with status not Printed for all clients", action="store_true")
    group.add_argument("--make-hourly-invoice-for-client", dest="make_hourly_invoice_for_client", help="check new timelogs for hourly issues or MRs in projects of CLIENT and make invoice", nargs=1, metavar=("CLIENT"))
    group.add_argument("--make-hourly-invoice-for-all-clients", dest="make_hourly_invoice_for_all_clients", help="check new timelogs for hourly issues or MRs in projects of all clients and make invoice for each", action="store_true")
    group.add_argument("--make-monthly-invoice-for-client", dest="make_monthly_invoice_for_client", help="make monthly invoice for month +MONTH by current month for client CLIENT", nargs=2, metavar=("CLIENT", "MONTH"))
    group.add_argument("--make-monthly-invoice-for-all-clients", dest="make_monthly_invoice_for_all_clients", help="make monthly invoice for month +MONTH by current month for all clients excluding --exclude-clients or only for --include-clients", nargs=1, metavar=("MONTH"))
    group.add_argument("--make-storage-invoice-for-client", dest="make_storage_invoice_for_client", help="compute monthly storage usage for month -MONTH of CLIENT and make invoice", nargs=2, metavar=("CLIENT", "MONTH"))
    group.add_argument("--make-storage-invoice-for-all-clients", dest="make_storage_invoice_for_all_clients", help="compute monthly storage usage for month -MONTH of all clients and make invoice for each", nargs=1, metavar=("MONTH"))
    group.add_argument("--list-assets-for-client", dest="list_assets_for_client", help="list assets for CLIENT", nargs=1, metavar=("CLIENT"))
    group.add_argument("--list-assets-for-all-clients", dest="list_assets_for_all_clients", help="list assets for all clients", action="store_true")
    group.add_argument("--count-assets", dest="count_assets", help="add a new record to the asset_count table", action="store_true")
    group.add_argument("--count-timelog-stats", dest="count_timelog_stats", help="add a new record to the timelogs_stats table", action="store_true")

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

    # Skip vars check where not needed
    if not (args.yaml_check or args.list_assets_for_client is not None or args.list_assets_for_all_clients):

        PG_DB_HOST = os.environ.get("PG_DB_HOST")
        if PG_DB_HOST is None:
            raise Exception("Env var PG_DB_HOST missing")

        PG_DB_NAME = os.environ.get("PG_DB_NAME")
        if PG_DB_NAME is None:
            raise Exception("Env var PG_DB_NAME missing")

        PG_DB_USER = os.environ.get("PG_DB_USER")
        if PG_DB_USER is None:
            raise Exception("Env var PG_DB_USER missing")

        PG_DB_PASS = os.environ.get("PG_DB_PASS")
        if PG_DB_PASS is None:
            raise Exception("Env var PG_DB_PASS missing")

    if not (args.yaml_check or args.list_assets_for_client is not None or args.list_assets_for_all_clients or args.db_structure):

        GL_ADMIN_PRIVATE_TOKEN = os.environ.get("GL_ADMIN_PRIVATE_TOKEN")
        if GL_ADMIN_PRIVATE_TOKEN is None:
            raise Exception("Env var GL_ADMIN_PRIVATE_TOKEN missing")

        GL_BOT_PRIVATE_TOKEN = os.environ.get("GL_BOT_PRIVATE_TOKEN")
        if GL_BOT_PRIVATE_TOKEN is None:
            raise Exception("Env var GL_BOT_PRIVATE_TOKEN missing")

        GL_PG_DB_HOST = os.environ.get("GL_PG_DB_HOST")
        if GL_PG_DB_HOST is None:
            raise Exception("Env var GL_PG_DB_HOST missing")

        GL_PG_DB_NAME = os.environ.get("GL_PG_DB_NAME")
        if GL_PG_DB_NAME is None:
            raise Exception("Env var GL_PG_DB_NAME missing")

        GL_PG_DB_USER = os.environ.get("GL_PG_DB_USER")
        if GL_PG_DB_USER is None:
            raise Exception("Env var GL_PG_DB_USER missing")

        GL_PG_DB_PASS = os.environ.get("GL_PG_DB_PASS")
        if GL_PG_DB_PASS is None:
            raise Exception("Env var GL_PG_DB_PASS missing")

        SA_SECRETS_FILE = os.environ.get("SA_SECRETS_FILE")
        if SA_SECRETS_FILE is None:
            raise Exception("Env var SA_SECRETS_FILE missing")

        SSH_DU_S_M_KEYFILE = os.environ.get("SSH_DU_S_M_KEYFILE")
        if SSH_DU_S_M_KEYFILE is None:
            raise Exception("Env var SSH_DU_S_M_KEYFILE missing")

        SSH_DU_S_M_USER = os.environ.get("SSH_DU_S_M_USER")
        if SSH_DU_S_M_USER is None:
            raise Exception("Env var SSH_DU_S_M_USER missing")

    # Catch exception to logger

    try:

        logger.info("Starting {LOGO}".format(LOGO=LOGO))

        # Chdir to work dir
        os.chdir(WORK_DIR)

        # Skip pgconnect where not needed
        if not (args.yaml_check or args.list_assets_for_client is not None or args.list_assets_for_all_clients):

            # Connect to PG
            dsn = "host={} dbname={} user={} password={}".format(PG_DB_HOST, PG_DB_NAME, PG_DB_USER, PG_DB_PASS)
            conn = psycopg2.connect(dsn)

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

        if args.db_structure:
            
            # New cursor
            cur = conn.cursor()

            # Queries
            sql = load_file_string("{0}/{1}".format(WORK_DIR, DB_STRUCTURE_FILE), logger)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
                conn.commit()
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Close cursor
            cur.close()

        if args.storage_usage:

            errors = False

            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):
                
                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                # Check if client is active
                if client_dict["active"] and (
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
                                                ):

                    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                    # If there are assets
                    if len(asset_list) > 0:

                        # Iterate over assets in client
                        for asset in asset_list:

                            if asset["active"]:
                                
                                logger.info("Active asset: {0}".format(asset["fqdn"]))

                                if "storage" in asset:

                                    for storage_item in asset["storage"]:

                                        for storage_asset, storage_paths in storage_item.items():

                                            for storage_path in storage_paths:

                                                # Compute path usage

                                                # As we have ssh cmd restrictions we need only to supply path as command
                                                cmd = "{folder}".format(folder=storage_path)
                                                logger.info("SSH cmd: {storage_asset}:{cmd}".format(storage_asset=storage_asset, cmd=cmd))

                                                # Clear value
                                                mb_used = None

                                                try:
                                                    private_key = paramiko.Ed25519Key.from_private_key_file(SSH_DU_S_M_KEYFILE)
                                                    ssh_client = paramiko.SSHClient()
                                                    ssh_client.load_system_host_keys()
                                                    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                                                    ssh_client.connect(hostname=storage_asset, username=SSH_DU_S_M_USER, pkey=private_key)
                                                    stdin, stdout, stderr = ssh_client.exec_command(cmd)
                                                    if stdout.channel.recv_exit_status() != 0:
                                                        logger.error("SSH exit code is not 0")
                                                        logger.error("SSH stderr:")
                                                        for line in iter(stderr.readline, ""):
                                                           logger.error(line)
                                                        ssh_client.close()
                                                        errors = True
                                                    else:
                                                        logger.info("SSH value received via stdout:")
                                                        mb_used = int("".join(stdout.readlines()))
                                                        logger.info(mb_used)

                                                        # Save usage to db

                                                        # New cursor
                                                        cur = conn.cursor()

                                                        # Queries
                                                        sql = """
                                                        INSERT INTO
                                                                storage_usage
                                                                (
                                                                        checked_at
                                                                ,       client_asset_fqdn
                                                                ,       storage_asset_fqdn
                                                                ,       storage_asset_path
                                                                ,       mb_used
                                                                )
                                                        VALUES
                                                                (
                                                                        NOW() AT TIME ZONE 'UTC'
                                                                ,       '{client_asset_fqdn}'
                                                                ,       '{storage_asset_fqdn}'
                                                                ,       '{storage_asset_path}'
                                                                ,       {mb_used}
                                                                )
                                                        ;
                                                        """.format(client_asset_fqdn=asset["fqdn"], storage_asset_fqdn=storage_asset, storage_asset_path=storage_path, mb_used=mb_used)
                                                        logger.info("Query:")
                                                        logger.info(sql)
                                                        try:
                                                            cur.execute(sql)
                                                            logger.info("Query execution status:")
                                                            logger.info(cur.statusmessage)
                                                            conn.commit()
                                                        except Exception as e:
                                                            raise Exception("Caught exception on query execution")

                                                        # Close cursor
                                                        cur.close()

                                                    ssh_client.close()
                                                except Exception as e:
                                                    logger.error("Caught exception on SSH execution")
                                                    logger.exception(e)
                                                    errors = True

            # Exit with error if there were errors
            if errors:
                raise Exception("There were errors within SSH execution")

        if args.update_envelopes_for_all_clients or args.update_envelopes_for_client is not None:

            # List all files in envelopes folder
            try:
                envelopes_folder_files = drive_ls(SA_SECRETS_FILE, acc_yaml_dict["envelopes"], acc_yaml_dict["gsuite"]["drive_user"])
            except Exception as e:
                raise Exception("Caught exception on gsuite execution")

            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.update_envelopes_for_client is not None:
                    client, = args.update_envelopes_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # If client yaml has envelope address and not excluded
                if (
                        "papers" in client_dict
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
                        and
                        (
                            (
                                "envelope_address" in client_dict["billing"]["papers"] and "envelope" in acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]
                            )
                            or
                            (
                                "envelope_address_no_recipient" in client_dict["billing"]["papers"] and "envelope_no_recipient" in acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]
                            )
                        )
                    ):

                    # Envelope file name client - merchant - template
                    envelope_file_name = client_dict["name"] + " - " + client_dict["billing"]["merchant"] + " - " + client_dict["billing"]["template"]
            
                    # Remove file in envelopes folder
                    if not args.dry_run_gsuite:
                        for item in envelopes_folder_files:
                            if item["name"] == envelope_file_name:
                                try:
                                    response = drive_rm(SA_SECRETS_FILE, item["id"], acc_yaml_dict["gsuite"]["drive_user"])
                                    logger.info("Envelope drive_rm response: {0}".format(response))
                                except Exception as e:
                                    raise Exception("Caught exception on gsuite execution")

                    # Copy envelope from template
                    if not args.dry_run_gsuite:
                        try:
                            client_doc_envelope = drive_cp(SA_SECRETS_FILE,
                                acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["envelope"] if "envelope_address" in client_dict["billing"]["papers"] else acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["envelope_no_recipient"],
                                acc_yaml_dict["envelopes"],
                                envelope_file_name, acc_yaml_dict["gsuite"]["drive_user"])
                            logger.info("New client envelope id: {0}".format(client_doc_envelope))
                        except Exception as e:
                            raise Exception("Caught exception on gsuite execution")
                    
                    # Templates
                    envelope_data = {
                        "__CONTRACT_RECIPIENT__":   client_dict["billing"]["contract"]["recipient"],
                        "__ENVELOPE_ADDRESS__":     client_dict["billing"]["papers"]["envelope_address"] if "envelope_address" in client_dict["billing"]["papers"] else client_dict["billing"]["papers"]["envelope_address_no_recipient"]
                    }
                    if not args.dry_run_gsuite:
                        try:
                            response = docs_replace_all_text(SA_SECRETS_FILE, client_doc_envelope, json.dumps(envelope_data))
                            logger.info("Envelope docs_replace_all_text response: {0}".format(response))
                        except Exception as e:
                            raise Exception("Caught exception on gsuite execution")

                    # PDF
                    
                    # Remove tmp file
                    if os.path.exists(envelope_file_name + ".pdf"):
                        os.remove(envelope_file_name + ".pdf")

                    # Download as pdf to tmp file
                    try:
                        response = drive_pdf(SA_SECRETS_FILE, client_doc_envelope, envelope_file_name + ".pdf", acc_yaml_dict["gsuite"]["drive_user"])
                        logger.info("Envelope drive_pdf response: {0}".format(response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                    # Upload pdf back to drive
                    if not args.dry_run_gsuite:
                        try:
                            response = drive_upload(SA_SECRETS_FILE, envelope_file_name + ".pdf", acc_yaml_dict["envelopes"], envelope_file_name + ".pdf", acc_yaml_dict["gsuite"]["drive_user"])
                            logger.info("Envelope drive_upload response: {0}".format(response))
                        except Exception as e:
                            raise Exception("Caught exception on gsuite execution")

                    # Remove original
                    if not args.dry_run_gsuite:
                        try:
                            response = drive_rm(SA_SECRETS_FILE, client_doc_envelope, acc_yaml_dict["gsuite"]["drive_user"])
                            logger.info("Envelope drive_rm response: {0}".format(response))
                        except Exception as e:
                            raise Exception("Caught exception on gsuite execution")
                    
                    # Remove tmp file
                    if os.path.exists(envelope_file_name + ".pdf"):
                        os.remove(envelope_file_name + ".pdf")

        if args.make_pdfs_for_all_clients or args.make_pdfs_for_client is not None:

            # Init empty list of new pdfs
            uploaded_pdfs = []

            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.make_pdfs_for_client is not None:
                    client, = args.make_pdfs_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # If client yaml has gsuite folder
                if "gsuite" in client_dict and "folder" in client_dict["gsuite"]:

                    # List all files in client folder
                    try:
                        client_folder_files = drive_ls(SA_SECRETS_FILE, client_dict["gsuite"]["folder"], acc_yaml_dict["gsuite"]["drive_user"])
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                    # For every file that is not pdf and do not have pdf copy and has mimeType = application/vnd.google-apps.document
                    for item in client_folder_files:

                        if not re.match("^.*\.pdf$", item["name"]) and not any(subitem["name"] == item["name"] + ".pdf" for subitem in client_folder_files) and item["mimeType"] == "application/vnd.google-apps.document":

                            # Remove tmp file
                            if os.path.exists(item["name"] + ".pdf"):
                                os.remove(item["name"] + ".pdf")

                            # Download as pdf to tmp file
                            try:
                                response = drive_pdf(SA_SECRETS_FILE, item["id"], item["name"] + ".pdf", acc_yaml_dict["gsuite"]["drive_user"])
                                logger.info("Doc drive_pdf response: {0}".format(response))
                            except Exception as e:
                                raise Exception("Caught exception on gsuite execution")

                            # Upload pdf back to drive
                            if not args.dry_run_gsuite:
                                try:
                                    response = drive_upload(SA_SECRETS_FILE, item["name"] + ".pdf", client_dict["gsuite"]["folder"], item["name"] + ".pdf", acc_yaml_dict["gsuite"]["drive_user"])
                                    logger.info("Pdf drive_upload response: {0}".format(response))
                                    if response is not None:
                                        uploaded_pdfs.append(response)
                                    else:
                                        logger.error("Cannot upload document {0} to folder {1} - may be duplicate".format(item["name"] + ".pdf", client_dict["gsuite"]["folder"]))
                                except Exception as e:
                                    raise Exception("Caught exception on gsuite execution")

                            # Remove tmp file
                            if os.path.exists(item["name"] + ".pdf"):
                                os.remove(item["name"] + ".pdf")

            print("New PDFs to check:")
            for item in uploaded_pdfs:
                print("https://drive.google.com/file/d/" + item + "/view")

            # Create issue to check new PDFs
            
            # Prepare issue header
            issue_text = textwrap.dedent("""
            Please check new PDFs.

            If markup errors found:
            - Delete PDF
            - Fix original Google Doc
            - Run Make PDFs job again
            - Check new issue with new PDFs again
            - Continue with Invoices procedure

            If data errors found:
            - Delete PDF
            - Delete original Google Doc
            - Delete Invoices sheet line for the Client
            - Delete DB records
            - Run Invoice generation again manually for that Client
            - Run Make PDFs job again
            - Check new issue with new PDFs again
            - Continue with Invoices procedure
            
            New PDFs to check:\
            """)
                    
            # Add report rows
            for item in uploaded_pdfs:
                issue_text = "{}\n- https://drive.google.com/file/d/{}/view".format(issue_text, item)
            
            # Connect to GitLab as Bot
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_BOT_PRIVATE_TOKEN)
            gl.auth()

            # Post report as an issue in GitLab
            logger.info("Going to create new issue:")
            logger.info("Title: New PDFs Check Report")
            logger.info("Body:")
            logger.info(issue_text)
            project = gl.projects.get(acc_yaml_dict["accounting"]["project"])
            if not args.dry_run_gitlab:
                issue = project.issues.create({"title": "New PDFs Check Report", "description": issue_text})
                # Add assignee
                issue.assignee_ids = [acc_yaml_dict["accounting"]["manager_id"]]
                issue.save()

        if args.make_gmail_drafts_for_client is not None or args.make_gmail_drafts_for_all_clients or args.print_papers_for_client is not None or args.print_papers_for_all_clients:

            # Get Invoices raw data
            try:
                invoices_raw_dict = sheets_get_as_json(SA_SECRETS_FILE, acc_yaml_dict["invoices"]["spreadsheet"], acc_yaml_dict["invoices"]["invoices"]["sheet"], acc_yaml_dict["invoices"]["invoices"]["range"], 'ROWS', 'FORMATTED_VALUE', 'FORMATTED_STRING')
            except Exception as e:
                raise Exception("Caught exception on gsuite execution")

            # Prepare structured Invoices dict per client
            invoices_dict = {}
            for invoices_line in invoices_raw_dict:
                
                invoices_order_dict = acc_yaml_dict["invoices"]["invoices"]["columns"]["order"]
                
                invoices_line_client = invoices_line[invoices_order_dict['client'] - 1]
                if not invoices_line_client in invoices_dict:
                    invoices_dict[invoices_line_client] = []

                invoices_dict[invoices_line_client].append(
                    {
                        'date_created':         invoices_line[invoices_order_dict['date_created'] - 1],
                        'type':                 invoices_line[invoices_order_dict['type'] - 1],
                        'period':               invoices_line[invoices_order_dict['period'] - 1],
                        'merchant':             invoices_line[invoices_order_dict['merchant'] - 1],
                        'ext_order_number':     invoices_line[invoices_order_dict['ext_order_number'] - 1],
                        'invoice_number':       invoices_line[invoices_order_dict['invoice_number'] - 1],
                        'invoice_currency':     invoices_line[invoices_order_dict['invoice_currency'] - 1],
                        'invoice_sum':          invoices_line[invoices_order_dict['invoice_sum'] - 1],
                        'status':               invoices_line[invoices_order_dict['status'] - 1],
                        'sum_processed':        invoices_line[invoices_order_dict['sum_processed'] - 1],
                        'sum_received':         invoices_line[invoices_order_dict['sum_received'] - 1],
                        'papers':               invoices_line[invoices_order_dict['papers'] - 1]
                    }
                )

            # Read all or specific clients
            clients_dict = {}
            if args.make_gmail_drafts_for_all_clients or args.print_papers_for_all_clients:

                # For *.yaml in client dir
                for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                    logger.info("Found client file: {0}".format(client_file))

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                    # Check client active and exclude/include
                    if client_dict["active"] and (
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
                                                    ):
                        clients_dict[client_dict["name"]] = client_dict
            
            else:

                # Read specific client yaml

                if args.make_gmail_drafts_for_client is not None:
                    client_in_arg, = args.make_gmail_drafts_for_client
                elif args.print_papers_for_client is not None:
                    client_in_arg, = args.print_papers_for_client
                else:
                    raise Exception("Impossible became possible")

                client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client_in_arg.lower(), YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                clients_dict[client_dict["name"]] = client_dict

            # List all files in envelopes folder for further usage
            try:
                envelopes_folder_files = drive_ls(SA_SECRETS_FILE, acc_yaml_dict["envelopes"], acc_yaml_dict["gsuite"]["drive_user"])
            except Exception as e:
                raise Exception("Caught exception on gsuite execution")
                    
            # Iterate over clients and check Invoices data per client
            for client in clients_dict:

                client_dict = clients_dict[client]
                
                # If client has Invoices and client active
                if client in invoices_dict and client_dict["active"]:
                    
                    # Make Drafts

                    if args.make_gmail_drafts_for_client is not None or args.make_gmail_drafts_for_all_clients:

                        # Iterate over each type of Invoice (needed for template file names)

                        for invoice_type in INVOICE_TYPES:

                            # Throw error if more than 1 Prepared Invoice of each type, we shouldn't accumulate Prepared Invoices - we need to send them after each preparation as email templates has specific Invoice number
                            
                            if sum(invoice["status"] == "Prepared" and invoice["type"] == invoice_type for invoice in invoices_dict[client]) == 1:
                                
                                # Find Prepared Invoice (first item of one item list)
                                client_prepared_invoice = [invoice for invoice in invoices_dict[client] if invoice["status"] == "Prepared" and invoice["type"] == invoice_type][0]
                                
                                # Prepare draft debt text template (any invoices with Status == Sent)
                                if any(invoice["status"] == "Sent" and invoice["type"] == invoice_type for invoice in invoices_dict[client]):
                                    if "pack_to_archive" in client_dict["billing"]["papers"]["email"] and client_dict["billing"]["papers"]["email"]["pack_to_archive"] == False:
                                        client_gmail_draft_text_debt = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["debt"].format(
                                                debt_list="\n".join([
                                                    "- "
                                                    + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["number_symbol"]
                                                    + " "
                                                    + invoice["invoice_number"]
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Sent" and invoice["type"] == invoice_type
                                                ])
                                            )
                                    else:
                                        client_gmail_draft_text_debt = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["debt"].format(
                                                debt_list="\n".join([
                                                    "- "
                                                    + invoice["invoice_number"]
                                                    + ".zip"
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Sent" and invoice["type"] == invoice_type
                                                ])
                                            )
                                else:
                                    client_gmail_draft_text_debt = ""

                                # Prepare draft partially received text template (any invoices with Status == Partially Received)
                                if any(invoice["status"] == "Partially Received" and invoice["type"] == invoice_type for invoice in invoices_dict[client]):
                                    if "pack_to_archive" in client_dict["billing"]["papers"]["email"] and client_dict["billing"]["papers"]["email"]["pack_to_archive"] == False:
                                        client_gmail_draft_text_part = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["part"].format(
                                                part_list="\n".join([
                                                    "- "
                                                    + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["number_symbol"]
                                                    + " "
                                                    + invoice["invoice_number"]
                                                    + " "
                                                    + "("
                                                    + (invoice["sum_processed"] if (invoice["sum_processed"] is not None and invoice["sum_processed"] != "") else invoice["sum_received"])
                                                    + "/"
                                                    + invoice["invoice_sum"]
                                                    + " "
                                                    + invoice["invoice_currency"]
                                                    + ")"
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Partially Received" and invoice["type"] == invoice_type
                                                ])
                                            )
                                    else:
                                        client_gmail_draft_text_part = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["part"].format(
                                                part_list="\n".join([
                                                    "- "
                                                    + invoice["invoice_number"]
                                                    + ".zip"
                                                    + " "
                                                    + "("
                                                    + (invoice["sum_processed"] if (invoice["sum_processed"] is not None and invoice["sum_processed"] != "") else invoice["sum_received"])
                                                    + "/"
                                                    + invoice["invoice_sum"]
                                                    + " "
                                                    + invoice["invoice_currency"]
                                                    + ")"
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Partially Received" and invoice["type"] == invoice_type
                                                ])
                                            )
                                else:
                                    client_gmail_draft_text_part = ""

                                # Prepare draft over received text template (any invoices with Status == Over Received)
                                if any(invoice["status"] == "Over Received" and invoice["type"] == invoice_type for invoice in invoices_dict[client]):
                                    if "pack_to_archive" in client_dict["billing"]["papers"]["email"] and client_dict["billing"]["papers"]["email"]["pack_to_archive"] == False:
                                        client_gmail_draft_text_over = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["over"].format(
                                                over_list="\n".join([
                                                    "- "
                                                    + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["number_symbol"]
                                                    + " "
                                                    + invoice["invoice_number"]
                                                    + " "
                                                    + "("
                                                    + (invoice["sum_processed"] if (invoice["sum_processed"] is not None and invoice["sum_processed"] != "") else invoice["sum_received"])
                                                    + "/"
                                                    + invoice["invoice_sum"]
                                                    + " "
                                                    + invoice["invoice_currency"]
                                                    + ")"
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Over Received" and invoice["type"] == invoice_type
                                                ])
                                            )
                                    else:
                                        client_gmail_draft_text_over = \
                                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["over"].format(
                                                over_list="\n".join([
                                                    "- "
                                                    + invoice["invoice_number"]
                                                    + ".zip"
                                                    + " "
                                                    + "("
                                                    + (invoice["sum_processed"] if (invoice["sum_processed"] is not None and invoice["sum_processed"] != "") else invoice["sum_received"])
                                                    + "/"
                                                    + invoice["invoice_sum"]
                                                    + " "
                                                    + invoice["invoice_currency"]
                                                    + ")"
                                                    for invoice in invoices_dict[client] if invoice["status"] == "Over Received" and invoice["type"] == invoice_type
                                                ])
                                            )
                                else:
                                    client_gmail_draft_text_over = ""

                                # Prepare draft act text template if act needed
                                if client_dict["billing"]["papers"]["act"]["email"]:
                                    client_gmail_draft_text_act = \
                                        acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"]["act"]
                                else:
                                    client_gmail_draft_text_act = ""

                                # Prepare draft final text and remove double newlines several times
                                if "woocommerce" in acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]:
                                    order_link_text = "{woocommerce_url}/my-account/view-order/{order_id}/".format(
                                        woocommerce_url=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["woocommerce"]["url"],
                                        order_id=client_prepared_invoice["ext_order_number"]
                                    )
                                else:
                                    order_link_text = ""

                                # Choose email main text depending on pack_to_archive
                                if "pack_to_archive" in client_dict["billing"]["papers"]["email"] and client_dict["billing"]["papers"]["email"]["pack_to_archive"] == False:
                                    main_text_key = "main_no_pack_to_archive"
                                else:
                                    main_text_key = "main"

                                client_gmail_draft_text = \
                                    acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["text"][main_text_key].format(
                                        invoice_number=client_prepared_invoice["invoice_number"],
                                        act_text=client_gmail_draft_text_act,
                                        debt_text=client_gmail_draft_text_debt,
                                        part_text=client_gmail_draft_text_part,
                                        over_text=client_gmail_draft_text_over,
                                        order_link=order_link_text
                                    ).replace("\n\n\n", "\n\n").replace("\n\n\n", "\n\n").replace("\n\n\n", "\n\n")
                                
                                # Compose subject
                                client_gmail_draft_subject = \
                                    acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["email"]["subject"].format(
                                        client=client,
                                        invoice_number=client_prepared_invoice["invoice_number"]
                                    )

                                # Download pdfs

                                client_gmail_draft_attach_list = []
                                
                                # Get and save once document list in client folder
                                try:
                                    client_folder_files = drive_ls(SA_SECRETS_FILE, client_dict["gsuite"]["folder"], acc_yaml_dict["gsuite"]["drive_user"])
                                except Exception as e:
                                    raise Exception("Caught exception on gsuite execution")

                                # Decide if to pack attachments in archives
                                if "pack_to_archive" in client_dict["billing"]["papers"]["email"] and client_dict["billing"]["papers"]["email"]["pack_to_archive"] == False:
                                    pack_to_archive = False
                                else:
                                    pack_to_archive = True
                                
                                # Attach prepared invoice
                                if client_dict["billing"]["papers"]["invoice"]["email"]:

                                    try:
                                        client_gmail_draft_attach_list.extend(download_pdf_package(
                                            acc_yaml_dict=acc_yaml_dict,
                                            client_dict=client_dict,
                                            client_folder_files=client_folder_files,
                                            invoice_type=invoice_type,
                                            invoice_number=client_prepared_invoice["invoice_number"],
                                            invoice_needed=client_dict["billing"]["papers"]["invoice"]["email"],
                                            act_needed=client_dict["billing"]["papers"]["act"]["email"],
                                            pack_to_archive=pack_to_archive,
                                            double_act=False
                                        ))
                                    except Exception as e:
                                        raise Exception("Caught exception on download_pdf_package execution")
                                
                                    # Prepare debt/partially received/over received invoices (any invoices with Status in Sent/Partially Received/Over Received)
                                    if any(invoice["status"] in ["Sent", "Partially Received", "Over Received"] and invoice["type"] == invoice_type for invoice in invoices_dict[client]):
                                        for invoice in invoices_dict[client]:
                                            if invoice["status"] in ["Sent", "Partially Received", "Over Received"] and invoice["type"] == invoice_type:
                                                try:
                                                    client_gmail_draft_attach_list.extend(download_pdf_package(
                                                        acc_yaml_dict=acc_yaml_dict,
                                                        client_dict=client_dict,
                                                        client_folder_files=client_folder_files,
                                                        invoice_type=invoice["type"],
                                                        invoice_number=invoice["invoice_number"],
                                                        invoice_needed=client_dict["billing"]["papers"]["invoice"]["email"],
                                                        act_needed=client_dict["billing"]["papers"]["act"]["email"],
                                                        pack_to_archive=pack_to_archive,
                                                        double_act=False
                                                    ))
                                                except Exception as e:
                                                    raise Exception("Caught exception on download_pdf_package execution")

                                # Create draft
                                if not args.dry_run_gsuite:
                                    try:
                                        draft_id, draft_message = gmail_create_draft(
                                            sa_secrets_file=SA_SECRETS_FILE,
                                            gmail_user=acc_yaml_dict["accounting"]["email"],
                                            message_from=acc_yaml_dict["accounting"]["email"],
                                            message_to=client_dict["billing"]["papers"]["email"]["to"],
                                            message_cc=client_dict["billing"]["papers"]["email"]["cc"] if "cc" in client_dict["billing"]["papers"]["email"] else "",
                                            message_bcc=client_dict["billing"]["papers"]["email"]["bcc"] if "bcc" in client_dict["billing"]["papers"]["email"] else "",
                                            message_subject=client_gmail_draft_subject,
                                            message_text=client_gmail_draft_text,
                                            attach_str=json.dumps(client_gmail_draft_attach_list)
                                        )
                                        logger.info("Drafts gmail_create_draft response: {0}".format(draft_id))
                                    except Exception as e:
                                        raise Exception("Caught exception on gsuite execution")
                                            
                                # Remove all local archives
                                for file in client_gmail_draft_attach_list:
                                    if os.path.exists(file):
                                        os.remove(file)

                            elif sum(invoice["status"] == "Prepared" and invoice["type"] == invoice_type for invoice in invoices_dict[client]) > 1:
                                raise Exception("Client {client} has more than 1 Prepared Invoice of type {invoice_type}, cannot decide which to take".format(client=client, invoice_type=invoice_type))
                        
                    # Print Papers

                    if args.print_papers_for_client is not None or args.print_papers_for_all_clients:

                        # Set lp cmd
                        # All margins but right should be minimized
                        lp_cmd = ["lp", "-d", acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["printer"], "-o", "media=A4", "-o", "fit-to-page", "-o", "page-left=0", "-o", "page-top=0", "-o", "page-bottom=0"]

                        # Papers statuses treated as not printed
                        printed_paper_statuses = ["Not Needed", "Printed", "Sent"]

                        # Check there are invoices to print and printing needed for a client
                        if any(invoice["papers"] not in printed_paper_statuses for invoice in invoices_dict[client]) and (client_dict["billing"]["papers"]["invoice"]["print"] or client_dict["billing"]["papers"]["act"]["print"]):

                            # Print envelope
                
                            # Download PDF
                            client_envelope_file_name = client_dict["name"] + " - " + client_dict["billing"]["merchant"] + " - " + client_dict["billing"]["template"] + ".pdf"
                            if any(envelope["name"] == client_envelope_file_name for envelope in envelopes_folder_files):
                                envelope = [envelope for envelope in envelopes_folder_files if envelope["name"] == client_envelope_file_name][0]
                                # Remove envelope file
                                if os.path.exists(client_envelope_file_name):
                                    os.remove(client_envelope_file_name)
                                try:
                                    response = drive_download(SA_SECRETS_FILE, envelope["id"], envelope["name"], acc_yaml_dict["gsuite"]["drive_user"])
                                    logger.info("Doc drive_download response: {0}".format(response))
                                except Exception as e:
                                    raise Exception("Caught exception on gsuite execution")
                            else:
                                raise Exception("Envelope pdf for client {client} - merchant {merchant} - template {template} not found".format(client=client_dict["name"], merchant=client_dict["billing"]["merchant"], template=client_dict["billing"]["template"]))

                            # Send to printer
                            if not args.dry_run_print:
                                try:
                                    result = subprocess.run(lp_cmd + [client_envelope_file_name])
                                    if result.returncode == 0:
                                        logger.info("Printing to lp succeeded")
                                    else:
                                        raise Exception("Printing to lp failed")
                                except Exception as e:
                                    raise Exception("Caught exception on subprocess.run execution")
                            else:
                                logger.info("Dry run printing {0}".format(client_envelope_file_name))

                            # Remove envelope file
                            if os.path.exists(client_envelope_file_name):
                                os.remove(client_envelope_file_name)
                                
                            # Get and save once document list in client folder
                            try:
                                client_folder_files = drive_ls(SA_SECRETS_FILE, client_dict["gsuite"]["folder"], acc_yaml_dict["gsuite"]["drive_user"])
                            except Exception as e:
                                raise Exception("Caught exception on gsuite execution")

                            # Iterate over each type of Invoice (needed for template file names)

                            for invoice_type in INVOICE_TYPES:
                                
                                # Iterate over all invoices
                                for invoice in invoices_dict[client]:
                                    
                                    # With not Printed status and needed type
                                    if invoice["papers"] not in printed_paper_statuses and invoice["type"] == invoice_type:

                                        # Download PDF
                                        try:
                                            pdf_to_print = download_pdf_package(
                                                acc_yaml_dict=acc_yaml_dict,
                                                client_dict=client_dict,
                                                client_folder_files=client_folder_files,
                                                invoice_type=invoice_type,
                                                invoice_number=invoice["invoice_number"],
                                                invoice_needed=client_dict["billing"]["papers"]["invoice"]["print"],
                                                act_needed=client_dict["billing"]["papers"]["act"]["print"],
                                                pack_to_archive=False,
                                                double_act=True
                                            )
                                        except Exception as e:
                                            raise Exception("Caught exception on download_pdf_package execution")
                            
                                        # Print PDF
                                        for pdf in pdf_to_print:
                                            
                                            # Send to printer
                                            if not args.dry_run_print:
                                                try:
                                                    result = subprocess.run(lp_cmd + [pdf])
                                                    if result.returncode == 0:
                                                        logger.info("Printing to lp succeeded")
                                                    else:
                                                        raise Exception("Printing to lp failed")
                                                except Exception as e:
                                                    raise Exception("Caught exception on subprocess.run execution")
                                            else:
                                                logger.info("Dry run printing {0}".format(pdf))
                                        
                                        # Remove files after print
                                        for pdf in pdf_to_print:
                                            # Remove pdf file
                                            if os.path.exists(pdf):
                                                os.remove(pdf)
                        
                        # Else log no invoices for client
                        else:
                            logger.info("No unprinted invoices for client {0} found".format(client))

        if args.yaml_check:

            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                logger.info("Found client file: {0}".format(client_file))

                try:

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                    # Basic checks

                    if "name" not in client_dict:
                        raise Exception("name key missing in: {0}/{1}".format(WORK_DIR, client_file))

                    if "billing" not in client_dict:
                        raise Exception("billing key missing in: {0}/{1}".format(WORK_DIR, client_file))

                    if "code" not in client_dict["billing"]:
                        raise Exception("billing:code key missing in: {0}/{1}".format(WORK_DIR, client_file))

                    if "active" not in client_dict:
                        raise Exception("active key missing in: {0}/{1}".format(WORK_DIR, client_file))
                    
                    if "start_date" not in client_dict:
                        raise Exception("start_date key missing in: {0}/{1}".format(WORK_DIR, client_file))

                    # Check these only for active clients
                    if client_dict["active"]:

                        if "gsuite" not in client_dict:
                            raise Exception("gsuite key missing in: {0}/{1}".format(WORK_DIR, client_file))
                        if "folder" not in client_dict["gsuite"]:
                            raise Exception("gsuite:folder key missing in: {0}/{1}".format(WORK_DIR, client_file))

                        if "gitlab" not in client_dict:
                            raise Exception("gitlab key missing in: {0}/{1}".format(WORK_DIR, client_file))
                        if "admin_project" not in client_dict["gitlab"]:
                            raise Exception("gitlab:admin_project key missing in: {0}/{1}".format(WORK_DIR, client_file))
                        if "path" not in client_dict["gitlab"]["admin_project"]:
                            raise Exception("gitlab:admin_project:path key missing in: {0}/{1}".format(WORK_DIR, client_file))

                        asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                        # For each asset
                        for asset in asset_list:

                            try:

                                logger.info("Asset: {0}".format(asset["fqdn"]))

                                if "fqdn" not in asset:
                                    raise Exception("fqdn key missing in asset {asset}".format(asset=asset))

                                if "active" not in asset:
                                    raise Exception("active key missing in asset {asset}".format(asset=asset["fqdn"]))

                                # Check only active assets
                                if asset["active"]:

                                    # location and os key is needed only with pipelines
                                    if (
                                            "salt_project" in client_dict["gitlab"]
                                            and
                                            (("jobs_disabled" in client_dict and not client_dict["jobs_disabled"]) or "jobs_disabled" not in client_dict)
                                            and
                                            (("jobs_disabled" in asset and not asset["jobs_disabled"]) or "jobs_disabled" not in asset)
                                        ):
                                        
                                        if "location" not in asset:
                                            raise Exception("location keys missing in asset {asset}".format(asset=asset["fqdn"]))
                                    
                                        if "os" not in asset:
                                            raise Exception("os key missing in asset {asset}".format(asset=asset["fqdn"]))
                                        
                                        if asset["os"] not in acc_yaml_dict["os"]:
                                            raise Exception("os value {os} not in globally allowed list for asset {asset}".format(os=asset["os"], asset=asset["fqdn"]))

                                    for asset_t in asset["tariffs"]:
                                        for asset_tariff in asset_t["tariffs"]:

                                            # If tariff has file key - load it
                                            if "file" in asset_tariff:
                                                
                                                tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                                                if tariff_dict is None:
                                                    
                                                    raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                            except:
                                logger.error("Asset {asset} yaml check exception".format(asset=asset["fqdn"]))
                                raise

                except:
                    logger.error("Client {client} yaml check exception".format(client=client_dict["name"]))
                    raise

        if args.asset_labels:
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()
        
            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):
                
                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                # Check if client is active
                if client_dict["active"]:

                    # Make project list (we need to add asset labels to admin_project AND other projects of client from accounting yaml with asset_labels = True)
                    project_list = []
                    project_list.append(client_dict["gitlab"]["admin_project"]["path"])

                    for acc_project_path, acc_project_vars  in acc_yaml_dict["projects"].items():
                        if acc_project_path != client_dict["gitlab"]["admin_project"]["path"]:
                            if acc_project_vars["client"] == client_dict["name"]:
                                if "asset_labels" in acc_project_vars and acc_project_vars["asset_labels"]:
                                    project_list.append(acc_project_path)

                    # For each project
                    for project_from_list in project_list:

                        # Get GitLab project for client
                        project = gl.projects.get(project_from_list)
                        labels = project.labels.list(all=True)

                        asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now(), False)

                        # Iterate over assets in client
                        for asset in asset_list:

                            logger.info("Asset: {0}".format(asset["fqdn"]))
                            # Set activeness
                            if asset["active"]:
                                asset_activeness = "Active"
                                asset_color = "#3377EE"
                            else:
                                asset_activeness = "NOT Active"
                                asset_color = "#330066"

                            # Take the first (upper and current) tariff and check it
                            asset_tariff_list = []
                            for asset_tariff in activated_tariff(asset["tariffs"], datetime.now(), logger)["tariffs"]:

                                # If tariff has file key - load it
                                if "file" in asset_tariff:
                                    
                                    tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                                    if tariff_dict is None:
                                        
                                        raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                                    # Add tariff plan and service to the tariff list for the label
                                    asset_tariff_list.append("{0} {1}".format(tariff_dict["plan"], tariff_dict["service"]))

                                # Also take inline plan and service
                                else:

                                    # Add tariff plan and service to the tariff list for the label
                                    asset_tariff_list.append("{0} {1}".format(asset_tariff["plan"], asset_tariff["service"]))
                            
                            # Construct label description
                            asset_label_description = "{0}, {1}".format(asset_activeness, ", ".join(asset_tariff_list))
                            logger.info("Asset Label description: {0}".format(asset_label_description))

                            # Check if label with the same description exists
                            if any(label.name == asset["fqdn"] and label.description == asset_label_description and label.color == asset_color for label in labels):
                                logger.info("Existing label found: {0}, {1}, {2}".format(asset["fqdn"], asset_label_description, asset_color))
                            # Else if exists with not the same
                            elif any(label.name == asset["fqdn"] for label in labels):
                                for label in labels:
                                    if label.name == asset["fqdn"]:
                                        label.description = asset_label_description
                                        label.color = asset_color
                                        logger.info("Existing label found but description or color didn't match, updated: {0}, {1}, {2}".format(asset["fqdn"], asset_label_description, asset_color))
                                        if not args.dry_run_gitlab:
                                            label.save()
                            # Add if not exists
                            else:
                                logger.info("No existing label found, added: {0}, {1}, {2}".format(asset["fqdn"], asset_label_description, asset_color))
                                if not args.dry_run_gitlab:
                                    label = project.labels.create({"name": asset["fqdn"], "color": asset_color, "description": asset_label_description})

        if args.issues_check:

            # For *.yaml in client dir
            clients_dict = {}
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                if "hourly_only" in client_dict["billing"]:
                    clients_dict[client_dict["name"].lower()] = {"hourly_only": client_dict["billing"]["hourly_only"]}
                else:
                    # Assume hourly_only = True by default
                    clients_dict[client_dict["name"].lower()] = {"hourly_only": True}

            # New cursor
            cur = conn.cursor()
            sub_cur = conn.cursor()

            # Queries

            # Get ids of issues that were modified after last check OR if there were no check at all for that issue
            # issues.updated_at cannot be used coz last report makes a mention of issue and it is updated by GitLab automatically like
            # @bot mentioned in issue #xxx 5 minutes ago
            # producing infinite loop
            #
            # Actually what does up in report of the issue:
            # close - yes
            # reopen - no
            # adding label - no
            # comment - no
            # adding timelog - yes
            #
            # Let us think this is enough for now
            sql = """
            CREATE TEMP TABLE
                    new_issues
            ON COMMIT DROP
            AS
                    SELECT
                            DISTINCT issues_and_timelogs.issue_id
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            issues.id
                                                    ,       issues.created_at
                                                    ,       issues.last_edited_at
                                                    ,       issues.closed_at
                                                    ,       timelogs.created_at
                                                    ,       timelogs.updated_at
                                                    ,       timelogs.spent_at
                                                    FROM
                                                            issues
                                                            LEFT OUTER JOIN
                                                                    timelogs
                                                            ON
                                                                    issues.id = timelogs.issue_id
                                                    ;
                                                    '
                                            )
                                    AS
                                            issues_and_timelogs
                                            (
                                                    issue_id                    INTEGER
                                            ,       issues_created_at           TIMESTAMP WITHOUT TIME ZONE
                                            ,       issues_last_edited_at       TIMESTAMP WITHOUT TIME ZONE
                                            ,       issues_closed_at            TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_created_at         TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_updated_at         TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_spent_at           TIMESTAMP WITHOUT TIME ZONE
                                            )
                            ) issues_and_timelogs
                            LEFT OUTER JOIN
                                    issues_checked
                            ON
                                    issues_and_timelogs.issue_id = issues_checked.issue_id
                    WHERE
                            issues_and_timelogs.issues_created_at           > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            issues_and_timelogs.issues_last_edited_at       > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            issues_and_timelogs.issues_closed_at            > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            issues_and_timelogs.timelogs_created_at         > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            issues_and_timelogs.timelogs_updated_at         > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            issues_and_timelogs.timelogs_spent_at           > ( SELECT MAX(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id GROUP BY issue_id )
                            OR
                            0                                               = ( SELECT count(checked_at) FROM issues_checked WHERE issue_id = issues_and_timelogs.issue_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Save ids in temp table to log
            sql = "SELECT * FROM new_issues;"
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                for row in cur:
                    logger.info(row)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Save new issues as checked
            sql = """
            INSERT INTO
                    issues_checked
                    (
                            issue_id
                    ,       checked_at
                    )
            SELECT
                    issue_id
            ,       NOW() AT TIME ZONE 'UTC'
            FROM
                    new_issues
            ;
            """
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Prepare issue header
            issue_text = textwrap.dedent("""
            Please check the report. If errors found - the transaction should be deleted and the report made again.
            HOC - Hourly Only Client.
            
            # Non Hourly Issues

            | TX | Issue Link | Title | HOC | Author | Created | Closed | Labels | Time Spent |
            |----|------------|-------|:---:|--------|---------|--------|--------|------------|\
            """)

            # Select non hourly issues
            sql = """
            SELECT
                    non_hourly_issues.namespace_id
            ,       non_hourly_issues.iid
            ,       non_hourly_issues.title
            ,       non_hourly_issues.author
            ,       non_hourly_issues.created_at
            ,       non_hourly_issues.closed_at
            ,       non_hourly_issues.labels
            ,       non_hourly_issues.time_spent
            ,       non_hourly_issues.id
            ,       non_hourly_issues.project_name
            ,       non_hourly_issues.project_path
            ,       issues_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                           AS namespace_id
                            ,       issues.iid                                              AS iid
                            ,       issues.title                                            AS title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    issues.author_id = users.id
                                    )                                                       AS author
                            ,       to_char(issues.created_at, ''YYYY-MM-DD HH24:MI:SS'')   AS created_at
                            ,       to_char(issues.closed_at, ''YYYY-MM-DD HH24:MI:SS'')    AS closed_at
                            ,       (
                                            SELECT
                                                    STRING_AGG(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = issues.id
                                                    AND
                                                    target_type = ''Issue''
                                    )                                                       AS labels
                            ,       (
                                            SELECT
                                                    round(sum(time_spent)/60::numeric/60, 2)
                                            FROM
                                                    timelogs
                                            WHERE
                                                    issues.id = timelogs.issue_id
                                    )                                                       AS time_spent
                            ,       issues.id                                               AS id
                            ,       projects.name                                           AS project_name
                            ,       projects.path                                           AS project_path
                            FROM
                                    issues
                            ,       namespaces
                            ,       projects
                            WHERE
                                    namespaces.id = projects.namespace_id
                                    AND
                                    issues.project_id = projects.id
                                    AND
                                    ''Hourly'' NOT IN
                                            (
                                                    SELECT
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    FROM
                                                            label_links
                                                    WHERE
                                                            label_links.target_id = issues.id
                                                            AND
                                                            target_type = ''Issue''
                                            )
                            ORDER BY
                                    issues.closed_at
                            ,       issues.created_at
                            ;
                            '
                    )
            AS
                    non_hourly_issues
                    (
                            namespace_id        INT
                    ,       iid                 INT
                    ,       title               TEXT
                    ,       author              TEXT
                    ,       created_at          TIMESTAMP
                    ,       closed_at           TIMESTAMP
                    ,       labels              TEXT
                    ,       time_spent          NUMERIC
                    ,       id                  INTEGER
                    ,       project_name        TEXT
                    ,       project_path        TEXT
                    )
            ,       issues_checked
            WHERE
                    non_hourly_issues.id IN
                            (
                                    SELECT
                                            issue_id
                                    FROM
                                            new_issues
                            )
                    AND
                    issues_checked.issue_id = non_hourly_issues.id
                    AND
                    issues_checked.transaction_id = ( SELECT MAX(transaction_id) FROM issues_checked WHERE issue_id = non_hourly_issues.id GROUP BY issue_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_issue_namespace_id  = row[0]
                    row_issue_iid           = row[1]
                    row_issue_title         = row[2]
                    row_issue_author        = row[3]
                    row_issue_created_at    = row[4]
                    row_issue_closed_at     = row[5]
                    row_issue_labels        = row[6]
                    row_issue_time_spent    = row[7]
                    row_issue_id            = row[8]
                    row_project_name        = row[9]
                    row_project_path        = row[10]
                    row_transaction_id      = row[11]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_issue_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_issue_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_issue_namespace_path + "/" + row_project_path
                    row_issue_link = row_issue_namespace_path + "/" + row_project_path + "/issues/" + str(row_issue_iid)

                    # Get client name
                    client_name = acc_yaml_dict["projects"][row_project_path_with_namespace]["client"].lower()

                    # Title not bold by default
                    title_bold = ""

                    # Check if client is hourly_only
                    hourly_only_sign = ""
                    if client_name in clients_dict:
                        if clients_dict[client_name]["hourly_only"]:
                            title_bold = "**"
                            hourly_only_sign = "**+**"

                    # Check if there is time spent -> bold title
                    if not row_issue_time_spent is None:
                        title_bold = "**"
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {}{}{} | {} | {} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_issue_link,
                        title_bold,
                        row_issue_title.replace("|", "-"),
                        title_bold,
                        hourly_only_sign,
                        row_issue_author,
                        row_issue_created_at.strftime("%Y-%m-%d"),
                        row_issue_closed_at.strftime("%Y-%m-%d") if not row_issue_closed_at is None else "",
                        row_issue_labels if not row_issue_labels is None else "",
                        "**{}**".format(row_issue_time_spent) if not row_issue_time_spent is None else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Add more text and second header
            issue_text = "{}\n{}".format(issue_text, textwrap.dedent("""
            # Hourly Issues

            | TX | Issue Link | Title | Author | Created | Closed | Labels | Time Spent |
            |----|------------|-------|--------|---------|--------|--------|------------|\
            """))
            
            # Select hourly issues
            sql = """
            SELECT
                    hourly_issues.namespace_id
            ,       hourly_issues.iid
            ,       hourly_issues.title
            ,       hourly_issues.author
            ,       hourly_issues.created_at
            ,       hourly_issues.closed_at
            ,       hourly_issues.labels
            ,       hourly_issues.time_spent
            ,       hourly_issues.id
            ,       hourly_issues.project_name
            ,       hourly_issues.project_path
            ,       issues_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                           AS namespace_id
                            ,       issues.iid                                              AS iid
                            ,       issues.title                                            AS title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    issues.author_id = users.id
                                    )                                                       AS author
                            ,       to_char(issues.created_at, ''YYYY-MM-DD HH24:MI:SS'')   AS created_at
                            ,       to_char(issues.closed_at, ''YYYY-MM-DD HH24:MI:SS'')    AS closed_at
                            ,       (
                                            SELECT
                                                    STRING_AGG(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = issues.id
                                                    AND
                                                    target_type = ''Issue''
                                    )                                                       AS labels
                            ,       (
                                            SELECT
                                                    round(sum(time_spent)/60::numeric/60, 2)
                                            FROM
                                                    timelogs
                                            WHERE
                                                    issues.id = timelogs.issue_id
                                    )                                                       AS time_spent
                            ,       issues.id                                               AS id
                            ,       projects.name                                           AS project_name
                            ,       projects.path                                           AS project_path
                            FROM
                                    issues
                            ,       namespaces
                            ,       projects
                            WHERE
                                    namespaces.id = projects.namespace_id
                                    AND
                                    issues.project_id = projects.id
                                    AND
                                    ''Hourly'' IN
                                            (
                                                    SELECT
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    FROM
                                                            label_links
                                                    WHERE
                                                            label_links.target_id = issues.id
                                                            AND
                                                            target_type = ''Issue''
                                            )
                            ORDER BY
                                    issues.closed_at
                            ,       issues.created_at
                            ;
                            '
                    )
            AS
                    hourly_issues
                    (
                            namespace_id        INT
                    ,       iid                 INT
                    ,       title               TEXT
                    ,       author              TEXT
                    ,       created_at          TIMESTAMP
                    ,       closed_at           TIMESTAMP
                    ,       labels              TEXT
                    ,       time_spent          NUMERIC
                    ,       id                  INTEGER
                    ,       project_name        TEXT
                    ,       project_path        TEXT
                    )
            ,       issues_checked
            WHERE
                    hourly_issues.id IN
                            (
                                    SELECT
                                            issue_id
                                    FROM
                                            new_issues
                            )
                    AND
                    issues_checked.issue_id = hourly_issues.id
                    AND
                    issues_checked.transaction_id = ( SELECT MAX(transaction_id) FROM issues_checked WHERE issue_id = hourly_issues.id GROUP BY issue_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_issue_namespace_id  = row[0]
                    row_issue_iid           = row[1]
                    row_issue_title         = row[2]
                    row_issue_author        = row[3]
                    row_issue_created_at    = row[4]
                    row_issue_closed_at     = row[5]
                    row_issue_labels        = row[6]
                    row_issue_time_spent    = row[7]
                    row_issue_id            = row[8]
                    row_project_name        = row[9]
                    row_project_path        = row[10]
                    row_transaction_id      = row[11]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_issue_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_issue_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_issue_namespace_path + "/" + row_project_path
                    row_issue_link = row_issue_namespace_path + "/" + row_project_path + "/issues/" + str(row_issue_iid)

                    # Title not bold by default
                    title_bold = ""

                    # Check if time spent is zero
                    if row_issue_time_spent is None or row_issue_time_spent == 0:
                        title_bold = "**"
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {}{}{} | {} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_issue_link,
                        title_bold,
                        row_issue_title.replace("|", "-"),
                        title_bold,
                        row_issue_author,
                        row_issue_created_at.strftime("%Y-%m-%d"),
                        row_issue_closed_at.strftime("%Y-%m-%d") if not row_issue_closed_at is None else "",
                        row_issue_labels if not row_issue_labels is None else "",
                        row_issue_time_spent if not row_issue_time_spent is None else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Connect to GitLab as Bot
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_BOT_PRIVATE_TOKEN)
            gl.auth()

            # Post report as an issue in GitLab
            logger.info("Going to create new issue:")
            logger.info("Title: New Issues Check Report")
            logger.info("Body:")
            logger.info(issue_text)
            project = gl.projects.get(acc_yaml_dict["accounting"]["project"])
            if not args.dry_run_gitlab:
                issue = project.issues.create({"title": "New Issues Check Report", "description": issue_text})
                # Add assignee
                issue.assignee_ids = [acc_yaml_dict["accounting"]["manager_id"]]
                issue.save()

            # Commit and close cursor
            if not args.dry_run_db:
                conn.commit()
            cur.close()
            sub_cur.close()

        if args.merge_requests_check:

            # For *.yaml in client dir
            clients_dict = {}
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                if "hourly_only" in client_dict["billing"]:
                    clients_dict[client_dict["name"].lower()] = {"hourly_only": client_dict["billing"]["hourly_only"]}
                else:
                    # Assume hourly_only = True by default
                    clients_dict[client_dict["name"].lower()] = {"hourly_only": True}

            # New cursor
            cur = conn.cursor()
            sub_cur = conn.cursor()

            # Queries

            # Get ids of MRs that were modified after last check OR if there were no check at all for that MR
            # merge_requests.updated_at cannot be used coz last report makes a mention of MR and it is updated by GitLab automatically like
            # @bot mentioned in MR #xxx 5 minutes ago
            # producing infinite loop
            #
            # Actually what does up in report of the MR:
            # adding label - no
            # comment - no
            # adding timelog - yes
            #
            # Let us think this is enough for now
            sql = """
            CREATE TEMP TABLE
                    new_merge_requests
            ON COMMIT DROP
            AS
                    SELECT
                            DISTINCT merge_requests_and_timelogs.merge_request_id
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            merge_requests.id
                                                    ,       merge_requests.created_at
                                                    ,       merge_requests.last_edited_at
                                                    ,       timelogs.created_at
                                                    ,       timelogs.updated_at
                                                    ,       timelogs.spent_at
                                                    FROM
                                                            merge_requests
                                                            LEFT OUTER JOIN
                                                                    timelogs
                                                            ON
                                                                    merge_requests.id = timelogs.merge_request_id
                                                    ;
                                                    '
                                            )
                                    AS
                                            merge_requests_and_timelogs
                                            (
                                                    merge_request_id                    INTEGER
                                            ,       merge_requests_created_at           TIMESTAMP WITHOUT TIME ZONE
                                            ,       merge_requests_last_edited_at       TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_created_at                 TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_updated_at                 TIMESTAMP WITHOUT TIME ZONE
                                            ,       timelogs_spent_at                   TIMESTAMP WITHOUT TIME ZONE
                                            )
                            ) merge_requests_and_timelogs
                            LEFT OUTER JOIN
                                    merge_requests_checked
                            ON
                                    merge_requests_and_timelogs.merge_request_id = merge_requests_checked.merge_request_id
                    WHERE
                            merge_requests_and_timelogs.merge_requests_created_at           > ( SELECT MAX(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id GROUP BY merge_request_id )
                            OR
                            merge_requests_and_timelogs.merge_requests_last_edited_at       > ( SELECT MAX(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id GROUP BY merge_request_id )
                            OR
                            merge_requests_and_timelogs.timelogs_created_at         > ( SELECT MAX(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id GROUP BY merge_request_id )
                            OR
                            merge_requests_and_timelogs.timelogs_updated_at         > ( SELECT MAX(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id GROUP BY merge_request_id )
                            OR
                            merge_requests_and_timelogs.timelogs_spent_at           > ( SELECT MAX(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id GROUP BY merge_request_id )
                            OR
                            0                                               = ( SELECT count(checked_at) FROM merge_requests_checked WHERE merge_request_id = merge_requests_and_timelogs.merge_request_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Save ids in temp table to log
            sql = "SELECT * FROM new_merge_requests;"
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                for row in cur:
                    logger.info(row)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Save new merge_requests as checked
            sql = """
            INSERT INTO
                    merge_requests_checked
                    (
                            merge_request_id
                    ,       checked_at
                    )
            SELECT
                    merge_request_id
            ,       NOW() AT TIME ZONE 'UTC'
            FROM
                    new_merge_requests
            ;
            """
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Prepare issue header
            issue_text = textwrap.dedent("""
            Please check the report. If errors found - the transaction should be deleted and the report made again.
            HOC - Hourly Only Client.
            
            # Non Hourly MRs

            | TX | MR Link | Title | HOC | Author | Created | Labels | Time Spent |
            |----|---------|-------|:---:|--------|---------|--------|------------|\
            """)

            # Select non hourly merge_requests
            sql = """
            SELECT
                    non_hourly_merge_requests.namespace_id
            ,       non_hourly_merge_requests.iid
            ,       non_hourly_merge_requests.title
            ,       non_hourly_merge_requests.author
            ,       non_hourly_merge_requests.created_at
            ,       non_hourly_merge_requests.labels
            ,       non_hourly_merge_requests.time_spent
            ,       non_hourly_merge_requests.id
            ,       non_hourly_merge_requests.project_name
            ,       non_hourly_merge_requests.project_path
            ,       merge_requests_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                           AS namespace_id
                            ,       merge_requests.iid                                              AS iid
                            ,       merge_requests.title                                            AS title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    merge_requests.author_id = users.id
                                    )                                                       AS author
                            ,       to_char(merge_requests.created_at, ''YYYY-MM-DD HH24:MI:SS'')   AS created_at
                            ,       (
                                            SELECT
                                                    STRING_AGG(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = merge_requests.id
                                                    AND
                                                    target_type = ''MergeRequest''
                                    )                                                       AS labels
                            ,       (
                                            SELECT
                                                    round(sum(time_spent)/60::numeric/60, 2)
                                            FROM
                                                    timelogs
                                            WHERE
                                                    merge_requests.id = timelogs.merge_request_id
                                    )                                                       AS time_spent
                            ,       merge_requests.id                                               AS id
                            ,       projects.name                                           AS project_name
                            ,       projects.path                                           AS project_path
                            FROM
                                    merge_requests
                            ,       namespaces
                            ,       projects
                            WHERE
                                    namespaces.id = projects.namespace_id
                                    AND
                                    merge_requests.target_project_id = projects.id
                                    AND
                                    ''Hourly'' NOT IN
                                            (
                                                    SELECT
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    FROM
                                                            label_links
                                                    WHERE
                                                            label_links.target_id = merge_requests.id
                                                            AND
                                                            target_type = ''MergeRequest''
                                            )
                            ORDER BY
                                    merge_requests.created_at
                            ;
                            '
                    )
            AS
                    non_hourly_merge_requests
                    (
                            namespace_id        INT
                    ,       iid                 INT
                    ,       title               TEXT
                    ,       author              TEXT
                    ,       created_at          TIMESTAMP
                    ,       labels              TEXT
                    ,       time_spent          NUMERIC
                    ,       id                  INTEGER
                    ,       project_name        TEXT
                    ,       project_path        TEXT
                    )
            ,       merge_requests_checked
            WHERE
                    non_hourly_merge_requests.id IN
                            (
                                    SELECT
                                            merge_request_id
                                    FROM
                                            new_merge_requests
                            )
                    AND
                    merge_requests_checked.merge_request_id = non_hourly_merge_requests.id
                    AND
                    merge_requests_checked.transaction_id = ( SELECT MAX(transaction_id) FROM merge_requests_checked WHERE merge_request_id = non_hourly_merge_requests.id GROUP BY merge_request_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_merge_request_namespace_id = row[0]
                    row_merge_request_iid          = row[1]
                    row_merge_request_title        = row[2]
                    row_merge_request_author       = row[3]
                    row_merge_request_created_at   = row[4]
                    row_merge_request_labels       = row[5]
                    row_merge_request_time_spent   = row[6]
                    row_merge_request_id           = row[7]
                    row_project_name               = row[8]
                    row_project_path               = row[9]
                    row_transaction_id             = row[10]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_merge_request_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_merge_request_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_merge_request_namespace_path + "/" + row_project_path
                    row_merge_request_link = row_merge_request_namespace_path + "/" + row_project_path + "/merge_requests/" + str(row_merge_request_iid)

                    # Get client name
                    client_name = acc_yaml_dict["projects"][row_project_path_with_namespace]["client"].lower()

                    # Title not bold by default
                    title_bold = ""

                    # Check if client is hourly_only
                    hourly_only_sign = ""
                    if client_name in clients_dict:
                        if clients_dict[client_name]["hourly_only"]:
                            title_bold = "**"
                            hourly_only_sign = "**+**"

                    # Check if there is time spent -> bold title
                    if not row_merge_request_time_spent is None:
                        title_bold = "**"
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {}{}{} | {} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_merge_request_link,
                        title_bold,
                        row_merge_request_title.replace("|", "-"),
                        title_bold,
                        hourly_only_sign,
                        row_merge_request_author,
                        row_merge_request_created_at.strftime("%Y-%m-%d"),
                        row_merge_request_labels if not row_merge_request_labels is None else "",
                        "**{}**".format(row_merge_request_time_spent) if not row_merge_request_time_spent is None else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Add more text and second header
            issue_text = "{}\n{}".format(issue_text, textwrap.dedent("""
            # Hourly MRs

            | TX | MR Link | Title | Author | Created | Labels | Time Spent |
            |----|---------|-------|--------|---------|--------|------------|\
            """))
            
            # Select hourly merge_requests
            sql = """
            SELECT
                    hourly_merge_requests.namespace_id
            ,       hourly_merge_requests.iid
            ,       hourly_merge_requests.title
            ,       hourly_merge_requests.author
            ,       hourly_merge_requests.created_at
            ,       hourly_merge_requests.labels
            ,       hourly_merge_requests.time_spent
            ,       hourly_merge_requests.id
            ,       hourly_merge_requests.project_name
            ,       hourly_merge_requests.project_path
            ,       merge_requests_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                           AS namespace_id
                            ,       merge_requests.iid                                              AS iid
                            ,       merge_requests.title                                            AS title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    merge_requests.author_id = users.id
                                    )                                                       AS author
                            ,       to_char(merge_requests.created_at, ''YYYY-MM-DD HH24:MI:SS'')   AS created_at
                            ,       (
                                            SELECT
                                                    STRING_AGG(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = merge_requests.id
                                                    AND
                                                    target_type = ''MergeRequest''
                                    )                                                       AS labels
                            ,       (
                                            SELECT
                                                    round(sum(time_spent)/60::numeric/60, 2)
                                            FROM
                                                    timelogs
                                            WHERE
                                                    merge_requests.id = timelogs.merge_request_id
                                    )                                                       AS time_spent
                            ,       merge_requests.id                                               AS id
                            ,       projects.name                                           AS project_name
                            ,       projects.path                                           AS project_path
                            FROM
                                    merge_requests
                            ,       namespaces
                            ,       projects
                            WHERE
                                    namespaces.id = projects.namespace_id
                                    AND
                                    merge_requests.target_project_id = projects.id
                                    AND
                                    ''Hourly'' IN
                                            (
                                                    SELECT
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    FROM
                                                            label_links
                                                    WHERE
                                                            label_links.target_id = merge_requests.id
                                                            AND
                                                            target_type = ''MergeRequest''
                                            )
                            ORDER BY
                                    merge_requests.created_at
                            ;
                            '
                    )
            AS
                    hourly_merge_requests
                    (
                            namespace_id        INT
                    ,       iid                 INT
                    ,       title               TEXT
                    ,       author              TEXT
                    ,       created_at          TIMESTAMP
                    ,       labels              TEXT
                    ,       time_spent          NUMERIC
                    ,       id                  INTEGER
                    ,       project_name        TEXT
                    ,       project_path        TEXT
                    )
            ,       merge_requests_checked
            WHERE
                    hourly_merge_requests.id IN
                            (
                                    SELECT
                                            merge_request_id
                                    FROM
                                            new_merge_requests
                            )
                    AND
                    merge_requests_checked.merge_request_id = hourly_merge_requests.id
                    AND
                    merge_requests_checked.transaction_id = ( SELECT MAX(transaction_id) FROM merge_requests_checked WHERE merge_request_id = hourly_merge_requests.id GROUP BY merge_request_id )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_merge_request_namespace_id = row[0]
                    row_merge_request_iid          = row[1]
                    row_merge_request_title        = row[2]
                    row_merge_request_author       = row[3]
                    row_merge_request_created_at   = row[4]
                    row_merge_request_labels       = row[5]
                    row_merge_request_time_spent   = row[6]
                    row_merge_request_id           = row[7]
                    row_project_name               = row[8]
                    row_project_path               = row[9]
                    row_transaction_id             = row[10]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_merge_request_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_merge_request_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_merge_request_namespace_path + "/" + row_project_path
                    row_merge_request_link = row_merge_request_namespace_path + "/" + row_project_path + "/merge_requests/" + str(row_merge_request_iid)

                    # Title not bold by default
                    title_bold = ""

                    # Check if time spent is zero
                    if row_merge_request_time_spent is None or row_merge_request_time_spent == 0:
                        title_bold = "**"
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {}{}{} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_merge_request_link,
                        title_bold,
                        row_merge_request_title.replace("|", "-"),
                        title_bold,
                        row_merge_request_author,
                        row_merge_request_created_at.strftime("%Y-%m-%d"),
                        row_merge_request_labels if not row_merge_request_labels is None else "",
                        row_merge_request_time_spent if not row_merge_request_time_spent is None else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Connect to GitLab as Bot
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_BOT_PRIVATE_TOKEN)
            gl.auth()

            # Post report as an issue in GitLab
            logger.info("Going to create new issue:")
            logger.info("Title: New MRs Check Report")
            logger.info("Body:")
            logger.info(issue_text)
            project = gl.projects.get(acc_yaml_dict["accounting"]["project"])
            if not args.dry_run_gitlab:
                issue = project.issues.create({"title": "New MRs Check Report", "description": issue_text})
                # Add assignee
                issue.assignee_ids = [acc_yaml_dict["accounting"]["manager_id"]]
                issue.save()

            # Commit and close cursor
            if not args.dry_run_db:
                conn.commit()
            cur.close()
            sub_cur.close()

        if args.report_hourly_employee_timelogs is not None:

            # New cursor
            cur = conn.cursor()
            sub_cur = conn.cursor()

            # Queries

            # Get arg
            hourly_employee, = args.report_hourly_employee_timelogs

            # Get unchecked timelogs for hourly employee to temp table
            sql = """
            CREATE TEMP TABLE
                    hourly_employee_timelogs_unchecked
            ON COMMIT DROP
            AS
                    SELECT
                            timelogs_for_user.timelog_id
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            timelogs.id
                                                    FROM
                                                            timelogs
                                                    ,       users
                                                    WHERE
                                                            timelogs.user_id = users.id
                                                            AND
                                                            users.email = ''{4}''
                                                    ;
                                                    '
                                            )
                                    AS
                                            timelogs_for_user
                                            (
                                                    timelog_id INTEGER
                                            )
                            ) timelogs_for_user
                    WHERE
                            timelogs_for_user.timelog_id NOT IN
                                    (
                                            SELECT
                                                    timelog_id
                                            FROM
                                                    hourly_employee_timelogs_checked
                                    )
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, hourly_employee)
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")
            
            # Save ids in temp table to log
            sql = "SELECT * FROM hourly_employee_timelogs_unchecked;"
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                for row in cur:
                    logger.info(row)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Save ids in temp table as checked
            sql = """
            INSERT INTO
                    hourly_employee_timelogs_checked
                    (
                            timelog_id
                    ,       checked_at
                    )
            SELECT
                    timelog_id
            ,       NOW() AT TIME ZONE 'UTC'
            FROM
                    hourly_employee_timelogs_unchecked
            ;
            """
            logger.info("Query:")
            logger.info(sql)
            try:
                cur.execute(sql)
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Get timelogs and issue details for ids in temp table
            sql = """
            SELECT
                    issue_namespace_id
            ,       issue_iid
            ,       issue_title
            ,       issue_author
            ,       issue_created
            ,       issue_closed
            ,       issue_labels
            ,       issue_is_hourly
            ,       hourly_employee_timelogs_checked_transaction_report.timelog_id
            ,       timelog_user_email
            ,       timelog_time_spent
            ,       timelog_updated
            ,       timelog_note_id
            ,       issue_project_name
            ,       issue_project_path
            ,       hourly_employee_timelogs_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                               AS issue_namespace_id
                            ,       issues.iid                                                  AS issue_iid
                            ,       issues.title                                                AS issue_title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    issues.author_id = users.id
                                    )                                                           AS issue_author
                            ,       to_char(issues.created_at, ''YYYY-MM-DD HH24:MI:SS'')       AS issue_created
                            ,       to_char(issues.closed_at, ''YYYY-MM-DD HH24:MI:SS'')        AS issue_closed
                            ,       (
                                            SELECT
                                                    string_agg(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = issues.id
                                                    AND
                                                    target_type = ''Issue''
                                    )                                                           AS issue_labels
                            ,       CASE
                                            WHEN ''Hourly'' IN
                                                    (
                                                            SELECT
                                                                    (
                                                                            SELECT
                                                                                    title
                                                                            FROM
                                                                                    labels
                                                                            WHERE
                                                                                    labels.id = label_links.label_id
                                                                    )
                                                            FROM
                                                                    label_links
                                                            WHERE
                                                                    label_links.target_id = issues.id
                                                                    AND
                                                                    target_type = ''Issue''
                                                    )
                                            THEN true
                                            ELSE false
                                    END                                                         AS issue_is_hourly
                            ,       timelogs.id                                                 AS timelog_id
                            ,       users.email                                                 AS timelog_user_email
                            ,       timelogs.time_spent                                         AS timelog_time_spent
                            ,       timelogs.spent_at                                           AS timelog_updated
                            ,       timelogs.note_id                                            AS timelog_note_id
                            ,       projects.name                                               AS issue_project_name
                            ,       projects.path                                               AS issue_project_path
                            FROM
                                    timelogs
                            ,       users
                            ,       issues
                            ,       projects
                            ,       namespaces
                            WHERE
                                    timelogs.issue_id = issues.id
                                    AND
                                    timelogs.user_id = users.id
                                    AND
                                    issues.project_id = projects.id
                                    AND
                                    projects.namespace_id = namespaces.id
                            ;
                            '
                    )
            AS
                    hourly_employee_timelogs_checked_transaction_report
                    (
                            issue_namespace_id      INT
                    ,       issue_iid               INT
                    ,       issue_title             TEXT
                    ,       issue_author            TEXT
                    ,       issue_created           TIMESTAMP
                    ,       issue_closed            TIMESTAMP
                    ,       issue_labels            TEXT
                    ,       issue_is_hourly         BOOL
                    ,       timelog_id              INT
                    ,       timelog_user_email      TEXT
                    ,       timelog_time_spent      INT
                    ,       timelog_updated         TIMESTAMP
                    ,       timelog_note_id         INT
                    ,       issue_project_name      TEXT
                    ,       issue_project_path      TEXT
                    )
            ,       hourly_employee_timelogs_checked
            WHERE
                    hourly_employee_timelogs_checked_transaction_report.timelog_id IN
                            (
                                    SELECT
                                            timelog_id
                                    FROM
                                            hourly_employee_timelogs_unchecked
                            )
                    AND
                    hourly_employee_timelogs_checked_transaction_report.timelog_id = hourly_employee_timelogs_checked.timelog_id
            ORDER BY
                    hourly_employee_timelogs_checked_transaction_report.timelog_updated
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)
           
            # Set sum to zero
            sum_seconds = 0

            # Prepare issue header
            issue_text = textwrap.dedent("""
            Please check the report. If errors found - the transaction should be deleted and the report made again.
            Hourly Issues are checked by client first, their Time Spent is not added to the sum.

            | TX | Issue Link | Title | Author | Created | Closed | Labels | Time Log User Email | Time Log Updated | Hourly for Client | Time Spent |
            |----|------------|-------|--------|---------|--------|--------|---------------------|------------------|-------------------|------------|\
            """)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_issue_namespace_id  = row[0]
                    row_issue_iid           = row[1]
                    row_issue_title         = row[2]
                    row_issue_author        = row[3]
                    row_issue_created       = row[4]
                    row_issue_closed        = row[5]
                    row_issue_labels        = row[6]
                    row_issue_is_hourly     = row[7]
                    row_timelog_id          = row[8]
                    row_user_email          = row[9]
                    row_time_spent          = row[10]
                    row_timelog_updated     = row[11]
                    row_timelog_note_id     = row[12]
                    row_project_name        = row[13]
                    row_project_path        = row[14]
                    row_transaction_id      = row[15]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_issue_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_issue_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_issue_namespace_path + "/" + row_project_path
                    row_issue_link = row_issue_namespace_path + "/" + row_project_path + "/issues/" + str(row_issue_iid)
                    if row_timelog_note_id is not None:
                        row_issue_link = row_issue_link + "#note_" + str(row_timelog_note_id)
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_issue_link,
                        row_issue_title.replace("|", "-"),
                        row_issue_author,
                        row_issue_created.strftime("%Y-%m-%d"),
                        row_issue_closed.strftime("%Y-%m-%d") if not row_issue_closed is None else "",
                        row_issue_labels if not row_issue_labels is None else "",
                        row_user_email,
                        row_timelog_updated.strftime("%Y-%m-%d"),
                        round((row_time_spent/60)/60, 2) if row_issue_is_hourly else "",
                        round((row_time_spent/60)/60, 2) if not row_issue_is_hourly else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                    # Add to sum if not hourly
                    if not row_issue_is_hourly:
                        sum_seconds = sum_seconds + row_time_spent
                
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Compute sum hours
            sum_hours = round(((sum_seconds/60)/60), 2)

            # Add issue footer
            issue_text = "{}\n{}".format(issue_text, "|  |  |  |  |  |  |  |  |  |  | **{}** |".format(sum_hours))

            # Connect to GitLab as Bot
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_BOT_PRIVATE_TOKEN)
            gl.auth()

            # Post report as an issue in GitLab
            logger.info("Going to create new issue:")
            logger.info("Title: New Hourly Employee Unchecked Issue Timelogs Report for Payout - {}".format(hourly_employee))
            logger.info("Body:")
            logger.info(issue_text)
            project = gl.projects.get(acc_yaml_dict["accounting"]["project"])
            if not args.dry_run_gitlab:
                issue = project.issues.create({"title": "New Hourly Employee Unchecked Issue Timelogs Report for Payout - {}".format(hourly_employee), "description": issue_text})
                # Add assignee
                issue.assignee_ids = [acc_yaml_dict["accounting"]["manager_id"]]
                issue.save()

            # Get timelogs and merge_request details for ids in temp table
            sql = """
            SELECT
                    merge_request_namespace_id
            ,       merge_request_iid
            ,       merge_request_title
            ,       merge_request_author
            ,       merge_request_created
            ,       merge_request_labels
            ,       merge_request_is_hourly
            ,       hourly_employee_timelogs_checked_transaction_report.timelog_id
            ,       timelog_user_email
            ,       timelog_time_spent
            ,       timelog_updated
            ,       timelog_note_id
            ,       merge_request_project_name
            ,       merge_request_project_path
            ,       hourly_employee_timelogs_checked.transaction_id
            FROM
                    dblink
                    (
                            'host={0} user={1} password={2} dbname={3}'
                    ,       '
                            SELECT
                                    namespaces.id                                               AS merge_request_namespace_id
                            ,       merge_requests.iid                                                  AS merge_request_iid
                            ,       merge_requests.title                                                AS merge_request_title
                            ,       (
                                            SELECT
                                                    email
                                            FROM
                                                    users
                                            WHERE
                                                    merge_requests.author_id = users.id
                                    )                                                           AS merge_request_author
                            ,       to_char(merge_requests.created_at, ''YYYY-MM-DD HH24:MI:SS'')       AS merge_request_created
                            ,       (
                                            SELECT
                                                    string_agg(
                                                            (
                                                                    SELECT
                                                                            title
                                                                    FROM
                                                                            labels
                                                                    WHERE
                                                                            labels.id = label_links.label_id
                                                            )
                                                    , '', '')
                                            FROM
                                                    label_links
                                            WHERE
                                                    label_links.target_id = merge_requests.id
                                                    AND
                                                    target_type = ''MergeRequest''
                                    )                                                           AS merge_request_labels
                            ,       CASE
                                            WHEN ''Hourly'' IN
                                                    (
                                                            SELECT
                                                                    (
                                                                            SELECT
                                                                                    title
                                                                            FROM
                                                                                    labels
                                                                            WHERE
                                                                                    labels.id = label_links.label_id
                                                                    )
                                                            FROM
                                                                    label_links
                                                            WHERE
                                                                    label_links.target_id = merge_requests.id
                                                                    AND
                                                                    target_type = ''MergeRequest''
                                                    )
                                            THEN true
                                            ELSE false
                                    END                                                         AS merge_request_is_hourly
                            ,       timelogs.id                                                 AS timelog_id
                            ,       users.email                                                 AS timelog_user_email
                            ,       timelogs.time_spent                                         AS timelog_time_spent
                            ,       timelogs.spent_at                                           AS timelog_updated
                            ,       timelogs.note_id                                            AS timelog_note_id
                            ,       projects.name                                               AS merge_request_project_name
                            ,       projects.path                                               AS merge_request_project_path
                            FROM
                                    timelogs
                            ,       users
                            ,       merge_requests
                            ,       projects
                            ,       namespaces
                            WHERE
                                    timelogs.merge_request_id = merge_requests.id
                                    AND
                                    timelogs.user_id = users.id
                                    AND
                                    merge_requests.target_project_id = projects.id
                                    AND
                                    projects.namespace_id = namespaces.id
                            ;
                            '
                    )
            AS
                    hourly_employee_timelogs_checked_transaction_report
                    (
                            merge_request_namespace_id      INT
                    ,       merge_request_iid               INT
                    ,       merge_request_title             TEXT
                    ,       merge_request_author            TEXT
                    ,       merge_request_created           TIMESTAMP
                    ,       merge_request_labels            TEXT
                    ,       merge_request_is_hourly         BOOL
                    ,       timelog_id                      INT
                    ,       timelog_user_email              TEXT
                    ,       timelog_time_spent              INT
                    ,       timelog_updated                 TIMESTAMP
                    ,       timelog_note_id                 INT
                    ,       merge_request_project_name      TEXT
                    ,       merge_request_project_path      TEXT
                    )
            ,       hourly_employee_timelogs_checked
            WHERE
                    hourly_employee_timelogs_checked_transaction_report.timelog_id IN
                            (
                                    SELECT
                                            timelog_id
                                    FROM
                                            hourly_employee_timelogs_unchecked
                            )
                    AND
                    hourly_employee_timelogs_checked_transaction_report.timelog_id = hourly_employee_timelogs_checked.timelog_id
            ORDER BY
                    hourly_employee_timelogs_checked_transaction_report.timelog_updated
            ;
            """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
            logger.info("Query:")
            logger.info(sql)
           
            # Set sum to zero
            sum_seconds = 0

            # Prepare issue header
            issue_text = textwrap.dedent("""
            Please check the report. If errors found - the transaction should be deleted and the report made again.
            Hourly MRs are checked by client first, their Time Spent is not added to the sum.

            | TX | MR Link | Title | Author | Created | Labels | Time Log User Email | Time Log Updated | Hourly for Client | Time Spent |
            |----|---------|-------|--------|---------|--------|---------------------|------------------|-------------------|------------|\
            """)

            # Read rows
            try:
                cur.execute(sql)
                
                for row in cur:
                    
                    # Set row fields
                    row_merge_request_namespace_id  = row[0]
                    row_merge_request_iid           = row[1]
                    row_merge_request_title         = row[2]
                    row_merge_request_author        = row[3]
                    row_merge_request_created       = row[4]
                    row_merge_request_labels        = row[5]
                    row_merge_request_is_hourly     = row[6]
                    row_timelog_id                  = row[7]
                    row_user_email                  = row[8]
                    row_time_spent                  = row[9]
                    row_timelog_updated             = row[10]
                    row_timelog_note_id             = row[11]
                    row_project_name                = row[12]
                    row_project_path                = row[13]
                    row_transaction_id              = row[14]

                    # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                    sql = """
                    SELECT
                            STRING_AGG(namespace_path, '/') AS n_path
                    FROM
                            (
                                    SELECT
                                            *
                                    FROM
                                            dblink
                                            (
                                                    'host={0} user={1} password={2} dbname={3}'
                                            ,       '
                                                    SELECT
                                                            namespaces.path
                                                    FROM
                                                            namespaces
                                                    ,       (
                                                                    SELECT
                                                                            ordinality
                                                                    ,       unnest
                                                                    FROM
                                                                            unnest(
                                                                                    (
                                                                                            SELECT
                                                                                                    traversal_ids
                                                                                            FROM
                                                                                                    namespaces
                                                                                            WHERE
                                                                                                    id = {4}
                                                                                    )
                                                                            ) WITH ORDINALITY
                                                            ) AS namespaces_unnested
                                                    WHERE
                                                            namespaces_unnested.unnest = namespaces.id
                                                    ORDER BY
                                                            namespaces_unnested.ordinality
                                                    ;
                                                    '
                                            )
                                    AS
                                            namespace_paths
                                            (
                                                    namespace_path      TEXT
                                            )
                            ) AS ns_path
                    ;
                    """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_merge_request_namespace_id)
                    logger.info("Query:")
                    logger.info(sql)
                    try:
                        sub_cur.execute(sql)
                        for sub_row in sub_cur:
                            row_merge_request_namespace_path = sub_row[0]
                        logger.info("Query execution status:")
                        logger.info(sub_cur.statusmessage)
                    except Exception as e:
                        raise Exception("Caught exception on query execution")

                    row_project_path_with_namespace = row_merge_request_namespace_path + "/" + row_project_path
                    row_merge_request_link = row_merge_request_namespace_path + "/" + row_project_path + "/merge_requests/" + str(row_merge_request_iid)
                    if row_timelog_note_id is not None:
                        row_merge_request_link = row_merge_request_link + "#note_" + str(row_timelog_note_id)
                    
                    # Add report row
                    issue_text = "{}\n{}".format(issue_text, "| {} | {}/{} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                        row_transaction_id,
                        acc_yaml_dict["gitlab"]["url"],
                        row_merge_request_link,
                        row_merge_request_title.replace("|", "-"),
                        row_merge_request_author,
                        row_merge_request_created.strftime("%Y-%m-%d"),
                        row_merge_request_labels if not row_merge_request_labels is None else "",
                        row_user_email,
                        row_timelog_updated.strftime("%Y-%m-%d"),
                        round((row_time_spent/60)/60, 2) if row_merge_request_is_hourly else "",
                        round((row_time_spent/60)/60, 2) if not row_merge_request_is_hourly else ""
                    ))

                    # Save raw data to log
                    logger.info(row)

                    # Add to sum if not hourly
                    if not row_merge_request_is_hourly:
                        sum_seconds = sum_seconds + row_time_spent
                
                logger.info("Query execution status:")
                logger.info(cur.statusmessage)
            except Exception as e:
                raise Exception("Caught exception on query execution")

            # Compute sum hours
            sum_hours = round(((sum_seconds/60)/60), 2)

            # Add issue footer
            issue_text = "{}\n{}".format(issue_text, "|  |  |  |  |  |  |  |  |  | **{}** |".format(sum_hours))

            # Connect to GitLab as Bot
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_BOT_PRIVATE_TOKEN)
            gl.auth()

            # Post report as an issue in GitLab
            logger.info("Going to create new issue:")
            logger.info("Title: New Hourly Employee Unchecked MR Timelogs Report for Payout - {}".format(hourly_employee))
            logger.info("Body:")
            logger.info(issue_text)
            project = gl.projects.get(acc_yaml_dict["accounting"]["project"])
            if not args.dry_run_gitlab:
                issue = project.issues.create({"title": "New Hourly Employee Unchecked MR Timelogs Report for Payout - {}".format(hourly_employee), "description": issue_text})
                # Add assignee
                issue.assignee_ids = [acc_yaml_dict["accounting"]["manager_id"]]
                issue.save()

            # Commit and close cursor
            if not args.dry_run_db:
                conn.commit()
            cur.close()
            sub_cur.close()
        
        if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients \
        or args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None \
        or args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients is not None:
            
            # Hourly

            if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
            
                # New cursor
                cur = conn.cursor()
                sub_cur = conn.cursor()

                # Queries

                # Different query if specific client or all
                if args.make_hourly_invoice_for_all_clients:
                    
                    # Limit timelogs by date if needed
                    if args.timelogs_spent_before_date is not None:
                        where_timelogs = "AND timelogs.spent_at < ''{date}''".format(date=args.timelogs_spent_before_date[0])
                    else:
                        where_timelogs = ""

                    # Get unchecked timelogs for hourly issues and mrs to 2 temp tables for all clients
                    sql = """
                    CREATE TEMP TABLE
                            hourly_issue_timelogs_unchecked
                    ON COMMIT DROP
                    AS
                            SELECT
                                    timelogs.timelog_id
                            FROM
                                    (
                                            SELECT
                                                    *
                                            FROM
                                                    dblink
                                                    (
                                                            'host={host} user={user} password={password} dbname={dbname}'
                                                    ,       '
                                                            SELECT
                                                                    timelogs.id
                                                            FROM
                                                                    timelogs
                                                            WHERE
                                                                    timelogs.issue_id IS NOT NULL
                                                                    {where_timelogs}
                                                            ;
                                                            '
                                                    )
                                            AS
                                                    timelogs
                                                    (
                                                            timelog_id INTEGER
                                                    )
                                    ) timelogs
                            WHERE
                                    timelogs.timelog_id NOT IN
                                            (
                                                    SELECT
                                                            timelog_id
                                                    FROM
                                                            hourly_timelogs_checked
                                            )
                    ;
                    CREATE TEMP TABLE
                            hourly_merge_request_timelogs_unchecked
                    ON COMMIT DROP
                    AS
                            SELECT
                                    timelogs.timelog_id
                            FROM
                                    (
                                            SELECT
                                                    *
                                            FROM
                                                    dblink
                                                    (
                                                            'host={host} user={user} password={password} dbname={dbname}'
                                                    ,       '
                                                            SELECT
                                                                    timelogs.id
                                                            FROM
                                                                    timelogs
                                                            WHERE
                                                                    timelogs.merge_request_id IS NOT NULL
                                                                    {where_timelogs}
                                                            ;
                                                            '
                                                    )
                                            AS
                                                    timelogs
                                                    (
                                                            timelog_id INTEGER
                                                    )
                                    ) timelogs
                            WHERE
                                    timelogs.timelog_id NOT IN
                                            (
                                                    SELECT
                                                            timelog_id
                                                    FROM
                                                            hourly_timelogs_checked
                                            )
                    ;
                    """.format(host=GL_PG_DB_HOST, user=GL_PG_DB_USER, password=GL_PG_DB_PASS, dbname=GL_PG_DB_NAME, where_timelogs=where_timelogs)
                else:
                    
                    timelogs_check_client, = args.make_hourly_invoice_for_client

                    # Get client projects
                    
                    # Get client name
                    client_name = timelogs_check_client.lower()

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client_name, YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}/{2}.{3}".format(WORK_DIR, CLIENTS_SUBDIR, client_name, YAML_EXT))

                    # Find project ids for needed projects

                    # Connect to GitLab
                    gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
                    gl.auth()

                    # Get admin project and other client projects from accounting yaml ids to search in query
                    c_project = gl.projects.get(client_dict["gitlab"]["admin_project"]["path"])
                    timelogs_check_client_projects = str(c_project.id)

                    for acc_project_path, acc_project_vars  in acc_yaml_dict["projects"].items():
                        if acc_project_path != client_dict["gitlab"]["admin_project"]["path"]:
                            if acc_project_vars["client"] == client_dict["name"]:
                                c_project = gl.projects.get(acc_project_path)
                                timelogs_check_client_projects = timelogs_check_client_projects + ", " + str(c_project.id)

                    # Limit timelogs by date if needed
                    if args.timelogs_spent_before_date is not None:
                        where_timelogs = "AND timelogs.spent_at < ''{date}''".format(date=args.timelogs_spent_before_date[0])
                    else:
                        where_timelogs = ""

                    # Get unchecked timelogs for hourly issues and mrs to 2 temp tables for specific client

                    sql = """
                    CREATE TEMP TABLE
                            hourly_issue_timelogs_unchecked
                    ON COMMIT DROP
                    AS
                            SELECT
                                    timelogs.timelog_id
                            FROM
                                    (
                                            SELECT
                                                    *
                                            FROM
                                                    dblink
                                                    (
                                                            'host={host} user={user} password={password} dbname={dbname}'
                                                    ,       '
                                                            SELECT
                                                                    timelogs.id
                                                            FROM
                                                                    timelogs
                                                            ,       issues
                                                            ,       projects
                                                            WHERE
                                                                    timelogs.issue_id = issues.id
                                                                    AND
                                                                    issues.project_id = projects.id
                                                                    AND
                                                                    projects.id IN
                                                                            (
                                                                                    {projects}
                                                                            )
                                                                    {where_timelogs}
                                                                    ;
                                                            '
                                                    )
                                            AS
                                                    timelogs
                                                    (
                                                            timelog_id INTEGER
                                                    )
                                    ) timelogs
                            WHERE
                                    timelogs.timelog_id NOT IN
                                            (
                                                    SELECT
                                                            timelog_id
                                                    FROM
                                                            hourly_timelogs_checked
                                            )
                    ;
                    CREATE TEMP TABLE
                            hourly_merge_request_timelogs_unchecked
                    ON COMMIT DROP
                    AS
                            SELECT
                                    timelogs.timelog_id
                            FROM
                                    (
                                            SELECT
                                                    *
                                            FROM
                                                    dblink
                                                    (
                                                            'host={host} user={user} password={password} dbname={dbname}'
                                                    ,       '
                                                            SELECT
                                                                    timelogs.id
                                                            FROM
                                                                    timelogs
                                                            ,       merge_requests
                                                            ,       projects
                                                            WHERE
                                                                    timelogs.merge_request_id = merge_requests.id
                                                                    AND
                                                                    merge_requests.target_project_id = projects.id
                                                                    AND
                                                                    projects.id IN
                                                                            (
                                                                                    {projects}
                                                                            )
                                                                    {where_timelogs}
                                                                    ;
                                                            '
                                                    )
                                            AS
                                                    timelogs
                                                    (
                                                            timelog_id INTEGER
                                                    )
                                    ) timelogs
                            WHERE
                                    timelogs.timelog_id NOT IN
                                            (
                                                    SELECT
                                                            timelog_id
                                                    FROM
                                                            hourly_timelogs_checked
                                            )
                    ;
                    """.format(host=GL_PG_DB_HOST, user=GL_PG_DB_USER, password=GL_PG_DB_PASS, dbname=GL_PG_DB_NAME, projects=timelogs_check_client_projects, where_timelogs=where_timelogs)
                
                logger.info("Query:")
                logger.info(sql)
                try:
                    cur.execute(sql)
                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")
                
                # Save ids in temp table to log

                sql = "SELECT * FROM hourly_issue_timelogs_unchecked;"
                logger.info("Query:")
                logger.info(sql)
                try:
                    cur.execute(sql)
                    for row in cur:
                        logger.info(row)
                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")

                sql = "SELECT * FROM hourly_merge_request_timelogs_unchecked;"
                logger.info("Query:")
                logger.info(sql)
                try:
                    cur.execute(sql)
                    for row in cur:
                        logger.info(row)
                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")

                # Save ids in temp table as checked
                sql = """
                INSERT INTO
                        hourly_timelogs_checked
                        (
                                timelog_id
                        ,       checked_at
                        )
                SELECT
                        timelog_id
                ,       NOW() AT TIME ZONE 'UTC'
                FROM
                        hourly_issue_timelogs_unchecked
                ;
                INSERT INTO
                        hourly_timelogs_checked
                        (
                                timelog_id
                        ,       checked_at
                        )
                SELECT
                        timelog_id
                ,       NOW() AT TIME ZONE 'UTC'
                FROM
                        hourly_merge_request_timelogs_unchecked
                ;
                """
                logger.info("Query:")
                logger.info(sql)
                try:
                    cur.execute(sql)
                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")

                # Get timelogs and issue union mrs details for ids in temp table
                sql = """
                SELECT
                        project_name
                ,       project_path
                ,       project_namespace_id
                ,       issue_iid
                ,       issue_title
                ,       issue_author
                ,       issue_created
                ,       issue_closed
                ,       issue_labels
                ,       issue_is_hourly
                ,       hourly_issue_timelogs_checked_transaction_report.timelog_id AS tr_timelog_id
                ,       timelog_user_email
                ,       timelog_time_spent
                ,       timelog_updated
                ,       timelog_note_id
                ,       hourly_timelogs_checked.transaction_id
                ,       'issue' AS timelog_kind
                FROM
                        dblink
                        (
                                'host={0} user={1} password={2} dbname={3}'
                        ,       '
                                SELECT
                                        projects.name                                               AS project_name
                                ,       projects.path                                               AS project_path
                                ,       namespaces.id                                               AS project_namespace_id
                                ,       issues.iid                                                  AS issue_iid
                                ,       issues.title                                                AS issue_title
                                ,       (
                                                SELECT
                                                        email
                                                FROM
                                                        users
                                                WHERE
                                                        issues.author_id = users.id
                                        )                                                           AS issue_author
                                ,       to_char(issues.created_at, ''YYYY-MM-DD HH24:MI:SS'')       AS issue_created
                                ,       to_char(issues.closed_at, ''YYYY-MM-DD HH24:MI:SS'')        AS issue_closed
                                ,       (
                                                SELECT
                                                        string_agg(
                                                                (
                                                                        SELECT
                                                                                title
                                                                        FROM
                                                                                labels
                                                                        WHERE
                                                                                labels.id = label_links.label_id
                                                                )
                                                        , '', '')
                                                FROM
                                                        label_links
                                                WHERE
                                                        label_links.target_id = issues.id
                                                        AND
                                                        target_type = ''Issue''
                                        )                                                           AS issue_labels
                                ,       CASE
                                                WHEN ''Hourly'' IN
                                                        (
                                                                SELECT
                                                                        (
                                                                                SELECT
                                                                                        title
                                                                                FROM
                                                                                        labels
                                                                                WHERE
                                                                                        labels.id = label_links.label_id
                                                                        )
                                                                FROM
                                                                        label_links
                                                                WHERE
                                                                        label_links.target_id = issues.id
                                                                        AND
                                                                        target_type = ''Issue''
                                                        )
                                                THEN true
                                                ELSE false
                                        END                                                         AS issue_is_hourly
                                ,       timelogs.id                                                 AS timelog_id
                                ,       users.email                                                 AS timelog_user_email
                                ,       timelogs.time_spent                                         AS timelog_time_spent
                                ,       timelogs.spent_at                                           AS timelog_updated
                                ,       timelogs.note_id                                            AS timelog_note_id
                                FROM
                                        timelogs
                                ,       users
                                ,       issues
                                ,       projects
                                ,       namespaces
                                WHERE
                                        timelogs.issue_id = issues.id
                                        AND
                                        timelogs.user_id = users.id
                                        AND
                                        issues.project_id = projects.id
                                        AND
                                        projects.namespace_id = namespaces.id
                                ;
                                '
                        )
                AS
                        hourly_issue_timelogs_checked_transaction_report
                        (
                                project_name            TEXT
                        ,       project_path            TEXT
                        ,       project_namespace_id    INT
                        ,       issue_iid               INT
                        ,       issue_title             TEXT
                        ,       issue_author            TEXT
                        ,       issue_created           TIMESTAMP
                        ,       issue_closed            TIMESTAMP
                        ,       issue_labels            TEXT
                        ,       issue_is_hourly         BOOL
                        ,       timelog_id              INT
                        ,       timelog_user_email      TEXT
                        ,       timelog_time_spent      INT
                        ,       timelog_updated         TIMESTAMP
                        ,       timelog_note_id         TEXT
                        )
                ,       hourly_timelogs_checked
                WHERE
                        hourly_issue_timelogs_checked_transaction_report.timelog_id IN
                                (
                                        SELECT
                                                timelog_id
                                        FROM
                                                hourly_issue_timelogs_unchecked
                                )
                        AND
                        hourly_issue_timelogs_checked_transaction_report.timelog_id = hourly_timelogs_checked.timelog_id
                UNION ALL
                SELECT
                        project_name
                ,       project_path
                ,       project_namespace_id
                ,       merge_request_iid
                ,       merge_request_title
                ,       merge_request_author
                ,       merge_request_created
                ,       merge_request_closed
                ,       merge_request_labels
                ,       merge_request_is_hourly
                ,       hourly_merge_request_timelogs_checked_transaction_report.timelog_id AS tr_timelog_id
                ,       timelog_user_email
                ,       timelog_time_spent
                ,       timelog_updated
                ,       timelog_note_id
                ,       hourly_timelogs_checked.transaction_id
                ,       'merge_request' AS timelog_kind
                FROM
                        dblink
                        (
                                'host={0} user={1} password={2} dbname={3}'
                        ,       '
                                SELECT
                                        projects.name                                                   AS project_name
                                ,       projects.path                                                   AS project_path
                                ,       namespaces.id                                                   AS project_namespace_id
                                ,       merge_requests.iid                                              AS merge_request_iid
                                ,       merge_requests.title                                            AS merge_request_title
                                ,       (
                                                SELECT
                                                        email
                                                FROM
                                                        users
                                                WHERE
                                                        merge_requests.author_id = users.id
                                        )                                                               AS merge_request_author
                                ,       to_char(merge_requests.created_at, ''YYYY-MM-DD HH24:MI:SS'')   AS merge_request_created
                                ,       NULL                                                            AS merge_request_closed
                                ,       (
                                                SELECT
                                                        string_agg(
                                                                (
                                                                        SELECT
                                                                                title
                                                                        FROM
                                                                                labels
                                                                        WHERE
                                                                                labels.id = label_links.label_id
                                                                )
                                                        , '', '')
                                                FROM
                                                        label_links
                                                WHERE
                                                        label_links.target_id = merge_requests.id
                                                        AND
                                                        target_type = ''MergeRequest''
                                        )                                                           AS merge_request_labels
                                ,       CASE
                                                WHEN ''Hourly'' IN
                                                        (
                                                                SELECT
                                                                        (
                                                                                SELECT
                                                                                        title
                                                                                FROM
                                                                                        labels
                                                                                WHERE
                                                                                        labels.id = label_links.label_id
                                                                        )
                                                                FROM
                                                                        label_links
                                                                WHERE
                                                                        label_links.target_id = merge_requests.id
                                                                        AND
                                                                        target_type = ''MergeRequest''
                                                        )
                                                THEN true
                                                ELSE false
                                        END                                                         AS merge_request_is_hourly
                                ,       timelogs.id                                                 AS timelog_id
                                ,       users.email                                                 AS timelog_user_email
                                ,       timelogs.time_spent                                         AS timelog_time_spent
                                ,       timelogs.spent_at                                           AS timelog_updated
                                ,       timelogs.note_id                                            AS note_id
                                FROM
                                        timelogs
                                ,       users
                                ,       merge_requests
                                ,       projects
                                ,       namespaces
                                WHERE
                                        timelogs.merge_request_id = merge_requests.id
                                        AND
                                        timelogs.user_id = users.id
                                        AND
                                        merge_requests.target_project_id = projects.id
                                        AND
                                        projects.namespace_id = namespaces.id
                                ;
                                '
                        )
                AS
                        hourly_merge_request_timelogs_checked_transaction_report
                        (
                                project_name            TEXT
                        ,       project_path            TEXT
                        ,       project_namespace_id    INT
                        ,       merge_request_iid       INT
                        ,       merge_request_title     TEXT
                        ,       merge_request_author    TEXT
                        ,       merge_request_created   TIMESTAMP
                        ,       merge_request_closed    TIMESTAMP
                        ,       merge_request_labels    TEXT
                        ,       merge_request_is_hourly BOOL
                        ,       timelog_id              INT
                        ,       timelog_user_email      TEXT
                        ,       timelog_time_spent      INT
                        ,       timelog_updated         TIMESTAMP
                        ,       timelog_note_id         TEXT
                        )
                ,       hourly_timelogs_checked
                WHERE
                        hourly_merge_request_timelogs_checked_transaction_report.timelog_id IN
                                (
                                        SELECT
                                                timelog_id
                                        FROM
                                                hourly_merge_request_timelogs_unchecked
                                )
                        AND
                        hourly_merge_request_timelogs_checked_transaction_report.timelog_id = hourly_timelogs_checked.timelog_id
                ORDER BY
                        tr_timelog_id
                ;
                """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME)
                logger.info("Query:")
                logger.info(sql)
               
                # Dict of lists to store hourly details for clients (no sense to mix different clients in one list)
                hourly_details = {}

                # imr belows stands for "issue or merge request"

                # Read rows
                try:
                    cur.execute(sql)
                    
                    for row in cur:
                       
                        # Set row fields
                        row_project_name         = row[0]
                        row_project_path         = row[1]
                        row_project_namespace_id = row[2]
                        row_imr_iid              = row[3]
                        row_imr_title            = row[4]
                        row_imr_author           = row[5]
                        row_imr_created          = row[6]
                        row_imr_closed           = row[7]
                        row_imr_labels           = row[8]
                        row_imr_is_hourly        = row[9]
                        row_timelog_id           = row[10]
                        row_user_email           = row[11]
                        row_time_spent           = row[12]
                        row_timelog_updated      = row[13]
                        row_timelog_note_id      = row[14]
                        row_transaction_id       = row[15]
                        row_timelog_kind         = row[16]

                        # Query namespace path - doing this as new query with unnest is much clearer than adding to the main query
                        sql = """
                        SELECT
                                STRING_AGG(namespace_path, '/') AS n_path
                        FROM
                                (
                                        SELECT
                                                *
                                        FROM
                                                dblink
                                                (
                                                        'host={0} user={1} password={2} dbname={3}'
                                                ,       '
                                                        SELECT
                                                                namespaces.path
                                                        FROM
                                                                namespaces
                                                        ,       (
                                                                        SELECT
                                                                                ordinality
                                                                        ,       unnest
                                                                        FROM
                                                                                unnest(
                                                                                        (
                                                                                                SELECT
                                                                                                        traversal_ids
                                                                                                FROM
                                                                                                        namespaces
                                                                                                WHERE
                                                                                                        id = {4}
                                                                                        )
                                                                                ) WITH ORDINALITY
                                                                ) AS namespaces_unnested
                                                        WHERE
                                                                namespaces_unnested.unnest = namespaces.id
                                                        ORDER BY
                                                                namespaces_unnested.ordinality
                                                        ;
                                                        '
                                                )
                                        AS
                                                namespace_paths
                                                (
                                                        namespace_path      TEXT
                                                )
                                ) AS ns_path
                        ;
                        """.format(GL_PG_DB_HOST, GL_PG_DB_USER, GL_PG_DB_PASS, GL_PG_DB_NAME, row_project_namespace_id)
                        logger.info("Query:")
                        logger.info(sql)
                        try:
                            sub_cur.execute(sql)
                            for sub_row in sub_cur:
                                row_project_namespace_path = sub_row[0]
                            logger.info("Query execution status:")
                            logger.info(sub_cur.statusmessage)
                        except Exception as e:
                            raise Exception("Caught exception on query execution")

                        row_project_path_with_namespace = row_project_namespace_path + "/" + row_project_path
                        if row_timelog_kind == "issue":
                            row_imr_link = row_project_namespace_path + "/" + row_project_path + "/issues/" + str(row_imr_iid)
                        elif row_timelog_kind == "merge_request":
                            row_imr_link = row_project_namespace_path + "/" + row_project_path + "/merge_requests/" + str(row_imr_iid)
                        if row_timelog_note_id is not None:
                            row_imr_link = row_imr_link + "#note_" + str(row_timelog_note_id)

                        # Init empty tariff for row
                        row_tariff_rate = 0
                        row_tariff_currency = ""
                        row_tariff_plan = ""
                        row_wc_pid = None

                        # Add to report if hourly imr only
                        if row_imr_is_hourly:

                            logger.info("Checking imr {}/{}".format(acc_yaml_dict["gitlab"]["url"], row_imr_link))

                            # Get imr labels
                            row_imr_labels_split = row_imr_labels.split(", ")
                            for row_imr_labels_split_label in row_imr_labels_split:
                                
                                # Init empty checked_tariffs
                                checked_tariffs = None

                                # Get client name
                                client_name = acc_yaml_dict["projects"][row_project_path_with_namespace]["client"].lower()

                                # Load client YAML
                                client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client_name, YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                                if client_dict is None:
                                    raise Exception("Config file error or missing: {0}/{1}/{2}.{3}".format(WORK_DIR, CLIENTS_SUBDIR, client_name, YAML_EXT))

                                # Check if other label name is asset
                                else:

                                    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now(), False)

                                    # If there are assets
                                    if len(asset_list) > 0:

                                        # Iterate over assets in client
                                        for client_asset in asset_list:

                                            # Check if asset name matches label
                                            if client_asset["fqdn"] == row_imr_labels_split_label:

                                                # Find checked tariff
                                                try:
                                                    checked_tariffs = activated_tariff(client_asset["tariffs"], row_timelog_updated, logger)["tariffs"]
                                                except:
                                                    logger.error("Asset {asset} imr {gitlab}/{imr} find active tariff error".format(asset=client_asset["fqdn"], gitlab=acc_yaml_dict["gitlab"]["url"], imr=row_imr_link))
                                                    raise

                                # Check if we have some tariff to check
                                # It is ok if None - it means the label is not asset label (not monetazible)
                                if checked_tariffs is not None:
                                
                                    # Init empty tariff
                                    checked_tariff_rate = 0
                                    checked_tariff_currency = ""

                                    # Take the first (upper and current) tariff and check it
                                    for checked_tariff in checked_tariffs:

                                        # If tariff has file key - load it
                                        if "file" in checked_tariff:
                                            
                                            tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, checked_tariff["file"]), logger)
                                            if tariff_dict is None:
                                                
                                                raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, checked_tariff["file"]))

                                            # Check if tariff has hourly
                                            if "hourly" in tariff_dict:

                                                # Only if no previous tariff found
                                                if checked_tariff_rate == 0 and checked_tariff_currency == "":

                                                    # Set found tariff
                                                    checked_tariff_rate = tariff_dict["hourly"]["rate"]
                                                    checked_tariff_currency = tariff_dict["hourly"]["currency"]
                                                    checked_tariff_plan = tariff_dict["service"] + " " + tariff_dict["plan"] + " rev. " + str(tariff_dict["revision"])
                                                    if "woocommerce_product_id" in tariff_dict["hourly"]:
                                                        checked_woocommerce_product_id = tariff_dict["hourly"]["woocommerce_product_id"]
                                                    else:
                                                        checked_woocommerce_product_id = None

                                                # If there is a previous tarfiff found
                                                else:

                                                    # Check if hourly tariff is not the same as found
                                                    if not (checked_tariff_rate == tariff_dict["hourly"]["rate"] and checked_tariff_currency == tariff_dict["hourly"]["currency"]):

                                                        error_text = "Error found on label {}, several tariffs may apply to the same label, but hourly rate should be the same, checked_tariff_rate = {}, tariff_dict_hourly_rate = {}, checked_tariff_currency = {}, tariff_dict_hourly_currency = {}".format(row_imr_labels_split_label, checked_tariff_rate, tariff_dict["hourly"]["rate"], checked_tariff_currency, tariff_dict["hourly"]["currency"])
                                                        if args.no_exceptions_on_label_errors:
                                                            print(error_text)
                                                        else:
                                                            raise Exception(error_text)

                                        # Also take inline plan and service
                                        else:
                                            
                                            # Check if tariff has hourly
                                            if "hourly" in checked_tariff:

                                                # Only if no previous tariff found
                                                if checked_tariff_rate == 0 and checked_tariff_currency == "":

                                                    # Set found tariff
                                                    checked_tariff_rate = checked_tariff["hourly"]["rate"]
                                                    checked_tariff_currency = checked_tariff["hourly"]["currency"]
                                                    checked_tariff_plan = checked_tariff["service"] + " " + checked_tariff["plan"] + " rev. " + str(checked_tariff["revision"])
                                                    if "woocommerce_product_id" in checked_tariff["hourly"]:
                                                        checked_woocommerce_product_id = checked_tariff["hourly"]["woocommerce_product_id"]
                                                    else:
                                                        checked_woocommerce_product_id = None

                                                # If there is a previous tarfiff found
                                                else:

                                                    # Check if hourly tariff is not the same as found
                                                    if not (checked_tariff_rate == checked_tariff["hourly"]["rate"] and checked_tariff_currency == checked_tariff["hourly"]["currency"]):

                                                        error_text = "Error found on label {}, several tariffs may apply to the same label, but hourly rate should be the same checked_tariff_rate = {}, checked_tariff_hourly_rate = {}, checked_tariff_currency = {}, checked_tariff_hourly_currency = {}".format(row_imr_labels_split_label, checked_tariff_rate, checked_tariff["hourly"]["rate"], checked_tariff_currency, checked_tariff["hourly"]["currency"])
                                                        if args.no_exceptions_on_label_errors:
                                                            print(error_text)
                                                        else:
                                                            raise Exception(error_text)

                                    # Only if no previous row tariff found
                                    if row_tariff_rate == 0 and row_tariff_currency == "":
                                    
                                        # Set row tariff from tariff
                                        row_tariff_rate = checked_tariff_rate
                                        row_tariff_currency = checked_tariff_currency
                                        row_tariff_plan = checked_tariff_plan
                                        row_wc_pid = checked_woocommerce_product_id

                                    # If there is a previous row tarfiff found
                                    else:

                                        # Check if hourly tariff is not the same as found
                                        if not (row_tariff_rate == checked_tariff_rate and row_tariff_currency == checked_tariff_currency):

                                            error_text = "Error found on label {} for imr {}/{}, several tariff labels may apply to the same imr, but hourly rate should be the same row_tariff_rate = {}, checked_tariff_rate = {}, row_tariff_currency = {}, checked_tariff_currency = {}".format(row_imr_labels_split_label, acc_yaml_dict["gitlab"]["url"], row_imr_link, row_tariff_rate, checked_tariff_rate, row_tariff_currency, checked_tariff_currency)
                                            if args.no_exceptions_on_label_errors:
                                                print(error_text)
                                            else:
                                                raise Exception(error_text)
                            
                            # Convert rate to float
                            row_tariff_rate = float(row_tariff_rate)
                            
                            # Check if tariff was found for the row
                            if row_tariff_rate == 0 or row_tariff_currency == "":

                                error_text = "Error found on imr {}/{}, each Hourly imr should has at least one tariff label which defines non zero tariff rate and currency".format(acc_yaml_dict["gitlab"]["url"], row_imr_link)
                                if args.no_exceptions_on_label_errors:
                                    print(error_text)
                                else:
                                    raise Exception(error_text)

                            # Else log rate and currency
                            else:

                                logger.info("Found hourly tariff rate {} and currency {} on imr {}/{}".format(row_tariff_rate, row_tariff_currency, acc_yaml_dict["gitlab"]["url"], row_imr_link))

                            # Calc row_time_spent_hours
                            row_time_spent_hours = round((row_time_spent/60)/60, 2)

                            # Prepare row to save in hourly_details
                            # We need to calculate employee share right here becase of different rates per hour, so time share doesn't give correct value
                            hourly_details_new_item = {
                                'project_name': row_project_name,
                                'project_link': acc_yaml_dict["gitlab"]["url"] + "/" + row_project_name,
                                'imr_title': row_imr_title,
                                'imr_link': acc_yaml_dict["gitlab"]["url"] + "/" + row_imr_link,
                                'imr_author': row_imr_author,
                                'imr_created': row_imr_created.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]),
                                'imr_closed': row_imr_closed.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]) if not row_imr_closed is None else "",
                                'imr_labels': ", ".join(sorted(row_imr_labels.split(", "))) if not row_imr_labels is None else "",
                                'timelog_employee_email': row_user_email,
                                'timelog_updated': row_timelog_updated.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]),
                                'timelog_spent_hours': row_time_spent_hours,
                                'tariff_currency': row_tariff_currency,
                                'tariff_rate': row_tariff_rate,
                                'tariff_plan': row_tariff_plan,
                                'woocommerce_product_id': row_wc_pid,
                                'timelog_cost': round(row_time_spent_hours * row_tariff_rate, 2),
                                'timelog_cost_employee_share': round((row_time_spent_hours * row_tariff_rate) * (acc_yaml_dict["employees"][row_user_email]["hourly_share"] / 100), 2)
                            }

                            # Init client timelogs list
                            if not client_name in hourly_details:
                                hourly_details[client_name] = []

                            # Save hourly details for a client
                            hourly_details[client_name].append(hourly_details_new_item)

                        # Save raw data to log
                        logger.info(row)

                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")

            # Monthly

            if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:

                # Read all or specific clients

                clients_dict = {}

                if args.make_monthly_invoice_for_all_clients is not None:
                    
                    month_in_arg, = args.make_monthly_invoice_for_all_clients

                    # For *.yaml in client dir

                    for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                        logger.info("Found client file: {0}".format(client_file))

                        # Load client YAML
                        client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                        if client_dict is None:
                            raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                        # Add only active clients and not excluded
                        if client_dict["active"] and not ("monthly_invoice_disabled" in client_dict["billing"] and client_dict["billing"]["monthly_invoice_disabled"]) and (
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
                                                        ):
                            clients_dict[client_dict["name"]] = client_dict
                
                else:

                    # Read specific client yaml

                    client_in_arg, month_in_arg = args.make_monthly_invoice_for_client

                    client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client_in_arg.lower(), YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                    clients_dict[client_dict["name"].lower()] = client_dict

                    if "monthly_invoice_disabled" in client_dict["billing"] and client_dict["billing"]["monthly_invoice_disabled"]:
                        raise Exception("You are trying to make monthly invoce for a client with monthly_invoice_disabled = True")

                # Iterate over clients to read assets for active clients

                client_asset_tariffs_dict = {}
                for client in clients_dict:
                    
                    client_dict = clients_dict[client]

                    if "month_shift" in client_dict["billing"]["papers"]:
                        month_delta = int(month_in_arg) + int(client_dict["billing"]["papers"]["month_shift"])
                    else:
                        month_delta = int(month_in_arg)

                    needed_month_for_tariff = datetime.today() + relativedelta(months=month_delta)

                    # Check if client is active
                    if client_dict["active"]:
                        
                        client_asset_tariffs_dict[client] = {}

                        asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                        # If there are assets
                        if len(asset_list) > 0:

                            # Iterate over assets in client
                            for asset in asset_list:

                                # Only active assets

                                if asset["active"]:

                                    # Skip assets with disabled monthly invoicing
                                    if "monthly_invoice_disabled" in asset and asset["monthly_invoice_disabled"]:
                                        logger.info("Monthly invoice disabled for asset: {0}".format(asset["fqdn"]))
                                        continue

                                    logger.info("Active asset: {0}".format(asset["fqdn"]))

                                    client_asset_tariffs_dict[client][asset["fqdn"]] = []

                                    # Find checked tariff
                                    for asset_tariff in activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["tariffs"]:

                                        # If tariff has file key - load it
                                        if "file" in asset_tariff:
                                            
                                            tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                                            if tariff_dict is None:
                                                
                                                raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                                            # Add tariff activation date per asset
                                            tariff_dict["activated_date"] = str(activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["activated"].strftime("%Y-%m-%d"))
                                            tariff_dict["added_date"] = str(activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["added"].strftime("%Y-%m-%d"))
                                        
                                            # Check tariff_older_than_activated_tariff
                                            if tariff_older_than_activated_tariff(asset["tariffs"], needed_month_for_tariff, logger) is not None:
                                                tariff_dict["older_tariff_exists"] = True
                                            else:
                                                tariff_dict["older_tariff_exists"] = False

                                            # Add migrated key
                                            if "migrated_from" in activated_tariff(asset["tariffs"], needed_month_for_tariff, logger):
                                                tariff_dict["migrated"] = True
                                            else:
                                                tariff_dict["migrated"] = False
                                            
                                            # Add monthly_employee_share key as dict
                                            if "monthly_employee_share" in asset_tariff:
                                                tariff_dict["monthly_employee_share"] = {}
                                                for empl_email, empl_share in asset_tariff["monthly_employee_share"].items():
                                                    tariff_dict["monthly_employee_share"][empl_email] = empl_share

                                            # Add tariff to the tariff list for the asset
                                            client_asset_tariffs_dict[client][asset["fqdn"]].append(tariff_dict)

                                        # Also take inline plan and service
                                        else:

                                            # Add tariff activation date per asset
                                            asset_tariff["activated_date"] = str(activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["activated"].strftime("%Y-%m-%d"))
                                            asset_tariff["added_date"] = str(activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["added"].strftime("%Y-%m-%d"))
                                            
                                            # Check tariff_older_than_activated_tariff
                                            if tariff_older_than_activated_tariff(asset["tariffs"], needed_month_for_tariff, logger) is not None:
                                                asset_tariff["older_tariff_exists"] = True
                                            else:
                                                asset_tariff["older_tariff_exists"] = False

                                            # Add migrated key
                                            if "migrated_from" in activated_tariff(asset["tariffs"], needed_month_for_tariff, logger):
                                                asset_tariff["migrated"] = True
                                            else:
                                                asset_tariff["migrated"] = False
                                            
                                            # Add monthly_employee_share key as dict
                                            if "monthly_employee_share" in asset_tariff:
                                                asset_tariff["monthly_employee_share"] = {}
                                                for empl_email, empl_share in asset_tariff["monthly_employee_share"].items():
                                                    asset_tariff["monthly_employee_share"][empl_email] = empl_share
                                
                                            # Add tariff to the tariff list for the asset
                                            client_asset_tariffs_dict[client][asset["fqdn"]].append(asset_tariff)

                                else:
                                    logger.info("Not active asset: {0}".format(asset["fqdn"]))
                        
                # We will need to read Invoices to know last billing date per client

                # Get Invoices raw data
                try:
                    invoices_raw_dict = sheets_get_as_json(SA_SECRETS_FILE, acc_yaml_dict["invoices"]["spreadsheet"], acc_yaml_dict["invoices"]["invoices"]["sheet"], acc_yaml_dict["invoices"]["invoices"]["range"], 'ROWS', 'FORMATTED_VALUE', 'FORMATTED_STRING')
                except Exception as e:
                    raise Exception("Caught exception on gsuite execution")

                # Get Monthly invoices dates per client
                invoices_dict = {}
                for invoices_line in invoices_raw_dict:
                    
                    invoices_order_dict = acc_yaml_dict["invoices"]["invoices"]["columns"]["order"]

                    invoices_line_client = invoices_line[invoices_order_dict['client'] - 1].lower()
                    if not invoices_line_client in invoices_dict:
                        invoices_dict[invoices_line_client] = []

                    if invoices_line[invoices_order_dict['type'] - 1] == "Monthly":

                        invoices_dict[invoices_line_client].append(
                            {
                                'date_created':         invoices_line[invoices_order_dict['date_created'] - 1],
                            }
                        )

                # Iterate over clients to prepare monthly details

                monthly_details = {}
                needed_month_per_client = {}
                for client in clients_dict:

                    monthly_details[client] = []

                    # Check invoice shift from client yaml if exist, if not = 0
                    
                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client.lower(), YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}/{2}.{3}".format(WORK_DIR, CLIENTS_SUBDIR, client, YAML_EXT))

                    if "month_shift" in client_dict["billing"]["papers"]:
                        month_delta = int(month_in_arg) + int(client_dict["billing"]["papers"]["month_shift"])
                    else:
                        month_delta = int(month_in_arg)

                    needed_month_per_client[client] = datetime.today() + relativedelta(months=month_delta)
                    monthly_period = str(needed_month_per_client[client].strftime("%Y-%m"))

                    # Iterate over client assets
                    for asset in client_asset_tariffs_dict[client]:

                        # Iterate over tariffs for the asset
                        for tariff in client_asset_tariffs_dict[client][asset]:
                            
                            # Calc period portion

                            # When debugging period portion remember:
                            # DELETE ALL test -N invoces from Invoices file, they affect last_client_billing_date

                            # Get crucial dates
                            activated_date_date = datetime.strptime(tariff["activated_date"], "%Y-%m-%d")
                            added_date_date = datetime.strptime(tariff["added_date"], "%Y-%m-%d")
                            if client.lower() in invoices_dict:
                                try:
                                    last_client_billing_date = datetime.strptime(invoices_dict[client.lower()][-1]["date_created"], "%Y-%m-%d")
                                except IndexError:
                                    last_client_billing_date = datetime.strptime("1970-01-01", "%Y-%m-%d")
                            else:
                                last_client_billing_date = datetime.strptime("1970-01-01", "%Y-%m-%d")
                            first_day_of_needed_month = needed_month_per_client[client].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                            last_day_of_needed_month = first_day_of_needed_month + relativedelta(months=1, days=-1)
                            first_day_of_activated_date_month = activated_date_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                            last_day_of_activated_date_month = first_day_of_activated_date_month + relativedelta(months=1, days=-1)

                            # Just add 1 portion for assets with older tariff exists, they were certainly billed before
                            if "older_tariff_exists" in tariff and tariff["older_tariff_exists"]:

                                # Just add one whole month
                                period_portion = 1

                            # Just add 1 portion for migrated assets, they were certainly billed before
                            elif "migrated" in tariff and tariff["migrated"]:

                                # Just add one whole month
                                period_portion = 1

                            else:

                                # Calculate diff between activated_date_date and last_day_of_needed_month
                                # We need +1 for each whole month and +0.x for partial month

                                # Give +1 for each month between last_day_of_activated_date_month and last_day_of_needed_month
                                whole_months = (last_day_of_needed_month.year - last_day_of_activated_date_month.year) * 12 + last_day_of_needed_month.month - last_day_of_activated_date_month.month

                                # Give decimal for partial month
                                partial_month = (last_day_of_activated_date_month.day - activated_date_date.day + 1) / (last_day_of_activated_date_month.day)

                                # Period portion is a sum of all whole months and partial month
                                period_portion = round(whole_months + partial_month, 2)

                                # We need to decide if we need to bill portion > 1 if it is larger than 1
                                if period_portion > 1:

                                    # If added_date_date <= last_client_billing_date --- no, the gap of billing was billed last time
                                    if (added_date_date - last_client_billing_date).days <= 0:

                                        # Just add one whole month
                                        period_portion = 1
                                
                                # If added_date_date > last_client_billing_date --- yes, we need to fill the gap of billing with portion > 1, so just leave portion as is

                                # If period_portion < 0 --- then activation_date is after last_day_of_needed_month, just take 0
                                if period_portion < 0:

                                    period_portion = 0
                            
                            # Calc employee share
                            price_in_period_employee_share = {}
                            if "monthly_employee_share" in tariff:
                                for empl_email, empl_share in tariff["monthly_employee_share"].items():
                                    price_in_period_employee_share[empl_email] = round(empl_share * tariff["monthly"]["rate"] * period_portion / 100, 2)

                            # Prepare row to save in monthly_details
                            monthly_details_new_item = {
                                "asset_fqdn":                       asset,
                                "activated_date":                   tariff["activated_date"],
                                "service":                          tariff["service"],
                                "plan":                             tariff["plan"],
                                "revision":                         tariff["revision"],
                                "tariff_plan":                      tariff["service"] + " " + tariff["plan"] + " rev. " + str(tariff["revision"]),
                                "tariff_currency":                  tariff["monthly"]["currency"],
                                "tariff_rate":                      tariff["monthly"]["rate"],
                                "period":                           monthly_period,
                                "period_portion":                   period_portion,
                                "price_in_period":                  round(tariff["monthly"]["rate"] * period_portion, 2),
                                "woocommerce_product_id":           tariff["monthly"]["woocommerce_product_id"] if "woocommerce_product_id" in tariff["monthly"] else None,
                                "price_in_period_employee_share":   price_in_period_employee_share
                            }

                            # Save monthly details for a client
                            monthly_details[client].append(monthly_details_new_item)

                    # Sort details:
                    # - Activation date
                    # - Asset FQDN within one date
                    monthly_details[client] = sorted(monthly_details[client], key = lambda x: (x["activated_date"], x["asset_fqdn"]))

            # Storage

            if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients is not None:

                if args.make_storage_invoice_for_client is not None:
                    needed_client, month_shift_back = args.make_storage_invoice_for_client
                if args.make_storage_invoice_for_all_clients is not None:
                    month_shift_back, = args.make_storage_invoice_for_all_clients
                    needed_client = None

                needed_month_for_tariff = datetime.today() - relativedelta(months=int(month_shift_back))

                # New cursor
                cur = conn.cursor()

                # Select all records for needed month

                sql = """
                SELECT
                        count(avg_per_day)
                ,       date_trunc('month', now() - interval '{month_shift}' month) AS usage_month /* month to compute, just for info */
                ,       date_part('days', date_trunc('month', now() - interval '{month_shift}' month) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL) AS days_in_month /* calc days in computed month, just for info, use the same later */
                ,       client_asset_fqdn
                ,       storage_asset_fqdn
                ,       storage_asset_path
                ,       sum(avg_per_day) / date_part('days', date_trunc('month', now() - interval '{month_shift}' month) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL)::INTEGER AS avg_per_month /* sum of averages for days of needed month / number of days in month */
                FROM
                        (
                                SELECT
                                        date(checked_at) AS date_checked_at
                                ,       client_asset_fqdn
                                ,       storage_asset_fqdn
                                ,       storage_asset_path
                                ,       count(date(checked_at))
                                ,       avg(mb_used)::INTEGER AS avg_per_day /* mb for the same days are taken as average */
                                FROM
                                        storage_usage
                                GROUP BY
                                        date_checked_at
                                ,       client_asset_fqdn
                                ,       storage_asset_fqdn
                                ,       storage_asset_path
                                ORDER BY
                                        date_checked_at
                        )
                AS
                        storage_usage_by_date_avg
                WHERE
                        date_trunc('month', now() - interval '{month_shift}' month)
                        =
                        date_trunc('month', date_checked_at)
                GROUP BY
                        client_asset_fqdn, storage_asset_fqdn, storage_asset_path
                ;
                """.format(month_shift=month_shift_back)
                logger.info("Query:")
                logger.info(sql)

                # Read rows and fill per asset dict
                try:
                    
                    cur.execute(sql)
                    
                    asset_storage_usage_monthly = {}

                    for row in cur:
                        
                        # Set row fields
                        row_usage_days          = row[0]
                        row_usage_month         = row[1]
                        row_days_in_month       = row[2]
                        row_client_asset_fqdn  = row[3]
                        row_storage_asset_fqdn = row[4]
                        row_storage_asset_path = row[5]
                        row_avg_per_month       = row[6]

                        asset_storage_usage_monthly[(row_client_asset_fqdn, row_storage_asset_fqdn, row_storage_asset_path)] = {
                                'usage_days':           row_usage_days,
                                'usage_month':          row_usage_month,
                                'avg_per_month':        row_avg_per_month
                        }
                        
                        logger.info("Storage usage for asset {asset}, storage_asset {storage_asset}, storage_path {storage_path}:".format(asset=row_client_asset_fqdn, storage_asset=row_storage_asset_fqdn, storage_path=row_storage_asset_path))
                        logger.info(asset_storage_usage_monthly[(row_client_asset_fqdn, row_storage_asset_fqdn, row_storage_asset_path)])

                    logger.info("Query execution status:")
                    logger.info(cur.statusmessage)
                except Exception as e:
                    raise Exception("Caught exception on query execution")

                # Dict of lists to store storage details for clients (no sense to mix different clients in one list)
                storage_details = {}

                # For *.yaml in client dir
                for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):
                    
                    logger.info("Found client file: {0}".format(client_file))

                    # Load client YAML
                    client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                    if client_dict is None:
                        raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                    # Check if specific client and client is active
                    if ((needed_client is not None and client_dict["name"].lower() == needed_client) or needed_client is None) and client_dict["active"] and (
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
                                                                                                                                                            ):

                        asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                        # If there are assets
                        if len(asset_list) > 0:

                            # Iterate over assets in client
                            for asset in asset_list:

                                if asset["active"] and "storage" in asset:
                                    
                                    logger.info("Active asset with storage: {0}".format(asset["fqdn"]))

                                    # Find storage tariff

                                    storage_tariff_found = False
                                    
                                    for tariff in activated_tariff(asset["tariffs"], needed_month_for_tariff, logger)["tariffs"]:

                                        # If tariff has file key - load it
                                        if "file" in tariff:
                                            
                                            tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, tariff["file"]), logger)
                                            if tariff_dict is None:
                                                
                                                raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, tariff["file"]))

                                            # Check if tariff has storage

                                            if "storage" in tariff_dict:

                                                # If storage tariff already found on prev step - error
                                                if storage_tariff_found:
                                                    raise Exception("Storage tariff found more than once for asset {asset}".format(asset=asset["fqdn"]))

                                                checked_tariff_rate = tariff_dict["storage"]["rate"]
                                                checked_tariff_currency = tariff_dict["storage"]["currency"]
                                                checked_tariff_plan = tariff_dict["service"] + " " + tariff_dict["plan"] + " rev. " + str(tariff_dict["revision"])
                                                if "woocommerce_product_id" in tariff_dict["storage"]:
                                                    checked_woocommerce_product_id = tariff_dict["storage"]["woocommerce_product_id"]
                                                else:
                                                    checked_woocommerce_product_id = None
                                                storage_tariff_found = True

                                        # Also take inline plan and service
                                        else:
                                            
                                            # Check if tariff has storage
                                            if "storage" in tariff:

                                                # If storage tariff already found on prev step - error
                                                if storage_tariff_found:
                                                    raise Exception("Storage tariff found more than once for asset {asset}".format(asset=asset["fqdn"]))

                                                checked_tariff_rate = tariff["storage"]["rate"]
                                                checked_tariff_currency = tariff["storage"]["currency"]
                                                checked_tariff_plan = tariff["service"] + " " + tariff["plan"] + " rev. " + str(tariff["revision"])
                                                if "woocommerce_product_id" in tariff["storage"]:
                                                    checked_woocommerce_product_id = tariff["storage"]["woocommerce_product_id"]
                                                else:
                                                    checked_woocommerce_product_id = None
                                                storage_tariff_found = True
                                    
                                    if not storage_tariff_found:

                                        raise Exception("Storage tariff not found for asset {asset}".format(asset=asset["fqdn"]))

                                    # Set row tariff from tariff
                                    row_tariff_rate = checked_tariff_rate
                                    row_tariff_currency = checked_tariff_currency
                                    row_tariff_plan = checked_tariff_plan
                                    row_wc_pid = checked_woocommerce_product_id
                                    
                                    # Convert rate to float
                                    row_tariff_rate = float(row_tariff_rate)
                                    
                                    # Check if non empty currency was found for the row, zero tariff is ok (e.g. storing backups of vps on our hypervisors)
                                    if row_tariff_currency == "":

                                        raise Exception("Storage tariff for asset {asset} has empty currency".format(asset=asset["fqdn"]))

                                    # Else log rate and currency
                                    else:

                                        logger.info("Found storage tariff rate {rate} and currency {currency} for asset {asset}".format(rate=row_tariff_rate, currency=row_tariff_currency, asset=asset["fqdn"]))

                                    # Migrated storage history kept in ex_storage, otherwise billing logic will not find in DB previous data within one month
                                    # So join two lists for billing
                                    if "ex_storage" in asset:
                                        storage_items = asset["storage"] + asset["ex_storage"]
                                    else:
                                        storage_items = asset["storage"]

                                    # Walk for storage items
                                    for storage_item in storage_items:

                                        for storage_asset, storage_paths in storage_item.items():

                                            for storage_path in storage_paths:

                                                # Check if asset has storage usage records and add rows to details if any
                                                if (asset["fqdn"], storage_asset, storage_path) in asset_storage_usage_monthly:

                                                    storage_details_new_item = {
                                                        'client_asset_fqdn':        asset["fqdn"],
                                                        'storage_asset_fqdn':       storage_asset,
                                                        'storage_asset_path':       storage_path,
                                                        'usage_days':               asset_storage_usage_monthly[(asset["fqdn"], storage_asset, storage_path)]["usage_days"],
                                                        'usage_month':              str(asset_storage_usage_monthly[(asset["fqdn"], storage_asset, storage_path)]["usage_month"].strftime("%Y-%m")),
                                                        'avg_per_month':            round(asset_storage_usage_monthly[(asset["fqdn"], storage_asset, storage_path)]["avg_per_month"] / 1000, 2),
                                                        'tariff_currency':          row_tariff_currency,
                                                        'tariff_rate':              row_tariff_rate,
                                                        'tariff_plan':              row_tariff_plan,
                                                        'woocommerce_product_id':   row_wc_pid,
                                                        'storage_cost':             round(round(asset_storage_usage_monthly[(asset["fqdn"], storage_asset, storage_path)]["avg_per_month"] / 1000, 2) * row_tariff_rate, 2)
                                                    }

                                                    # Init client storage list
                                                    if not client_dict["name"].lower() in storage_details:
                                                        storage_details[client_dict["name"].lower()] = []

                                                    # Save storage details for a client
                                                    storage_details[client_dict["name"].lower()].append(storage_details_new_item)

                        # Sort details and log:
                        if client_dict["name"].lower() in storage_details:
                            
                            storage_details[client_dict["name"].lower()] = sorted(storage_details[client_dict["name"].lower()], key = lambda x: (x["client_asset_fqdn"], x["storage_asset_fqdn"], x["storage_asset_path"]))
                            logger.info("Storage details for client {client}".format(client=client_dict["name"].lower()))
                            logger.info(storage_details[client_dict["name"].lower()])

            # Make documents

            # Iterate over clients, make actions depending on invoice type for each client in dict
            if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                    invoice_details = hourly_details
            if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:
                    invoice_details = monthly_details
            if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients is not None:
                    invoice_details = storage_details

            for client in invoice_details:

                # Skip client if invoice_details is empty (e.g. no assets, only hourly)
                if not invoice_details[client]:
                    continue
                
                # Save client raw invoice details to log for debug
                logger.info("Client {0} invoice raw details:".format(client))
                logger.info(json.dumps(invoice_details[client], indent=2))
                        
                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, "{0}/{1}.{2}".format(CLIENTS_SUBDIR, client.lower(), YAML_EXT), CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}/{2}.{3}".format(WORK_DIR, CLIENTS_SUBDIR, client, YAML_EXT))

                # Check if papers needed or not
                if client_dict["billing"]["papers"]["invoice"]["print"] or client_dict["billing"]["papers"]["act"]["print"]:
                    papers_needed = "Needed"
                else:
                    papers_needed = "Not Needed"

                # Set invoice type and vars for it

                if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                    invoice_type = "Hourly"
                    invoice_prefix = "H-"
                    
                    # Details are ordered by date, so period is: date of of first detail - date of last detail
                    client_doc_invoice_period = invoice_details[client][0]["timelog_updated"] + ' - ' + invoice_details[client][-1]["timelog_updated"]

                if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:
                    invoice_type = "Monthly"
                    invoice_prefix = "M-"

                    # Take period from first detail
                    client_doc_invoice_period = invoice_details[client][0]["period"]
                    
                if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients is not None:
                    invoice_type = "Storage"
                    invoice_prefix = "S-"

                    # Take usage_month from first detail
                    client_doc_invoice_period = invoice_details[client][0]["usage_month"]

                    # Save needed_month for later in docs
                    needed_month = invoice_details[client][0]["usage_month"]

                # Sanity checks
                # Check currency
                for detail in invoice_details[client]:
                    if detail["tariff_currency"] != acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency"]:
                        if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                            logger.error("Issue or MR {imr} has non valid currency {currency} for the merchant {merchant} and template {template}".format(
                                imr=detail["imr_link"],
                                currency=detail["tariff_currency"],
                                merchant=client_dict["billing"]["merchant"],
                                template=client_dict["billing"]["template"]
                            ))
                        if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:
                            logger.error("Asset {asset} has non valid currency {currency} for the merchant {merchant} and template {template}".format(
                                asset=detail["asset_fqdn"],
                                currency=detail["tariff_currency"],
                                merchant=client_dict["billing"]["merchant"],
                                template=client_dict["billing"]["template"]
                            ))
                        if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients is not None:
                            logger.error("Asset {asset} has non valid currency {currency} for the merchant {merchant} and template {template}".format(
                                asset=detail["client_asset_fqdn"],
                                currency=detail["tariff_currency"],
                                merchant=client_dict["billing"]["merchant"],
                                template=client_dict["billing"]["template"]
                            ))
                        raise Exception("Error in currency found")
        
                # Get and save once document list in client folder
                try:
                    client_folder_files = drive_ls(SA_SECRETS_FILE, client_dict["gsuite"]["folder"], acc_yaml_dict["gsuite"]["drive_user"])
                except Exception as e:
                    raise Exception("Caught exception on gsuite execution")

                # Doc number
                client_doc_num = invoice_prefix + client_dict["billing"]["code"] + "-" + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["code"] + "-" + str(datetime.today().strftime("%Y-%m-%d"))
                latest_subnum = "01"

                # Get latest invoice for client for today and increase latest_subnum if found
                for item in client_folder_files:
                    fn_prefix = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][0]
                    if item["name"].startswith(fn_prefix):
                        part_1 = item["name"].replace(fn_prefix + " ", "")
                        if part_1.startswith(client_doc_num):
                            part_2 = part_1.replace(client_doc_num + "-", "")
                            part_3 = part_2.split(" " + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][1] + " ")
                            if int(part_3[0]) >= int(latest_subnum):
                                latest_subnum = str(int(part_3[0]) + 1).zfill(2)

                logger.info("Doc number: {0}".format(client_doc_num + "-" + latest_subnum))

                # Prepare the data

                details_row_n = 0
                client_total = float(0)
                client_doc_details_list = []
                client_per_tariff_plan_total_sum = {}
                client_per_tariff_plan_total_qty = {}
                client_per_tariff_plan_tariff_rate = {}
                client_per_tariff_plan_woocommerce_product_id = {}
                client_per_employee_share = {}

                # Calc invoice data depending on invoice type

                # Hourly

                if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                    
                    for detail in invoice_details[client]:

                        # Details row number
                        details_row_n += 1

                        # Total cost
                        client_total += detail["timelog_cost"]

                        # Precalc per tariff dict
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_sum:
                            client_per_tariff_plan_total_sum[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_sum[detail["tariff_plan"]] += detail["timelog_cost"]
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_qty:
                            client_per_tariff_plan_total_qty[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_qty[detail["tariff_plan"]] += detail["timelog_spent_hours"]
                        client_per_tariff_plan_tariff_rate[detail["tariff_plan"]] = detail["tariff_rate"]
                        client_per_tariff_plan_woocommerce_product_id[detail["tariff_plan"]] = detail["woocommerce_product_id"]
                        
                        # Precalc per employee
                        if detail["timelog_employee_email"] not in client_per_employee_share:
                            client_per_employee_share[detail["timelog_employee_email"]] = float(0)
                        client_per_employee_share[detail["timelog_employee_email"]] += detail["timelog_cost_employee_share"]

                        # Format detail rows with template settings
                        if acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["decimal"] == "," and acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["thousands"] == " ":
                            formatted_timelog_spent_hours = '{:,.2f}'.format(detail["timelog_spent_hours"]).replace(",", " ").replace(".", ",")
                            formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"]).replace(",", " ").replace(".", ",")
                            formatted_timelog_cost = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["timelog_cost"]).replace(",", " ").replace(".", ",")
                        else:
                            formatted_timelog_spent_hours = '{:,.2f}'.format(detail["timelog_spent_hours"])
                            formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"])
                            formatted_timelog_cost = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["timelog_cost"])
                        
                        # Populate details rows
                        client_doc_details_list_item = [
                            str(details_row_n),
                            detail["imr_link"],
                            detail["imr_title"],
                            detail["imr_labels"] + "\n" + detail["tariff_plan"],
                            detail["timelog_updated"],
                            formatted_timelog_spent_hours,
                            formatted_tariff_rate,
                            formatted_timelog_cost
                        ]
                        client_doc_details_list.append(client_doc_details_list_item)

                # Monthly

                if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:

                    for detail in invoice_details[client]:

                        # Details row number
                        details_row_n += 1

                        # Total cost
                        client_total += detail["price_in_period"]

                        # Precalc per tariff dict
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_sum:
                            client_per_tariff_plan_total_sum[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_sum[detail["tariff_plan"]] += detail["price_in_period"]
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_qty:
                            client_per_tariff_plan_total_qty[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_qty[detail["tariff_plan"]] += detail["period_portion"]
                        client_per_tariff_plan_tariff_rate[detail["tariff_plan"]] = detail["tariff_rate"]
                        client_per_tariff_plan_woocommerce_product_id[detail["tariff_plan"]] = detail["woocommerce_product_id"]
                        
                        # Precalc per employee
                        if "price_in_period_employee_share" in detail:
                            for empl_email, empl_share in detail["price_in_period_employee_share"].items():
                                if empl_email not in client_per_employee_share:
                                    client_per_employee_share[empl_email] = float(0)
                                client_per_employee_share[empl_email] += empl_share

                        # Format detail rows with template settings
                        if acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["decimal"] == "," and acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["thousands"] == " ":
                            formatted_period_portion = '{:,.2f}'.format(detail["period_portion"]).replace(",", " ").replace(".", ",")
                            formatted_monthly_tariff_price = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"]).replace(",", " ").replace(".", ",")
                            formatted_price_within_period = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["price_in_period"]).replace(",", " ").replace(".", ",")

                        else:
                            formatted_period_portion = '{:,.2f}'.format(detail["period_portion"])
                            formatted_monthly_tariff_price = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"])
                            formatted_price_within_period = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["price_in_period"])

                        # Populate details rows
                        client_doc_details_list_item = [
                            str(details_row_n),
                            detail["asset_fqdn"],
                            detail["activated_date"],
                            detail["service"],
                            detail["plan"],
                            str(detail["revision"]),
                            formatted_monthly_tariff_price,
                            client_doc_invoice_period,
                            formatted_period_portion,
                            formatted_price_within_period
                        ]
                        client_doc_details_list.append(client_doc_details_list_item)

                # Storage

                if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients:
                    
                    for detail in invoice_details[client]:

                        # Details row number
                        details_row_n += 1

                        # Total cost
                        client_total += detail["storage_cost"]

                        # Precalc per tariff dict
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_sum:
                            client_per_tariff_plan_total_sum[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_sum[detail["tariff_plan"]] += detail["storage_cost"]
                        if detail["tariff_plan"] not in client_per_tariff_plan_total_qty:
                            client_per_tariff_plan_total_qty[detail["tariff_plan"]] = float(0)
                        client_per_tariff_plan_total_qty[detail["tariff_plan"]] += detail["avg_per_month"]
                        client_per_tariff_plan_tariff_rate[detail["tariff_plan"]] = detail["tariff_rate"]
                        client_per_tariff_plan_woocommerce_product_id[detail["tariff_plan"]] = detail["woocommerce_product_id"]
                        
                        # Format detail rows with template settings
                        if acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["decimal"] == "," and acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["thousands"] == " ":
                            formatted_avg_per_month = '{:,.2f}'.format(detail["avg_per_month"]).replace(",", " ").replace(".", ",")
                            formatted_usage_days = '{:,.2f}'.format(detail["usage_days"]).replace(",", " ").replace(".", ",")
                            formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"]).replace(",", " ").replace(".", ",")
                            formatted_storage_cost = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["storage_cost"]).replace(",", " ").replace(".", ",")
                        else:
                            formatted_avg_per_month = '{:,.2f}'.format(detail["avg_per_month"])
                            formatted_usage_days = '{:,.2f}'.format(detail["usage_days"])
                            formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["tariff_rate"])
                            formatted_storage_cost = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(detail["storage_cost"])
                        
                        # Populate details rows
                        client_doc_details_list_item = [
                            str(details_row_n),
                            detail["client_asset_fqdn"],
                            detail["storage_asset_fqdn"],
                            detail["storage_asset_path"],
                            formatted_usage_days,
                            formatted_avg_per_month,
                            formatted_tariff_rate,
                            formatted_storage_cost
                        ]
                        client_doc_details_list.append(client_doc_details_list_item)


                # Make WooCommerce order draft if woocommerce key in merchant and woocommerce: True in client

                if "woocommerce" in client_dict["billing"] and client_dict["billing"]["woocommerce"]["draft_order"] and not args.dry_run_woocommerce:

                    # Prepare order items, API doesn't accept decimal until:
                    # /var/www/microdevopscom/wordpress/wp-content/plugins/woocommerce/includes/rest-api/Controllers/Version2/class-wc-rest-orders-v2-controller.php
                    # under "Quantity ordered" integer changed to float.
                    order_items = []
                    for tariff_plan in sorted(client_per_tariff_plan_tariff_rate):
                        order_items.append(
                            {
                                "product_id": client_per_tariff_plan_woocommerce_product_id[tariff_plan],
                                "quantity": client_per_tariff_plan_total_qty[tariff_plan],
                                "meta_data": [
                                    {
                                        "key": "Period",
                                        "value": client_doc_invoice_period
                                    },
                                    {
                                        "key": "Quantity",
                                        "value": client_per_tariff_plan_total_qty[tariff_plan]
                                    }
                                ]
                            }
                        )

                    try:
                        
                        # Create api object
                        wcapi = woocommerce.API(
                            url=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["woocommerce"]["url"],
                            consumer_key=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["woocommerce"]["key"],
                            consumer_secret=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["woocommerce"]["secret"],
                            version=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["woocommerce"]["version"]
                        )

                        # Get customer billing and shipping
                        woo_customer = wcapi.get("customers/{id}".format(id=client_dict["billing"]["woocommerce"]["customer_id"])).json()
                        if woo_customer["billing"]["email"] == "":
                            woo_customer["billing"]["email"] = woo_customer["email"]

                        # Prepare the data
                        data = {
                            "set_paid": False,
                            "status": "pending",
                            "customer_id": client_dict["billing"]["woocommerce"]["customer_id"],
                            "billing": woo_customer["billing"],
                            "shipping": woo_customer["shipping"],
                            "line_items": order_items
                        }
                        logger.info("Woo order data to insert:")
                        logger.info(json.dumps(data, indent=2))

                        # Request
                        response = wcapi.post("orders", data)
                        logger.info("Woo api response:")
                        logger.info(response.json())

                        # Check status code
                        if response.status_code != 201:
                            raise Exception("Got non 201 status code from woo api: {status_code}".format(status_code=response.status_code))

                        # Get response needed keys
                        woocommerce_order_id = response.json()["number"]
                        woocommerce_order_currency = response.json()["currency"]

                        # Prepare the data for note update request
                        data = {
                            "note": "client: {client}\ncustomer_id: {customer_id}".format(customer_id=client_dict["billing"]["woocommerce"]["customer_id"], client=client_dict["name"])
                        }

                        # Request
                        response = wcapi.post("orders/{order_id}/notes".format(order_id=woocommerce_order_id), data)
                        logger.info("Woo api response:")
                        logger.info(response.json())
                        
                        # Check status code
                        if response.status_code != 201:
                            raise Exception("Got non 201 status code from woo api: {status_code}".format(status_code=response.status_code))

                        # Check order currency with mechant currency
                        if woocommerce_order_currency != acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency"]:
                            logger.error("Woo order {order} currency {order_currency} didn't match merchant {merchant} template {template} currency {merchant_currency}".format(
                                order=woocommerce_order_id,
                                order_currency=woocommerce_order_currency,
                                merchant=client_dict["billing"]["merchant"],
                                template=client_dict["billing"]["template"],
                                merchant_currency=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency"]
                            ))
                            raise Exception("Error in currency found")

                        # TODO: We could also check total from order and invoice later

                    except Exception as e:
                        raise Exception("Caught exception on woo api query")

                # Else just set empty woocommerce_order_id for templates
                else:
                    woocommerce_order_id = ""

                # Populate invoice rows

                invoice_row_n = 0
                client_doc_invoice_list = []
                # Keys in client_per_tariff_plan_total_sum and formatted_client_per_tariff_plan_total_qty are the same, only one iteration needed
                # Sort by tariff plan
                for tariff_plan in sorted(client_per_tariff_plan_tariff_rate):
                    
                    # Invoice row number
                    invoice_row_n += 1

                    # Format client_per_tariff_plan_total_sum after calcs
                    if acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["decimal"] == "," and acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["thousands"] == " ":
                        formatted_client_per_tariff_plan_total_sum = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_per_tariff_plan_total_sum[tariff_plan]).replace(",", " ").replace(".", ",")
                        formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_per_tariff_plan_tariff_rate[tariff_plan]).replace(",", " ").replace(".", ",")
                        formatted_client_per_tariff_plan_total_qty = '{:,.2f}'.format(client_per_tariff_plan_total_qty[tariff_plan]).replace(",", " ").replace(".", ",")
                    else:
                        formatted_client_per_tariff_plan_total_sum = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_per_tariff_plan_total_sum[tariff_plan])
                        formatted_tariff_rate = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_per_tariff_plan_tariff_rate[tariff_plan])
                        formatted_client_per_tariff_plan_total_qty = '{:,.2f}'.format(client_per_tariff_plan_total_qty[tariff_plan])

                    # Rows
                    client_doc_invoice_list_item = [
                        str(invoice_row_n),
                        acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["row_name"].format(plan=tariff_plan),
                        client_doc_invoice_period,
                        formatted_client_per_tariff_plan_total_qty,
                        formatted_tariff_rate,
                        formatted_client_per_tariff_plan_total_sum
                    ]
                    client_doc_invoice_list.append(client_doc_invoice_list_item)
                
                # Format totals after calcs
                if acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["decimal"] == "," and acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["thousands"] == " ":
                    formatted_client_total = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_total).replace(",", " ").replace(".", ",")
                else:
                    formatted_client_total = acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency_symbol"] + '{:,.2f}'.format(client_total)
                
                client_total_written = num2words(client_total,
                    to='currency',
                    lang=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["language"],
                    currency=acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency"])

                # Calc dates

                # Hourly
                if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                    # Date of last timelog
                    invoice_act_date = invoice_details[client][-1]["timelog_updated"]
                    # Today + 2 weeks
                    in_two_weeks = datetime.today() + timedelta(days=14)
                    invoice_last_day = str(in_two_weeks.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))

                # Monthly
                if args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients is not None:
                    if "monthly_act_last_date_of_month" in client_dict["billing"]["papers"] and client_dict["billing"]["papers"]["monthly_act_last_date_of_month"]:
                        # Last day of month by period
                        next_month = needed_month_per_client[client] + relativedelta(months=1)
                        first_day_of_next_month = next_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        last_day_of_needed_month = first_day_of_next_month - relativedelta(days=1)
                        invoice_act_date = str(last_day_of_needed_month.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))
                    else:
                        # First day of next month by period
                        next_month = needed_month_per_client[client] + relativedelta(months=1)
                        first_day_of_next_month = next_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        invoice_act_date = str(first_day_of_next_month.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))
                    # Today + 4 weeks
                    in_four_weeks = datetime.today() + timedelta(days=28)
                    invoice_last_day = str(in_four_weeks.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))
                
                # Storage
                if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients:
                    if "monthly_act_last_date_of_month" in client_dict["billing"]["papers"] and client_dict["billing"]["papers"]["monthly_act_last_date_of_month"]:
                        # Last day of month by period
                        next_month = datetime.strptime(needed_month, "%Y-%m") + relativedelta(months=1)
                        first_day_of_next_month = next_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        last_day_of_needed_month = first_day_of_next_month - relativedelta(days=1)
                        invoice_act_date = str(last_day_of_needed_month.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))
                    else:
                        # First day of next month by period
                        next_month = datetime.strptime(needed_month, "%Y-%m") + relativedelta(months=1)
                        first_day_of_next_month = next_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        invoice_act_date = str(first_day_of_next_month.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))
                    # Today + 2 weeks
                    in_two_weeks = datetime.today() + timedelta(days=14)
                    invoice_last_day = str(in_two_weeks.strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"]))

                # Copy templates

                # Invoice
                if not args.dry_run_gsuite:
                    try:
                        client_doc_invoice = drive_cp(SA_SECRETS_FILE, acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["source"],
                            client_dict["gsuite"]["folder"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][0] + " " + client_doc_num + "-" + latest_subnum + 
                                " " + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["filename"][1] + " " + str(datetime.today().strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"])), acc_yaml_dict["gsuite"]["drive_user"])
                        logger.info("New invoice id: {0}".format(client_doc_invoice))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Details
                if not args.dry_run_gsuite:
                    try:
                        client_doc_details = drive_cp(SA_SECRETS_FILE, acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["source"],
                            client_dict["gsuite"]["folder"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["filename"][0] + " " + client_doc_num + "-" + latest_subnum + 
                                " " + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["filename"][1] + " " + str(datetime.today().strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"])), acc_yaml_dict["gsuite"]["drive_user"])
                        logger.info("New details id: {0}".format(client_doc_details))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Act
                if not args.dry_run_gsuite and (client_dict["billing"]["papers"]["act"]["email"] or client_dict["billing"]["papers"]["act"]["print"]):
                    try:
                        client_doc_act = drive_cp(SA_SECRETS_FILE, acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["source"],
                            client_dict["gsuite"]["folder"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["filename"][0] + " " + client_doc_num + "-" + latest_subnum + 
                                " " + acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["filename"][1] + " " + invoice_act_date, acc_yaml_dict["gsuite"]["drive_user"])
                        logger.info("New act id: {0}".format(client_doc_act))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Templates
                
                client_doc_data = {
                    "__CONTRACT_RECIPIENT__":   client_dict["billing"]["contract"]["recipient"],
                    "__CONTRACT_DETAILS__":     client_dict["billing"]["contract"]["details"],
                    "__CONTRACT_NAME__":        client_dict["billing"]["contract"]["name"],
                    "__CONTRACT_PERSON__":      client_dict["billing"]["contract"]["person"],
                    "__SIGN__":                 client_dict["billing"]["contract"]["sign"],
                    "__INVOICE_NUM__":          client_doc_num + "-" + latest_subnum,
                    "__ACT_NUM__":              client_doc_num + "-" + latest_subnum,
                    "__INVOICE_DATE__":         str(datetime.today().strftime(acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["date_format"])),
                    "__ACT_DATE__":             invoice_act_date,
                    "__INV_L_DATE__":           invoice_last_day,
                    "__TOTAL__":                formatted_client_total,
                    "__TOTAL_WRITTEN__":        client_total_written,
                    "__ORDER_NUM__":            woocommerce_order_id
                }
                
                # Employee Share sheet

                invoices_rows_emplloyee = []

                # Employee Share sheet for Hourly or Monthly
                if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients \
                or args.make_monthly_invoice_for_client is not None or args.make_monthly_invoice_for_all_clients:
                    for employee in client_per_employee_share:

                        # Make row of empty values of range size
                        invoices_rows_emplloyee_row = []
                        for c in range(calculate_range_size(acc_yaml_dict["invoices"]["employee_share"]["range"])):
                            invoices_rows_emplloyee_row.append("")

                        # Set needed values row
                        employee_order_dict = acc_yaml_dict["invoices"]["employee_share"]["columns"]["order"]
                        invoices_rows_emplloyee_row[employee_order_dict["employee"] - 1] =             employee
                        invoices_rows_emplloyee_row[employee_order_dict["invoice_number"] - 1] =       client_doc_num + "-" + latest_subnum
                        # In case of negative timelogs usage, total time could become zero -> division by zero -> employee share of zero = zero
                        if client_total != 0:
                            invoices_rows_emplloyee_row[employee_order_dict["share"] - 1] =                round(((client_per_employee_share[employee] / client_total) * 100), 2)
                        else:
                            invoices_rows_emplloyee_row[employee_order_dict["share"] - 1] =                0
                        invoices_rows_emplloyee_row[employee_order_dict["currency_received"] - 1] =             acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["currency_received"]
                        invoices_rows_emplloyee_row[employee_order_dict["sum_received"] - 1] =                  acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["sum_received"]
                        invoices_rows_emplloyee_row[employee_order_dict["sum_after_taxes"] - 1] =               acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["sum_after_taxes"]
                        invoices_rows_emplloyee_row[employee_order_dict["employee_sum"] - 1] =                  acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["employee_sum"]
                        invoices_rows_emplloyee_row[employee_order_dict["invoice_currency"] - 1] =              acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["invoice_currency"]
                        invoices_rows_emplloyee_row[employee_order_dict["invoice_sum"] - 1] =                   acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["invoice_sum"]
                        invoices_rows_emplloyee_row[employee_order_dict["invoice_sum_after_taxes"] - 1] =       acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["invoice_sum_after_taxes"]
                        invoices_rows_emplloyee_row[employee_order_dict["employee_sum_by_invoice"] - 1] =       acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["employee_sum_by_invoice"]
                        invoices_rows_emplloyee_row[employee_order_dict["employee_sum_by_invoice_conv"] - 1] =  acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["employee_sum_by_invoice_conv"]
                        invoices_rows_emplloyee_row[employee_order_dict["employee_sum_to_pay_projected"] - 1] = acc_yaml_dict["invoices"]["employee_share"]["columns"]["defaults"]["employee_sum_to_pay_projected"]

                        # Append
                        invoices_rows_emplloyee.append(invoices_rows_emplloyee_row)

                # Invoices sheet

                # Make row of empty values of range size
                invoices_rows_invoices_row = []
                for c in range(calculate_range_size(acc_yaml_dict["invoices"]["invoices"]["range"])):
                    invoices_rows_invoices_row.append("")

                # Set needed values row
                invoices_order_dict = acc_yaml_dict["invoices"]["invoices"]["columns"]["order"]
                invoices_rows_invoices_row[invoices_order_dict['date_created'] - 1] =           str(datetime.today().strftime("%Y-%m-%d"))
                invoices_rows_invoices_row[invoices_order_dict['type'] - 1] =                   invoice_type
                invoices_rows_invoices_row[invoices_order_dict['period'] - 1] =                 client_doc_invoice_period
                invoices_rows_invoices_row[invoices_order_dict['client'] - 1] =                 client_dict["name"]
                invoices_rows_invoices_row[invoices_order_dict['merchant'] - 1] =               client_dict["billing"]["merchant"]
                invoices_rows_invoices_row[invoices_order_dict['invoice_number'] - 1] =         client_doc_num + "-" + latest_subnum
                invoices_rows_invoices_row[invoices_order_dict['invoice_currency'] - 1] =       acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]]["currency"]
                invoices_rows_invoices_row[invoices_order_dict['invoice_sum'] - 1] =            client_total
                invoices_rows_invoices_row[invoices_order_dict['status'] - 1] =                 "Prepared"
                invoices_rows_invoices_row[invoices_order_dict['ext_order_number'] - 1] =       woocommerce_order_id
                invoices_rows_invoices_row[invoices_order_dict['papers'] - 1] =                 papers_needed

                invoices_rows_invoices = [invoices_rows_invoices_row]

                # Fill the data

                # Invoice

                # Templates
                if not args.dry_run_gsuite:
                    try:
                        response = docs_replace_all_text(SA_SECRETS_FILE, client_doc_invoice, json.dumps(client_doc_data))
                        logger.info("{invoice_type} invoice docs_replace_all_text response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Table rows fill
                if not args.dry_run_gsuite:
                    try:
                        response = docs_insert_table_rows(SA_SECRETS_FILE, client_doc_invoice,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["row_num"],
                            json.dumps(client_doc_invoice_list))
                        logger.info("{invoice_type} invoice docs_insert_table_rows response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")
                
                # First row removal
                if not args.dry_run_gsuite:
                    try:
                        response = docs_delete_table_row(SA_SECRETS_FILE, client_doc_invoice,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["invoice"]["row_num"])
                        logger.info("{invoice_type} invoice docs_delete_table_row response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Details

                # Templates
                if not args.dry_run_gsuite:
                    try:
                        response = docs_replace_all_text(SA_SECRETS_FILE, client_doc_details, json.dumps(client_doc_data))
                        logger.info("{invoice_type} invoice docs_replace_all_text response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Table rows fill
                if not args.dry_run_gsuite:
                    try:
                        response = docs_insert_table_rows(SA_SECRETS_FILE, client_doc_details,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["row_num"],
                            json.dumps(client_doc_details_list))
                        logger.info("{invoice_type} invoice docs_insert_table_rows response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")
                
                # First row removal
                if not args.dry_run_gsuite:
                    try:
                        response = docs_delete_table_row(SA_SECRETS_FILE, client_doc_details,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["details"]["row_num"])
                        logger.info("{invoice_type} invoice docs_delete_table_row response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Act

                # Templates
                if not args.dry_run_gsuite and (client_dict["billing"]["papers"]["act"]["email"] or client_dict["billing"]["papers"]["act"]["print"]):
                    try:
                        response = docs_replace_all_text(SA_SECRETS_FILE, client_doc_act, json.dumps(client_doc_data))
                        logger.info("{invoice_type} invoice docs_replace_all_text response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Table rows fill
                if not args.dry_run_gsuite and (client_dict["billing"]["papers"]["act"]["email"] or client_dict["billing"]["papers"]["act"]["print"]):
                    try:
                        response = docs_insert_table_rows(SA_SECRETS_FILE, client_doc_act,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["row_num"],
                            # Act table data is the same as in invoice
                            json.dumps(client_doc_invoice_list))
                        logger.info("{invoice_type} invoice docs_insert_table_rows response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")
                
                # First row removal
                if not args.dry_run_gsuite and (client_dict["billing"]["papers"]["act"]["email"] or client_dict["billing"]["papers"]["act"]["print"]):
                    try:
                        response = docs_delete_table_row(SA_SECRETS_FILE, client_doc_act,
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["table_num"],
                            acc_yaml_dict["merchants"][client_dict["billing"]["merchant"]]["templates"][client_dict["billing"]["template"]][invoice_type.lower()]["act"]["row_num"])
                        logger.info("{invoice_type} invoice docs_delete_table_row response: {response}".format(invoice_type=invoice_type, response=response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Append Invoices

                # Invoices
                if not args.dry_run_gsuite:
                    try:
                        response = sheets_append_data(SA_SECRETS_FILE, acc_yaml_dict["invoices"]["spreadsheet"], acc_yaml_dict["invoices"]["invoices"]["sheet"], acc_yaml_dict["invoices"]["invoices"]["range"], 'ROWS', json.dumps(invoices_rows_invoices))
                        logger.info("Invoices - Invoices sheets_append_data response: {0}".format(response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

                # Employee Share
                if not args.dry_run_gsuite:
                    try:
                        response = sheets_append_data(SA_SECRETS_FILE, acc_yaml_dict["invoices"]["spreadsheet"], acc_yaml_dict["invoices"]["employee_share"]["sheet"], acc_yaml_dict["invoices"]["employee_share"]["range"], 'ROWS', json.dumps(invoices_rows_emplloyee))
                        logger.info("Invoices - Invoices sheets_append_data response: {0}".format(response))
                    except Exception as e:
                        raise Exception("Caught exception on gsuite execution")

            # Commit and close cursor
            if not args.dry_run_db:
                conn.commit()
            
            if args.make_hourly_invoice_for_client is not None or args.make_hourly_invoice_for_all_clients:
                cur.close()
                sub_cur.close()
            
            if args.make_storage_invoice_for_client is not None or args.make_storage_invoice_for_all_clients:
                cur.close()

        if args.list_assets_for_client is not None or args.list_assets_for_all_clients:
            
            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):
                
                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.list_assets_for_client is not None:
                    client, = args.list_assets_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # Check if client is active
                if client_dict["active"]:

                    asset_list = sorted(get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now()), key = lambda x: (x["tariffs"][-1]["activated"]))

                    # Iterate over assets
                    for asset in asset_list:

                        logger.info("Asset: {0}".format(asset["fqdn"]))

                        # ssh for print
                        if "ssh" in asset:
                            ssh_key = ", ssh: "
                            ssh_text = ""
                            if "jump" in asset["ssh"]:
                                ssh_text += "jump: "
                                ssh_text += asset["ssh"]["jump"]["host"]
                                if "port" in asset["ssh"]["jump"]:
                                    ssh_text += ":" + str(asset["ssh"]["jump"]["port"])
                                ssh_text += " "
                            if "host" in asset["ssh"]:
                                ssh_text += asset["ssh"]["host"]
                            if "port" in asset["ssh"]:
                                ssh_text += ":" + str(asset["ssh"]["port"])
                        else:
                            ssh_key = ""
                            ssh_text = ""

                        # tariffs for print
                        tar_text = ""
                        tar_list = []
                        for tar in asset["activated_tariff"]:
                            tar_list.append("{service} {plan} {revision}".format(service=tar["service"], plan=tar["plan"], revision=tar["revision"]))
                        tar_text = ", ".join(tar_list)

                        # Print
                        print("{fqdn}: {{ kind: {kind}, from: {first_activated_date}, loc: {location}{desc_key}{description}{ssh_key}{ssh}, tar: {tariff} }}".format(
                            fqdn=asset["fqdn"],
                            kind=asset["kind"],
                            first_activated_date=asset["tariffs"][-1]["activated"],
                            location=asset["location"],
                            desc_key=", desc: " if "description" in asset else "",
                            description=asset["description"] if "description" in asset else "",
                            ssh_key=ssh_key,
                            ssh=ssh_text,
                            tariff=tar_text
                        ))

        if args.count_assets:
            
            # Save total count for ALL pseudo client per asset kind
            total_asset_count = {}
                
            # For *.yaml in client dir
            for client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):
                
                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                # Check if client is active
                if client_dict["active"]:

                    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, datetime.strptime(args.at_date[0], "%Y-%m-%d") if args.at_date is not None else datetime.now())

                    # Save count for client per asset kind
                    asset_count = {}

                    # Save total asset count to ALL pseudo kind
                    asset_count["ALL"] = 0

                    # Iterate over assets
                    for asset in asset_list:
                        if asset["kind"] not in asset_count:
                            asset_count[asset["kind"]] = 0
                        asset_count[asset["kind"]] += 1
                        asset_count["ALL"] += 1
                    
                    # Add to total
                    for kind in asset_count:
                        if kind not in total_asset_count:
                            total_asset_count[kind] = 0
                        total_asset_count[kind] += asset_count[kind]

                    # Save to DB
                    if not args.dry_run_db:
                        cur = conn.cursor()
                        for kind in asset_count:
                            cur.execute("INSERT INTO asset_count (client, kind, asset_count) VALUES (%s, %s, %s)", (client_dict["name"], kind, asset_count[kind]))
                        cur.close()
                        conn.commit()

            # Calculate ALL pseudo kind
            total_asset_count["ALL"] = 0
            for kind in total_asset_count:
                total_asset_count["ALL"] += total_asset_count[kind]

            # Save total to DB
            if not args.dry_run_db:
                cur = conn.cursor()
                for kind in total_asset_count:
                    cur.execute("INSERT INTO asset_count (client, kind, asset_count) VALUES (%s, %s, %s)", ("ALL", kind, total_asset_count[kind]))
                cur.close()
                conn.commit()

        if args.count_timelog_stats:

            # Connect to GitLab DB
            gitlab_dsn = "host={} dbname={} user={} password={}".format(GL_PG_DB_HOST, GL_PG_DB_NAME, GL_PG_DB_USER, GL_PG_DB_PASS)
            gitlab_conn = psycopg2.connect(gitlab_dsn)

            # Get sum of time_spent from gitlab database
            cur = gitlab_conn.cursor()
            cur.execute("SELECT ROUND(SUM(time_spent)::DEC/60/60, 2) FROM timelogs")
            gitlab_time_spent = cur.fetchone()[0]
            cur.close()

            # Insert sum of time_spent to DB
            if not args.dry_run_db:
                cur = conn.cursor()
                cur.execute("INSERT INTO timelogs_stats (hours_sum) VALUES (%s)", (gitlab_time_spent,))
                cur.close()
                conn.commit()

            # Close GitLab Db connection
            gitlab_conn.close()
            
        # Skip connection close where not needed
        if not (args.yaml_check or args.list_assets_for_client is not None or args.list_assets_for_all_clients):
            # Close connection
            conn.close()

    # Reroute catched exception to log
    except Exception as e:
        logger.exception(e)
        logger.info("Finished {LOGO} with errors".format(LOGO=LOGO))
        sys.exit(1)

    logger.info("Finished {LOGO}".format(LOGO=LOGO))
