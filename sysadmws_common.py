# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
import datetime
import yaml
import logging
from logging.handlers import RotatingFileHandler
from collections import OrderedDict
import json
import argparse
import glob
from datetime import datetime
from datetime import time
from mergedeep import merge
#import pdb

# Custom Exceptions
class DictError(Exception):
    pass

class LoadError(Exception):
    pass

# Check needed key in dict
def check_key(key, c_dict):
    if not key in c_dict:
        raise DictError("No '{0}' key in dict '{1}'".format(key, c_dict))

# Load JSON
def load_json(f, l):
    l.info("Loading JSON from file {0}".format(f))
    try:
        json_dict = json.load(f, object_pairs_hook=OrderedDict)
    except:
        try:
            json_dict = json.load(f)
        except:
            try:
                file_data = f.read()
                json_dict = json.loads(file_data)
            except:
                raise LoadError("Reading JSON from file '{0}' failed".format(f))
    return json_dict

# Load YAML
def load_yaml(f, l):
    l.info("Loading YAML from file {0}".format(f))
    try:
        with open(f, 'r') as yaml_file:
            yaml_dict = yaml.load(yaml_file, Loader=yaml.SafeLoader)
    except:
        raise LoadError("Reading YAML from file '{0}' failed".format(f))
    return yaml_dict

# Load FILE
def load_file_string(f, l):
    l.info("Loading string from file {0}".format(f))
    try:
        with open(f, 'r') as file_file:
            file_string = file_file.read().replace("\n", "")
    except:
        raise LoadError("Reading string from file '{0}' failed".format(f))
    return file_string

# Set logger
def set_logger(console_level, log_dir, log_file):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir, 0o755)
    log_handler = RotatingFileHandler("{0}/{1}".format(log_dir, log_file), maxBytes=10485760, backupCount=10, encoding="utf-8")
    os.chmod("{0}/{1}".format(log_dir, log_file), 0o600)
    log_handler.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    formatter = logging.Formatter(fmt='%(asctime)s %(filename)s %(name)s %(process)d/%(threadName)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S %Z")
    log_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.addHandler(console_handler)
    return logger

# Helps to find tariff in tariffs list which is activated for event date
def activated_tariff(tariffs, event_date_time, logger):
    event_tariff = None
    for tariff in tariffs:
        tariff_date_time = datetime.combine(tariff["activated"], time.min)
        # Event datetime must be later than tariff datetime
        if event_date_time > tariff_date_time:
            event_tariff = tariff
            break
    if event_tariff is not None:
        logger.info("Found activated tariff {0} for event date time {1}".format(event_tariff, event_date_time))
        return event_tariff
    else:
        raise Exception("Event date time {0} out of available tariffs date time".format(event_date_time))

def get_active_assets(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger):

    assets = {}
    tariffs = {}
    licenses = {}

    asset_list = get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger)

    # Iterate over assets in client
    for asset in asset_list:

        if asset["active"]:

            assets[asset["fqdn"]] = asset
            tariffs[asset["fqdn"]] = []
            licenses[asset["fqdn"]] = []

            # Iterate over tariffs
            for asset_tariff in activated_tariff(asset["tariffs"], datetime.now(), logger)["tariffs"]:

                # If tariff has file key - load it
                if "file" in asset_tariff:

                    tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                    if tariff_dict is None:
                        raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                    # Add tariff to the tariff list for the asset
                    tariffs[asset["fqdn"]].append(tariff_dict)

                    # Add tariff plan licenses to all tariffs lic list if exist
                    if "licenses" in tariff_dict:
                        licenses[asset["fqdn"]].extend(tariff_dict["licenses"])

                # Also take inline plan and service
                else:

                    # Add tariff to the tariff list for the asset
                    tariffs[asset["fqdn"]].append(asset_tariff)

                    # Add tariff plan licenses to all tariffs lic list if exist
                    if "licenses" in asset_tariff:
                        licenses[asset["fqdn"]].extend(asset_tariff["licenses"])

    return assets, tariffs, licenses

# Get asset list
def get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger, only_active=True):

    # Prepare asset list from servers (deprecated) and assets
    asset_list_to_process = []
    if "servers" in client_dict:
        asset_list_to_process.extend(client_dict["servers"])
    if "assets" in client_dict:
        asset_list_to_process.extend(client_dict["assets"])

    # Include salt masters in list
    if client_dict["configuration_management"]["type"] == "salt":
        asset_list_to_process.extend(client_dict["configuration_management"]["salt"]["masters"])

    asset_list = []
    # Set additional or default fields in assets

    for asset in asset_list_to_process:

        # Skip not active assets if only_active
        if only_active:
            if not asset["active"]:
                continue

        # Default kind: server
        if "kind" not in asset:
            asset["kind"] = "server"

        # Set activated tariff
        asset["activated_tariff"] = []
        for asset_tariff in activated_tariff(asset["tariffs"], datetime.now(), logger)["tariffs"]:

            # If tariff has file key - load it
            if "file" in asset_tariff:

                tariff_dict = load_yaml("{0}/{1}/{2}".format(WORK_DIR, TARIFFS_SUBDIR, asset_tariff["file"]), logger)
                if tariff_dict is None:

                    raise Exception("Tariff file error or missing: {0}/{1}".format(WORK_DIR, asset_tariff["file"]))

                asset["activated_tariff"].append(tariff_dict)

            # Also take inline plan and service
            else:

               asset["activated_tariff"].append(asset_tariff)

        asset_list.append(asset)

    return asset_list

# Load asset YAML
def load_client_yaml(WORK_DIR, f, CLIENTS_SUBDIR, YAML_GLOB, logger):
    logger.info("Loading asset YAML from file {0}/{1}".format(WORK_DIR, f))
    try:
        with open("{0}/{1}".format(WORK_DIR, f), 'r') as yaml_file:
            yaml_dict = yaml.load(yaml_file, Loader=yaml.SafeLoader)
    except:
        raise LoadError("Reading YAML from file {0}/{1} failed".format(WORK_DIR, f))
    
    # Asset YAMLs have could have includes
    if "include" in yaml_dict:

        # Include dirs
        if "dirs" in yaml_dict["include"]:

            for dir_name in yaml_dict["include"]["dirs"]:

                # Include dir_name/*.yaml
                for include_file in sorted(glob.glob("{0}/{1}/{2}/{3}".format(WORK_DIR, CLIENTS_SUBDIR, dir_name, YAML_GLOB))):

                    logger.info("Found include file: {0}".format(include_file))

                    # Skip skip_files in found dir
                    should_open = True
                    if "skip_files" in yaml_dict["include"]:
                        for skip_file in yaml_dict["include"]["skip_files"]:
                            if skip_file in include_file:
                                should_open = False
                    if should_open:
                        try:
                            with open(include_file, 'r') as included_yaml_file:
                                included_yaml_dict = yaml.load(included_yaml_file, Loader=yaml.SafeLoader)
                        except:
                            raise LoadError("Reading YAML from file {0} failed".format(include_file))
                    else:
                        continue

                    # Save previous assets or servers (deprecated) list before merging dicts
                    if "servers" in yaml_dict:
                        old_servers = yaml_dict["servers"]
                    else:
                        old_servers = []
                    if "assets" in yaml_dict:
                        old_assets = yaml_dict["assets"]
                    else:
                        old_assets = []

                    # Save new assets or servers (deprecated) list before merging dicts
                    if "servers" in included_yaml_dict:
                        new_servers = included_yaml_dict["servers"]
                    else:
                        new_servers = []
                    if "assets" in included_yaml_dict:
                        new_assets = included_yaml_dict["assets"]
                    else:
                        new_assets = []

                    # Merge dicts, assets will be replaced by included from file
                    merge(yaml_dict, included_yaml_dict)

                    # Set assets or servers from old and new
                    yaml_dict["servers"] = old_servers + new_servers
                    yaml_dict["assets"] = old_assets + new_assets

        # Include files, data in files supersedes data in dirs
        if "files" in yaml_dict["include"]:

                # Include dir_name/*.yaml
                for include_file in yaml_dict["include"]["files"]:

                    try:
                        with open("{0}/{1}".format(CLIENTS_SUBDIR, include_file), 'r') as included_yaml_file:
                            included_yaml_dict = yaml.load(included_yaml_file, Loader=yaml.SafeLoader)
                    except:
                        raise LoadError("Reading YAML from file {0} failed".format(include_file))

                    # Save previous assets or servers (deprecated) list before merging dicts
                    if "servers" in yaml_dict:
                        old_servers = yaml_dict["servers"]
                    else:
                        old_servers = []
                    if "assets" in yaml_dict:
                        old_assets = yaml_dict["assets"]
                    else:
                        old_assets = []

                    # Save new assets or servers (deprecated) list before merging dicts
                    if "servers" in included_yaml_dict:
                        new_servers = included_yaml_dict["servers"]
                    else:
                        new_servers = []
                    if "assets" in included_yaml_dict:
                        new_assets = included_yaml_dict["assets"]
                    else:
                        new_assets = []

                    # Merge dicts, assets will be replaced by included from file
                    merge(yaml_dict, included_yaml_dict)

                    # Set assets or servers from old and new
                    yaml_dict["servers"] = old_servers + new_servers
                    yaml_dict["assets"] = old_assets + new_assets

    return yaml_dict
