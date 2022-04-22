#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Import common code
from sysadmws_common import *
import gitlab
import glob
import textwrap
import subprocess
import re
import yaml
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString as pss
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from datetime import datetime
from datetime import time

# Constants and envs

LOGO="Projects"
WORK_DIR = os.environ.get("ACC_WORKDIR", "/opt/sysadmws/accounting")
LOG_DIR = os.environ.get("ACC_LOGDIR", "/opt/sysadmws/accounting/log")
LOG_FILE = "projects.log"
TARIFFS_SUBDIR = "tariffs"
CLIENTS_SUBDIR = "clients"
YAML_GLOB = "*.yaml"
YAML_EXT = "yaml"
ACC_YAML = "accounting.yaml"
PROJECTS_SUBDIR = ".projects"

# Funcs

def open_file(d, f, mode):
    # Check dir
    if not os.path.isdir(os.path.dirname("{0}/{1}".format(d, f))):
        try:
            os.makedirs(os.path.dirname("{0}/{1}".format(d, f)), 0o755)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(os.path.dirname("{0}/{1}".format(d, f))):
                pass
            else:
                raise
    return open("{0}/{1}".format(d, f), mode)

# Main

if __name__ == "__main__":

    # Set parser and parse args
    parser = argparse.ArgumentParser(description='{LOGO} functions.'.format(LOGO=LOGO))
    parser.add_argument("--debug", dest="debug", help="enable debug", action="store_true")
    parser.add_argument("--git-reset", dest="git_reset", help="reset git before applying template", action="store_true")
    parser.add_argument("--git-commit", dest="git_commit", help="commit changes of template apply", action="store_true")
    parser.add_argument("--git-branch", dest="git_branch", help="commit to branch BRANCH instead of master", nargs=1, metavar=("BRANCH"))
    parser.add_argument("--git-push", dest="git_push", help="push after commit", action="store_true")
    parser.add_argument("--dry-run-gitlab", dest="dry_run_gitlab", help="no new objects created in gitlab", action="store_true")
    parser.add_argument("--gitlab-runner-registration-token", dest="gitlab_runner_registration_token", help="set gitlab runner registration token for template if you do not have maintainer rights to get it with code", nargs=1, metavar=("TOKEN"))
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--exclude-clients", dest="exclude_clients", help="exclude clients defined by JSON_LIST from all-clients operations", nargs=1, metavar=("JSON_LIST"))
    group.add_argument("--include-clients", dest="include_clients", help="include only clients defined by JSON_LIST for all-clients operations", nargs=1, metavar=("JSON_LIST"))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup-projects-for-client", dest="setup_projects_for_client", help="ensure -salt, -admin projects created in GitLab, their settings setup for client CLIENT", nargs=1, metavar=("CLIENT"))
    group.add_argument("--setup-projects-for-all-clients", dest="setup_projects_for_all_clients", help="ensure -salt, -admin project created in GitLab, their settings setup for all clients excluding --exclude-clients or only for --include-clients", action="store_true")
    group.add_argument("--clone-project-for-client", dest="clone_project_for_client", help="clones project for client CLIENT using current user git creds", nargs=1, metavar=("CLIENT"))
    group.add_argument("--clone-project-for-all-clients", dest="clone_project_for_all_clients", help="clones project for all clients excluding --exclude-clients or only for --include-clients using current user git creds", action="store_true")
    group.add_argument("--template-salt-project-for-client", dest="template_salt_project_for_client", help="apply templates for salt project for client CLIENT using current user git creds", nargs=1, metavar=("CLIENT"))
    group.add_argument("--template-salt-project-for-all-clients", dest="template_salt_project_for_all_clients", help="apply templates for salt project for all clients excluding --exclude-clients or only for --include-clients using current user git creds", action="store_true")
    group.add_argument("--update-admin-project-wiki-for-client", dest="update_admin_project_wiki_for_client", help="update admin project wiki (asset list, memo etc) for client CLIENT using current user git creds", nargs=1, metavar=("CLIENT"))
    group.add_argument("--update-admin-project-wiki-for-all-clients", dest="update_admin_project_wiki_for_all_clients", help="update admin project wiki (asset list, memo etc) for all clients excluding --exclude-clients or only for --include-clients using current user git creds", action="store_true")

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

        if args.setup_projects_for_client is not None or args.setup_projects_for_all_clients:
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.setup_projects_for_client is not None:
                    client, = args.setup_projects_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # Check client active, inclusions, exclusions
                if (
                        client_dict["active"]
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
            
                    # Salt Project
                    if "salt_project" in client_dict["gitlab"]:
                    
                        # Get GitLab project for client
                        try:
                            logger.info("Checking salt project {project} for client {client}".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"]))
                            project = gl.projects.get(client_dict["gitlab"]["salt_project"]["path"])
                        except gitlab.exceptions.GitlabGetError as e:
                            # Create if not found
                            logger.info("Salt project {project} for client {client} not found, creating".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"]))
                            group_name = client_dict["gitlab"]["salt_project"]["path"].split("/")[0]
                            project_name = client_dict["gitlab"]["salt_project"]["path"].split("/")[1]
                            for gr in gl.groups.list(search=group_name):
                                if gr.full_path == group_name:
                                    group_id = gr.id
                            if not args.dry_run_gitlab:
                                project = gl.projects.create({'name': project_name, 'namespace_id': group_id})
                                # Add first files on creating
                                f = project.files.create(
                                    {
                                        'file_path': 'README.md',
                                        'branch': 'master',
                                        'content': '{client} Salt Masters /srv'.format(client=client_dict["name"]),
                                        'author_email': acc_yaml_dict["gitlab"]["salt_project"]["author_email"],
                                        'author_name': acc_yaml_dict["gitlab"]["salt_project"]["author_name"],
                                        'commit_message': 'Initial commit'
                                    }
                                )

                        # Set needed project params
                        if not args.dry_run_gitlab:
                            project.description = "{client} Salt Masters /srv".format(client=client_dict["name"])
                            project.visibility = "private"
                            project.shared_runners_enabled = False
                            project.only_allow_merge_if_pipeline_succeeds = acc_yaml_dict["gitlab"]["salt_project"]["only_allow_merge_if_pipeline_succeeds"]
                            project.only_allow_merge_if_all_discussions_are_resolved = True
                            project.resolve_outdated_diff_discussions = True
                            project.build_timeout = 86400
                            # Maintainer group
                            if "salt_project" in acc_yaml_dict["gitlab"] and "maintainers_group_id" in acc_yaml_dict["gitlab"]["salt_project"]:
                                if not any(shared_group["group_id"] == acc_yaml_dict["gitlab"]["salt_project"]["maintainers_group_id"] for shared_group in project.shared_with_groups):
                                    project.share(acc_yaml_dict["gitlab"]["salt_project"]["maintainers_group_id"], gitlab.MAINTAINER_ACCESS)
                            # Deploy keys
                            if "deploy_keys" in client_dict["gitlab"]["salt_project"]:
                                for deploy_key in client_dict["gitlab"]["salt_project"]["deploy_keys"]:
                                    key = project.keys.create({'title': deploy_key["title"], 'key': deploy_key["key"]})
                            # Protected tags
                            if any(project_tag.name == 'run_*' for project_tag in project.protectedtags.list(all=True)):
                                p_tag = project.protectedtags.get('run_*')
                                p_tag.delete()
                            project.protectedtags.create({'name': 'run_*', 'create_access_level': str(acc_yaml_dict["gitlab"]["salt_project"]["run_tag_create_access_level"])})
                            # Runner for salt
                            if client_dict["configuration_management"]["type"] == "salt":
                                dev_runner_to_add = client_dict["gitlab"]["salt_project"]["runners"]["dev"] if "runners" in client_dict["gitlab"]["salt_project"] and "dev" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["dev"]
                                for runner in gl.runners.list(all=True):
                                    if runner.description == dev_runner_to_add:
                                        if not any(added_runner.description == runner.description for added_runner in project.runners.list(all=True)):
                                            project.runners.create({'runner_id': runner.id})
                            # Runner for salt-ssh
                            if client_dict["configuration_management"]["type"] == "salt-ssh":
                                dev_runner_to_add = client_dict["gitlab"]["salt_project"]["runners"]["dev"] if "runners" in client_dict["gitlab"]["salt_project"] and "dev" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["dev"]
                                prod_runner_to_add = client_dict["gitlab"]["salt_project"]["runners"]["prod"] if "runners" in client_dict["gitlab"]["salt_project"] and "prod" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["prod"]
                                for runner in gl.runners.list(all=True):
                                    if runner.description == dev_runner_to_add or runner.description == prod_runner_to_add:
                                        if not any(added_runner.description == runner.description for added_runner in project.runners.list(all=True)):
                                            project.runners.create({'runner_id': runner.id})
                            # Variables
                            if "variables" in client_dict["gitlab"]["salt_project"]:
                                for var_key in ["SALTSSH_ROOT_ED25519_PRIV", "SALTSSH_ROOT_ED25519_PUB"]:
                                    if var_key in client_dict["gitlab"]["salt_project"]["variables"]:
                                        if not any(project_var.environment_scope == "*" and project_var.key == var_key for project_var in project.variables.list(all=True)):
                                            script = textwrap.dedent(
                                                """
                                                curl --request POST \
                                                        --header "PRIVATE-TOKEN: {private_token}" \
                                                        "{gitlab_url}/api/v4/projects/{path_with_namespace_encoded}/variables" \
                                                        --form "key={var_key}" \
                                                        --form "value={var_value}" \
                                                        --form "variable_type=env_var" \
                                                        --form "protected=True" \
                                                        --form "masked=False" \
                                                        --form "environment_scope=*"
                                                """
                                            ).format(
                                                gitlab_url=acc_yaml_dict["gitlab"]["url"],
                                                private_token=GL_ADMIN_PRIVATE_TOKEN,
                                                path_with_namespace_encoded=project.path_with_namespace.replace("/", "%2F"),
                                                var_key=var_key,
                                                var_value=client_dict["gitlab"]["salt_project"]["variables"][var_key]
                                            )
                                        else:
                                            script = textwrap.dedent(
                                                """
                                                curl --request PUT \
                                                        --header "PRIVATE-TOKEN: {private_token}" \
                                                        "{gitlab_url}/api/v4/projects/{path_with_namespace_encoded}/variables/{var_key}" \
                                                        --form "value={var_value}" \
                                                        --form "variable_type=env_var" \
                                                        --form "protected=True" \
                                                        --form "masked=False" \
                                                        --form "environment_scope=*" \
                                                        --form "filter[environment_scope]=*"
                                                """
                                            ).format(
                                                gitlab_url=acc_yaml_dict["gitlab"]["url"],
                                                private_token=GL_ADMIN_PRIVATE_TOKEN,
                                                path_with_namespace_encoded=project.path_with_namespace.replace("/", "%2F"),
                                                var_key=var_key,
                                                var_value=client_dict["gitlab"]["salt_project"]["variables"][var_key]
                                            )
                                        logger.info("Running bash script to create or update project variable:")
                                        logger.info(script)
                                        process = subprocess.run(script, shell=True, universal_newlines=True, check=False, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                        if process.returncode:
                                            logger.error("Check stdout:")
                                            logger.error(process.stdout)
                                            logger.error("Check stderr:")
                                            logger.error(process.stderr)
                                            raise SubprocessRunError("Subprocess run failed")
                                        else:
                                            logger.info("Check stdout:")
                                            logger.info(process.stdout)
                                            logger.info("Check stderr:")
                                            logger.info(process.stderr)

                            # Save
                            project.save()

                        logger.info("Salt project {project} for client {client} settings:".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"]))
                        logger.info(project)
                        logger.info("Salt project {project} for client {client} deploy keys:".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"]))
                        logger.info(project.keys.list())
                        logger.info("Salt project {project} for client {client} protected tags:".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"]))
                        logger.info(project.protectedtags.list())
            
                    # Admin Project
                    
                    # Get GitLab project for client
                    try:
                        logger.info("Checking admin project {project} for client {client}".format(project=client_dict["gitlab"]["admin_project"]["path"], client=client_dict["name"]))
                        project = gl.projects.get(client_dict["gitlab"]["admin_project"]["path"])
                    except gitlab.exceptions.GitlabGetError as e:
                        # Create if not found
                        logger.info("Admin project {project} for client {client} not found, creating".format(project=client_dict["gitlab"]["admin_project"]["path"], client=client_dict["name"]))
                        group_name = client_dict["gitlab"]["admin_project"]["path"].split("/")[0]
                        project_name = client_dict["gitlab"]["admin_project"]["path"].split("/")[1]
                        for gr in gl.groups.list(search=group_name):
                            if gr.full_path == group_name:
                                group_id = gr.id
                        if not args.dry_run_gitlab:
                            project = gl.projects.create({'name': project_name, 'namespace_id': group_id})

                    # Set needed project params
                    if not args.dry_run_gitlab:
                        project.description = "{client}".format(client=client_dict["name"])
                        project.visibility = "private"
                        # Save
                        project.save()
                    logger.info("Admin project {project} for client {client} settings:".format(project=client_dict["gitlab"]["admin_project"]["path"], client=client_dict["name"]))
                    logger.info(project)

        if args.clone_project_for_client is not None or args.clone_project_for_all_clients:
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.clone_project_for_client is not None:
                    client, = args.clone_project_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # Check client active, inclusions, exclusions
                if (
                        client_dict["active"]
                        and
                        "salt_project" in client_dict["gitlab"]
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
            
                    # Get GitLab project for client
                    project = gl.projects.get(client_dict["gitlab"]["salt_project"]["path"])
                    logger.info("Salt project {project} for client {client} ssh_url_to_repo: {ssh_url_to_repo}, path_with_namespace: {path_with_namespace}".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"], path_with_namespace=project.path_with_namespace, ssh_url_to_repo=project.ssh_url_to_repo))

                    # Prepare local repo

                    if args.git_reset:
                        git_fetch_text = "git fetch origin --no-tags"
                        git_reset_text = "git reset --hard origin/master"
                        git_clean_text = "git clean -ffdx"
                    else:
                        git_fetch_text = ""
                        git_reset_text = ""
                        git_clean_text = ""

                    script = textwrap.dedent(
                        """
                        if [ -d {PROJECTS_SUBDIR}/{path_with_namespace}/.git ] && ( cd {PROJECTS_SUBDIR}/{path_with_namespace}/.git && git rev-parse --is-inside-git-dir | grep -q -e true ); then
                            echo Already cloned
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                            {fetch}
                            {reset}
                            {clean}
                        else
                            git clone --no-tags {ssh_url_to_repo} {PROJECTS_SUBDIR}/{path_with_namespace}
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                        fi
                        git submodule init
                        git submodule update -f --checkout
                        git submodule foreach "git checkout master && git pull --no-tags"
                        ln -sf ../../.githooks/pre-push .git/hooks/pre-push
                        """
                    ).format(ssh_url_to_repo=project.ssh_url_to_repo, PROJECTS_SUBDIR=PROJECTS_SUBDIR, path_with_namespace=project.path_with_namespace, fetch=git_fetch_text, reset=git_reset_text, clean=git_clean_text)
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

        if args.template_salt_project_for_client is not None or args.template_salt_project_for_all_clients:
            
            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))
                
                # Check specific client
                if args.template_salt_project_for_client is not None:
                    client, = args.template_salt_project_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # Check client active, inclusions, exclusions
                if (
                        client_dict["active"]
                        and
                        "salt_project" in client_dict["gitlab"]
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
            
                    # Get GitLab project for client
                    project = gl.projects.get(client_dict["gitlab"]["salt_project"]["path"])
                    logger.info("Salt project {project} for client {client} ssh_url_to_repo: {ssh_url_to_repo}, path_with_namespace: {path_with_namespace}".format(project=client_dict["gitlab"]["salt_project"]["path"], client=client_dict["name"], path_with_namespace=project.path_with_namespace, ssh_url_to_repo=project.ssh_url_to_repo))

                    # Prepare local repo

                    if args.git_reset:
                        git_fetch_text = "git fetch origin --no-tags"
                        git_reset_text = "git reset --hard origin/master"
                        git_clean_text = "git clean -ffdx"
                    else:
                        git_fetch_text = ""
                        git_reset_text = ""
                        git_clean_text = ""

                    script = textwrap.dedent(
                        """
                        if [ -d {PROJECTS_SUBDIR}/{path_with_namespace}/.git ] && ( cd {PROJECTS_SUBDIR}/{path_with_namespace}/.git && git rev-parse --is-inside-git-dir | grep -q -e true ); then
                            echo Already cloned
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                            {fetch}
                            {reset}
                            {clean}
                        else
                            git clone --no-tags {ssh_url_to_repo} {PROJECTS_SUBDIR}/{path_with_namespace}
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                        fi
                        git submodule init
                        git submodule update -f --checkout
                        git submodule foreach "git checkout master && git pull --no-tags"
                        ln -sf ../../.githooks/pre-push .git/hooks/pre-push
                        """
                    ).format(ssh_url_to_repo=project.ssh_url_to_repo, PROJECTS_SUBDIR=PROJECTS_SUBDIR, path_with_namespace=project.path_with_namespace, fetch=git_fetch_text, reset=git_reset_text, clean=git_clean_text)
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

                    # Init empty template vars
                    template_var_clients = {}
                    template_var_asset_dicts = {}
                    template_var_asset_tariffs = {}
                    template_var_asset_licenses = {}

                    # Check sub_clients before adding
                    if "sub_clients" in client_dict["configuration_management"]:

                        # For *.yaml in client dir
                        for template_var_client_file in sorted(glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB))):

                            # Load client YAML
                            template_var_client_dict = load_client_yaml(WORK_DIR, template_var_client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                            if template_var_client_dict is None:
                                raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, template_var_client_file))

                            # Add if sub_clients match, add parent client to sub_client as well
                            if (type(client_dict["configuration_management"]["sub_clients"]) == str and client_dict["configuration_management"]["sub_clients"] == "ALL") or template_var_client_dict["name"] in client_dict["configuration_management"]["sub_clients"] or template_var_client_dict["name"] == client_dict["name"]:
                                template_var_clients[template_var_client_dict["name"]] = template_var_client_dict

                                template_var_asset_dicts[template_var_client_dict["name"]], \
                                    template_var_asset_tariffs[template_var_client_dict["name"]], \
                                    template_var_asset_licenses[template_var_client_dict["name"]] = get_active_assets(template_var_client_dict, WORK_DIR, TARIFFS_SUBDIR, logger)

                                logger.info("Added client to template: {0}".format(template_var_client_file))

                    # File Templates
                    if "templates" in client_dict["configuration_management"] and "files" in client_dict["configuration_management"]["templates"]:

                        for templated_file in client_dict["configuration_management"]["templates"]["files"]:

                            # Jinja templates
                            if "jinja" in templated_file:

                                logger.info("Rendering jinja template: {0}".format(templated_file["jinja"]))
                                j2_env = Environment(loader=FileSystemLoader(PROJECTS_SUBDIR + "/" + project.path_with_namespace), trim_blocks=True)
                                j2_env.add_extension('jinja2.ext.do')
                                template = j2_env.get_template(templated_file["jinja"])
                                rendered_template = template.render(
                                    clients = template_var_clients,
                                    asset_dicts = template_var_asset_dicts,
                                    asset_tariffs = template_var_asset_tariffs,
                                    asset_licenses = template_var_asset_licenses
                                )

                                logger.info("Rendered template: {0}".format(rendered_template))

                                logger.info("Saving jinja template to file: {0}".format(templated_file["path"]))
                                with open_file(PROJECTS_SUBDIR + "/" + project.path_with_namespace, templated_file["path"], "w") as templated_file_handler:
                                    templated_file_handler.write(rendered_template)

                            # Copy files from other projects
                            if "sub_client_project_file" in templated_file:

                                # Get Gitlab project
                                sub_client_project = gl.projects.get(template_var_clients[templated_file["sub_client_project_file"]["sub_client"]]["gitlab"]["salt_project"]["path"])
                                logger.info("Sub client salt project {project} for client {client} loaded".format(project=template_var_clients[templated_file["sub_client_project_file"]["sub_client"]]["gitlab"]["salt_project"]["path"], client=templated_file["sub_client_project_file"]["sub_client"]))

                                # Get File from project and save it
                                with open_file(PROJECTS_SUBDIR + "/" + project.path_with_namespace, templated_file["path"], "wb") as templated_file_handler:
                                    sub_client_project.files.raw(file_path=templated_file["sub_client_project_file"]["path"], ref="master", streamed=True, action=templated_file_handler.write)

                    # Defaults

                    if "templates" in client_dict["configuration_management"] and "ufw_type" in client_dict["configuration_management"]["templates"]:
                        ufw_type = client_dict["configuration_management"]["templates"]["ufw_type"]
                    else:
                        ufw_type = acc_yaml_dict["defaults"]["ufw_type"]

                    if "templates" in client_dict["configuration_management"] and "monitoring_disabled" in client_dict["configuration_management"]["templates"]:
                        monitoring_enabled = not client_dict["configuration_management"]["templates"]["monitoring_disabled"]
                    else:
                        monitoring_enabled = True

                    # Salt-SSH
                    if client_dict["configuration_management"]["type"] == "salt-ssh":

                        # Install templates
                        script = textwrap.dedent(
                            """
                            set -e
                            cd .salt-project-template
                            TELEGRAM_TOKEN={TELEGRAM_TOKEN} \
                                    TELEGRAM_CHAT_ID={TELEGRAM_CHAT_ID} \
                                    MONITORING_ENABLED={MONITORING_ENABLED} \
                                    ALERTA_URL={ALERTA_URL} \
                                    ALERTA_API_KEY={ALERTA_API_KEY} \
                                    HB_RECEIVER_HN={HB_RECEIVER_HN} \
                                    HB_TOKEN={HB_TOKEN} \
                                    ROOT_EMAIL={ROOT_EMAIL} \
                                    CLIENT={CLIENT} \
                                    CLIENT_FULL={CLIENT_FULL} \
                                    VENDOR={VENDOR} \
                                    VENDOR_FULL={VENDOR_FULL} \
                                    DEFAULT_TZ={DEFAULT_TZ} \
                                    CLIENT_DOMAIN={CLIENT_DOMAIN} \
                                    DEV_RUNNER={DEV_RUNNER} \
                                    PROD_RUNNER={PROD_RUNNER} \
                                    SALTSSH_ROOT_ED25519_PUB="{SALTSSH_ROOT_ED25519_PUB}" \
                                    SALTSSH_RUNNER_SOURCE_IP={SALTSSH_RUNNER_SOURCE_IP} \
                                    SALT_VERSION={SALT_VERSION} \
                                    UFW={UFW} \
                                    ./install.sh ../{PROJECTS_SUBDIR}/{path_with_namespace} salt-ssh
                            
                            cd ../.salt-project-private-template
                            CLIENT={CLIENT} \
                                    ./install.sh ../{PROJECTS_SUBDIR}/{path_with_namespace}
                            """
                        ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR,
                            path_with_namespace=project.path_with_namespace,
                            ROOT_EMAIL=client_dict["configuration_management"]["templates"]["root_email"],
                            TELEGRAM_TOKEN=client_dict["configuration_management"]["templates"]["telegram_token"],
                            TELEGRAM_CHAT_ID=client_dict["configuration_management"]["templates"]["telegram_chat_id"],
                            MONITORING_ENABLED=monitoring_enabled,
                            ALERTA_URL=client_dict["configuration_management"]["templates"]["alerta_url"],
                            ALERTA_API_KEY=client_dict["configuration_management"]["templates"]["alerta_api_key"],
                            HB_RECEIVER_HN=client_dict["configuration_management"]["templates"]["heartbeat_mesh"]["sender"]["receiver"],
                            HB_TOKEN=client_dict["configuration_management"]["templates"]["heartbeat_mesh"]["sender"]["token"],
                            CLIENT=client_dict["name"].lower(),
                            CLIENT_FULL=client_dict["name"],
                            VENDOR=client_dict["vendor"].lower(),
                            VENDOR_FULL=client_dict["vendor"],
                            DEFAULT_TZ=client_dict["configuration_management"]["templates"]["default_tz"],
                            CLIENT_DOMAIN=client_dict["configuration_management"]["templates"]["client_domain"],
                            DEV_RUNNER=client_dict["gitlab"]["salt_project"]["runners"]["dev"] if "runners" in client_dict["gitlab"]["salt_project"] and "dev" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["dev"],
                            PROD_RUNNER=client_dict["gitlab"]["salt_project"]["runners"]["prod"] if "runners" in client_dict["gitlab"]["salt_project"] and "prod" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["prod"],
                            SALTSSH_ROOT_ED25519_PUB=client_dict["gitlab"]["salt_project"]["variables"]["SALTSSH_ROOT_ED25519_PUB"],
                            SALTSSH_RUNNER_SOURCE_IP=client_dict["configuration_management"]["templates"]["runner_source_ip"],
                            SALT_VERSION=client_dict["configuration_management"]["salt-ssh"]["version"],
                            UFW=ufw_type
                        )
                        logger.info("Running bash script:")
                        logger.info(script)
                        subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

                    # Salt
                    if client_dict["configuration_management"]["type"] == "salt":

                        # Prepare client data
                        salt_master_names = []
                        salt_master_ips = []
                        salt_master_ext_ips = []
                        salt_master_ssh_pub = []
                        salt_master_ext_ports = []
                        for salt_master in client_dict["configuration_management"]["salt"]["masters"]:
                            salt_master_names.append(salt_master["fqdn"])
                            salt_master_ips.append(salt_master["ip"])
                            salt_master_ext_ips.append(salt_master["external_ip"])
                            if "root_ed25519" in salt_master:
                                salt_master_ssh_pub.append(salt_master["root_ed25519"]["pub"].rstrip())
                            if "root_rsa" in salt_master:
                                salt_master_ssh_pub.append(salt_master["root_rsa"]["pub"].rstrip())
                            # For now salt minion cannot manage both ports per salt master, so take ports from the salt1
                            salt_master_ext_ports.append(salt_master["external_ports"])

                        # Install templates
                        script = textwrap.dedent(
                            """
                            set -e
                            cd .salt-project-template
                            TELEGRAM_TOKEN={TELEGRAM_TOKEN} \
                                    TELEGRAM_CHAT_ID={TELEGRAM_CHAT_ID} \
                                    MONITORING_ENABLED={MONITORING_ENABLED} \
                                    ALERTA_URL={ALERTA_URL} \
                                    ALERTA_API_KEY={ALERTA_API_KEY} \
                                    HB_RECEIVER_HN={HB_RECEIVER_HN} \
                                    HB_TOKEN={HB_TOKEN} \
                                    ROOT_EMAIL={ROOT_EMAIL} \
                                    CLIENT={CLIENT} \
                                    CLIENT_FULL={CLIENT_FULL} \
                                    VENDOR={VENDOR} \
                                    VENDOR_FULL={VENDOR_FULL} \
                                    DEFAULT_TZ={DEFAULT_TZ} \
                                    CLIENT_DOMAIN={CLIENT_DOMAIN} \
                                    DEV_RUNNER={DEV_RUNNER} \
                                    SALT_MINION_VERSION={SALT_MINION_VERSION} \
                                    SALT_MASTER_VERSION={SALT_MASTER_VERSION} \
                                    SALT_VERSION={SALT_VERSION} \
                                    SALT_MASTER_1_NAME={SALT_MASTER_1_NAME} \
                                    SALT_MASTER_1_IP={SALT_MASTER_1_IP} \
                                    SALT_MASTER_1_EXT_IP={SALT_MASTER_1_EXT_IP} \
                                    SALT_MASTER_1_SSH_PUB="{SALT_MASTER_1_SSH_PUB}" \
                                    SALT_MASTER_2_NAME={SALT_MASTER_2_NAME} \
                                    SALT_MASTER_2_IP={SALT_MASTER_2_IP} \
                                    SALT_MASTER_2_EXT_IP={SALT_MASTER_2_EXT_IP} \
                                    SALT_MASTER_2_SSH_PUB="{SALT_MASTER_2_SSH_PUB}" \
                                    SALT_MASTER_PORT_1={SALT_MASTER_PORT_1} \
                                    SALT_MASTER_PORT_2={SALT_MASTER_PORT_2} \
                                    UFW={UFW} \
                                    ./install.sh ../{PROJECTS_SUBDIR}/{path_with_namespace} salt
                            
                            cd ../.salt-project-private-template
                            CLIENT={CLIENT} \
                                    ./install.sh ../{PROJECTS_SUBDIR}/{path_with_namespace}
                            """
                        ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR,
                            path_with_namespace=project.path_with_namespace,
                            SALT_MASTER_1_NAME=salt_master_names[0],
                            SALT_MASTER_2_NAME=salt_master_names[1],
                            SALT_MASTER_1_IP=salt_master_ips[0],
                            SALT_MASTER_2_IP=salt_master_ips[1],
                            SALT_MASTER_1_EXT_IP=salt_master_ext_ips[0],
                            SALT_MASTER_2_EXT_IP=salt_master_ext_ips[1],
                            SALT_MASTER_1_SSH_PUB=salt_master_ssh_pub[0],
                            SALT_MASTER_2_SSH_PUB=salt_master_ssh_pub[1],
                            SALT_MASTER_PORT_1=salt_master_ext_ports[0][0],
                            SALT_MASTER_PORT_2=salt_master_ext_ports[0][1],
                            SALT_MINION_VERSION=client_dict["configuration_management"]["salt"]["version"],
                            SALT_MASTER_VERSION=client_dict["configuration_management"]["salt"]["version"],
                            SALT_VERSION=client_dict["configuration_management"]["salt"]["version"],
                            ROOT_EMAIL=client_dict["configuration_management"]["templates"]["root_email"],
                            TELEGRAM_TOKEN=client_dict["configuration_management"]["templates"]["telegram_token"],
                            TELEGRAM_CHAT_ID=client_dict["configuration_management"]["templates"]["telegram_chat_id"],
                            MONITORING_ENABLED=monitoring_enabled,
                            ALERTA_URL=client_dict["configuration_management"]["templates"]["alerta_url"],
                            ALERTA_API_KEY=client_dict["configuration_management"]["templates"]["alerta_api_key"],
                            HB_RECEIVER_HN=client_dict["configuration_management"]["templates"]["heartbeat_mesh"]["sender"]["receiver"],
                            HB_TOKEN=client_dict["configuration_management"]["templates"]["heartbeat_mesh"]["sender"]["token"],
                            CLIENT=client_dict["name"].lower(),
                            CLIENT_FULL=client_dict["name"],
                            VENDOR=client_dict["vendor"].lower(),
                            VENDOR_FULL=client_dict["vendor"],
                            DEFAULT_TZ=client_dict["configuration_management"]["templates"]["default_tz"],
                            CLIENT_DOMAIN=client_dict["configuration_management"]["templates"]["client_domain"],
                            DEV_RUNNER=client_dict["gitlab"]["salt_project"]["runners"]["dev"] if "runners" in client_dict["gitlab"]["salt_project"] and "dev" in client_dict["gitlab"]["salt_project"]["runners"] else acc_yaml_dict["gitlab"]["salt_project"]["runners"]["dev"],
                            UFW=ufw_type
                        )
                        logger.info("Running bash script:")
                        logger.info(script)
                        subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")
                    
                    # Prepare the roster file
                    if "skip_roster" in client_dict["configuration_management"] and client_dict["configuration_management"]["skip_roster"]:
                        logger.info("Skipping roster update")

                    else:

                        # It is needed for both salt and salt-ssh types
                        client_asset_list = ""

                        for asset in sorted(get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger), key = lambda x: (x["tariffs"][-1]["activated"], x["fqdn"])):

                            # Add only servers to roster
                            if asset["kind"] == "server":
                                client_asset_list += textwrap.dedent(
                                    """
                                    echo "{fqdn}:" >> etc/salt/roster
                                    echo "  host: {host}" >> etc/salt/roster
                                    echo "  port: {port}" >> etc/salt/roster
                                    echo "  priv: __ROSTER_PRIV__" >> etc/salt/roster
                                    """
                                ).format(fqdn=asset["fqdn"],
                                    host=asset["ssh"]["host"] if ("ssh" in asset and "host" in asset["ssh"]) else asset["fqdn"],
                                    port=asset["ssh"]["port"] if ("ssh" in asset and "port" in asset["ssh"]) else "22"
                                )
                                if "ssh" in asset and "jump" in asset["ssh"]:
                                    if "port" in asset["ssh"]["jump"]:
                                        client_asset_list += textwrap.dedent(
                                            """
                                            echo "  ssh_options:" >> etc/salt/roster
                                            echo "    - ProxyJump={jump_host}:{jump_port}" >> etc/salt/roster
                                            """
                                        ).format(jump_host=asset["ssh"]["jump"]["host"],
                                            jump_port=asset["ssh"]["jump"]["port"]
                                        )
                                    else:
                                        client_asset_list += textwrap.dedent(
                                            """
                                            echo "  ssh_options:" >> etc/salt/roster
                                            echo "    - ProxyJump={jump_host}" >> etc/salt/roster
                                            """
                                        ).format(jump_host=asset["ssh"]["jump"]["host"])
                                if "roster_opts" in asset:
                                    for optname, optval in asset["roster_opts"].items():
                                        client_asset_list += textwrap.dedent(
                                            """
                                            echo "  {optname}: {optval}" >> etc/salt/roster
                                            """
                                        ).format(optname=optname, optval=optval)

                        script = textwrap.dedent(
                            """
                            set -e
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                            > etc/salt/roster
                            {client_asset_list}
                            """
                        ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR,
                            path_with_namespace=project.path_with_namespace,
                            client_asset_list=client_asset_list
                        )
                        logger.info("Running bash script:")
                        logger.info(script)
                        subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

                    # Template salt.master_pki and salt.minion_pki pillar
                    
                    if client_dict["configuration_management"]["type"] == "salt":
                        
                        pillar_dirname = PROJECTS_SUBDIR + "/" + project.path_with_namespace + "/pillar/salt"
                        
                        for asset in get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger):

                            if asset["active"] and asset["kind"] == "server" and "minion" in asset:
                                
                                # Check validity of pem/pub vars (start keyword, end keyword, at least 3 newlines)
                                if not re.match(r'^-----BEGIN.*KEY-----$', asset["minion"]["pem"], re.MULTILINE) or asset["minion"]["pem"].count("\n") < 3:
                                    raise Exception("Minion {minion} pem \n{pem} doesn't match needed regexp or at least 3 newlines".format(minion=asset["fqdn"], pem=asset["minion"]["pem"]))
                                if not re.match(r'^-----BEGIN.*KEY-----$', asset["minion"]["pub"], re.MULTILINE) or asset["minion"]["pub"].count("\n") < 3:
                                    raise Exception("Minion {minion} pub \n{pub} doesn't match needed regexp or at least 3 newlines".format(minion=asset["fqdn"], pub=asset["minion"]["pub"]))
                            
                                # Minion keys
                                pillar_minion_dict = {
                                    "salt": {
                                        "minion": {
                                            "pki": {
                                                "minion": {
                                                    "pem": pss(asset["minion"]["pem"]),
                                                    "pub": pss(asset["minion"]["pub"])
                                                },
                                                "master_sign": pss(client_dict["configuration_management"]["salt"]["pki"]["master_sign"]["pub"]) # Pub of Master Signature on Minion
                                            }
                                        }
                                    }
                                }
                                
                                # Minion pillar
                                pillar_filename = "minion_" + asset["fqdn"].replace(".", "_") + ".sls"
                                with open_file(pillar_dirname, pillar_filename, "w") as pillar_file:
                                    pillar_yaml = YAML()
                                    pillar_yaml.dump(pillar_minion_dict, pillar_file)
                                    logger.info("Pillar written to the file: {dir}/{file_name}".format(dir=pillar_dirname, file_name=pillar_filename))

                        for salt_master in client_dict["configuration_management"]["salt"]["masters"]:

                            # Minion keys
                            pillar_minion_dict = {
                                "salt": {
                                    "minion": {
                                        "pki": {
                                            "minion": {
                                                "pem": pss(salt_master["pki"]["minion"]["pem"]),
                                                "pub": pss(salt_master["pki"]["minion"]["pub"])
                                            },
                                            "minion_master": pss(salt_master["pki"]["master"]["pub"]), # Accepted Master on Minion
                                            "master_sign": pss(client_dict["configuration_management"]["salt"]["pki"]["master_sign"]["pub"]) # Pub of Master Signature on Minion
                                        }
                                    }
                                }
                            }

                            # Master keys
                            pillar_master_dict = {
                                "salt": {
                                    "master": {
                                        "pki": {
                                            "master_sign": { # Master Signature (should be the same on all masters)
                                                "pem": pss(client_dict["configuration_management"]["salt"]["pki"]["master_sign"]["pem"]),
                                                "pub": pss(client_dict["configuration_management"]["salt"]["pki"]["master_sign"]["pub"])
                                            },
                                            "master": {
                                                "pem": pss(salt_master["pki"]["master"]["pem"]),
                                                "pub": pss(salt_master["pki"]["master"]["pub"])
                                            },
                                            "minions": {}
                                        }
                                    }
                                }
                            }

                            # Accepted Minion on Master
                            pillar_master_dict["salt"]["master"]["pki"]["minions"][salt_master["fqdn"]] = pss(salt_master["pki"]["minion"]["pub"])

                            # Other active assets accepted Minions on Master
                            for asset in get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger):
                                if asset["active"] and asset["kind"] == "server" and "minion" in asset:
                                    pillar_master_dict["salt"]["master"]["pki"]["minions"][asset["fqdn"]] = pss(asset["minion"]["pub"])

                            # Root Keys for deploy
                            if "root_ed25519" in salt_master:
                                pillar_master_dict["salt"]["master"]["root_ed25519"] = {}
                                pillar_master_dict["salt"]["master"]["root_ed25519"]["priv"] = pss(salt_master["root_ed25519"]["priv"])
                                pillar_master_dict["salt"]["master"]["root_ed25519"]["pub"] = pss(salt_master["root_ed25519"]["pub"])
                            if "root_rsa" in salt_master:
                                pillar_master_dict["salt"]["master"]["root_rsa"] = {}
                                pillar_master_dict["salt"]["master"]["root_rsa"]["priv"] = pss(salt_master["root_rsa"]["priv"])
                                pillar_master_dict["salt"]["master"]["root_rsa"]["pub"] = pss(salt_master["root_rsa"]["pub"])

                            # SSH Url to repo
                            pillar_master_dict["salt"]["master"]["repo"] = project.ssh_url_to_repo

                            # Gitlab-runner
                            pillar_master_dict["salt"]["master"]["gitlab-runner"] = {}
                            pillar_master_dict["salt"]["master"]["gitlab-runner"]["gitlab_url"] = acc_yaml_dict["gitlab"]["url"]
                            pillar_master_dict["salt"]["master"]["gitlab-runner"]["gitlab_runner_name"] = salt_master["fqdn"]

                            # Gitlab-runner registration token
                            # You have to have project maintainer rights to get token with code

                            # Token from args has highest priority, then from client yaml, then from gitlab api
                            if args.gitlab_runner_registration_token is not None:
                                args_registration_token, = args.gitlab_runner_registration_token
                                pillar_master_dict["salt"]["master"]["gitlab-runner"]["registration_token"] = args_registration_token
                            elif "gitlab" in client_dict and "salt_project" in client_dict["gitlab"] and "gitlab-runner" in client_dict["gitlab"]["salt_project"] and "registration_token" in client_dict["gitlab"]["salt_project"]["gitlab-runner"]:
                                pillar_master_dict["salt"]["master"]["gitlab-runner"]["registration_token"] = client_dict["gitlab"]["salt_project"]["gitlab-runner"]["registration_token"]
                            else:
                                pillar_master_dict["salt"]["master"]["gitlab-runner"]["registration_token"] = project.runners_token

                            # Master pillar
                            pillar_filename = "master_" + salt_master["fqdn"].replace(".", "_") + ".sls"
                            with open_file(pillar_dirname, pillar_filename, "w") as pillar_file:
                                pillar_yaml = YAML()
                                pillar_yaml.dump(pillar_master_dict, pillar_file)
                                logger.info("Pillar written to the file: {dir}/{file_name}".format(dir=pillar_dirname, file_name=pillar_filename))

                            # Minion pillar
                            pillar_filename = "minion_" + salt_master["fqdn"].replace(".", "_") + ".sls"
                            with open_file(pillar_dirname, pillar_filename, "w") as pillar_file:
                                pillar_yaml = YAML()
                                pillar_yaml.dump(pillar_minion_dict, pillar_file)
                                logger.info("Pillar written to the file: {dir}/{file_name}".format(dir=pillar_dirname, file_name=pillar_filename))

                    # Commit changes

                    if args.git_branch is not None:
                        git_branch_branch, = args.git_branch
                        git_branch_text = "git checkout -b {git_branch_branch}".format(git_branch_branch=git_branch_branch)
                    else:
                        git_branch_text = ""

                    if args.git_commit:
                        git_add_text = "git add -A"
                        git_commit_text = "git commit -m '.salt-project-template, .salt-project-private-template installed' || true"
                    else:
                        git_add_text = ""
                        git_commit_text = ""

                    if args.git_push:
                        if args.git_branch is not None:
                            git_branch_branch, = args.git_branch
                            git_push_text = "git push --set-upstream origin {git_branch_branch}".format(git_branch_branch=git_branch_branch)
                        else:
                            git_push_text = "git push"
                    else:
                        git_push_text = ""

                    script = textwrap.dedent(
                        """
                        set -e
                        cd {PROJECTS_SUBDIR}/{path_with_namespace}
                        {branch}
                        {add}
                        {commit}
                        {push}
                        """
                    ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR, path_with_namespace=project.path_with_namespace, branch=git_branch_text, push=git_push_text, add=git_add_text, commit=git_commit_text)
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

        if args.update_admin_project_wiki_for_client is not None or args.update_admin_project_wiki_for_all_clients:

            # Connect to GitLab
            gl = gitlab.Gitlab(acc_yaml_dict["gitlab"]["url"], private_token=GL_ADMIN_PRIVATE_TOKEN)
            gl.auth()

            # For *.yaml in client dir
            for client_file in glob.glob("{0}/{1}".format(CLIENTS_SUBDIR, YAML_GLOB)):

                logger.info("Found client file: {0}".format(client_file))

                # Load client YAML
                client_dict = load_client_yaml(WORK_DIR, client_file, CLIENTS_SUBDIR, YAML_GLOB, logger)
                if client_dict is None:
                    raise Exception("Config file error or missing: {0}/{1}".format(WORK_DIR, client_file))

                # Check specific client
                if args.update_admin_project_wiki_for_client is not None:
                    client, = args.update_admin_project_wiki_for_client
                    if client_dict["name"].lower() != client:
                        continue

                # Check client active, inclusions, exclusions
                if (
                        client_dict["active"]
                        and
                        "admin_project" in client_dict["gitlab"]
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
            
                    # Get GitLab project for client
                    project = gl.projects.get(client_dict["gitlab"]["admin_project"]["path"])
                    ssh_url_to_repo = re.sub(r'\.git$', '.wiki.git', project.ssh_url_to_repo)
                    path_with_namespace = project.path_with_namespace + ".wiki"
                    logger.info("Admin project {project} wiki for client {client} ssh_url_to_repo: {ssh_url_to_repo}, path_with_namespace: {path_with_namespace}".format(project=client_dict["gitlab"]["admin_project"]["path"], client=client_dict["name"], path_with_namespace=path_with_namespace, ssh_url_to_repo=ssh_url_to_repo))

                    # Prepare local repo

                    if args.git_reset:
                        git_fetch_text = "git fetch origin --no-tags"
                        git_reset_text = "git reset --hard origin/master"
                        git_clean_text = "git clean -ffdx"
                    else:
                        git_fetch_text = ""
                        git_reset_text = ""
                        git_clean_text = ""

                    script = textwrap.dedent(
                        """
                        if [ -d {PROJECTS_SUBDIR}/{path_with_namespace}/.git ] && ( cd {PROJECTS_SUBDIR}/{path_with_namespace}/.git && git rev-parse --is-inside-git-dir | grep -q -e true ); then
                            echo Already cloned
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                            {fetch}
                            {reset}
                            {clean}
                        else
                            git clone --no-tags {ssh_url_to_repo} {PROJECTS_SUBDIR}/{path_with_namespace}
                            cd {PROJECTS_SUBDIR}/{path_with_namespace}
                        fi
                        git submodule init
                        git submodule update -f --checkout
                        git submodule foreach "git checkout master && git pull --no-tags"
                        """
                    ).format(ssh_url_to_repo=ssh_url_to_repo, PROJECTS_SUBDIR=PROJECTS_SUBDIR, path_with_namespace=path_with_namespace, fetch=git_fetch_text, reset=git_reset_text, clean=git_clean_text)
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

                    asset_list_text = ""
                    asset_list = sorted(get_asset_list(client_dict, WORK_DIR, TARIFFS_SUBDIR, logger), key = lambda x: (x["tariffs"][-1]["activated"]))

                    # Iterate over assets
                    for asset in asset_list:

                        logger.info("Asset: {0}".format(asset["fqdn"]))

                        # ssh for print
                        if "ssh" in asset:
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
                            ssh_text = ""

                        # tariffs for print
                        tar_text = ""
                        tar_list = []
                        for tar in asset["activated_tariff"]:
                            tar_list.append("{service} {plan} {revision}".format(service=tar["service"], plan=tar["plan"], revision=tar["revision"]))
                        tar_text = ", ".join(tar_list)

                        # Print
                        asset_list_text += textwrap.dedent(
                            """echo  "| {fqdn} | {kind} | {first_activated_date} | {location} | {description} | {ssh} | {tariff} |" >> Assets.md
                            """
                            ).format(
                                fqdn=asset["fqdn"],
                                kind=asset["kind"],
                                first_activated_date=asset["tariffs"][-1]["activated"],
                                location=asset["location"],
                                description=asset["description"] if "description" in asset else "",
                                ssh=ssh_text,
                                tariff=tar_text
                            )

                    # Update info
                    script = textwrap.dedent(
                        """
                        set -e
                        mkdir -p {PROJECTS_SUBDIR}/{path_with_namespace}/Accounting
                        cd {PROJECTS_SUBDIR}/{path_with_namespace}/Accounting

                        echo  "| Key | Value |" > Requisites.md
                        echo  "| --- | ----- |" >> Requisites.md
                        echo  "| Client Name | {client_name} |" >> Requisites.md
                        echo  "| Client Code | {client_code} |" >> Requisites.md
                        echo  "| Start Date | {client_start_date} |" >> Requisites.md
                        echo  "| Contract Recipient | {client_contract_recipient} |" >> Requisites.md
                        echo  "| Contract Requisites | {client_contract_requisites} |" >> Requisites.md
                        echo  "| Contract Name | {client_contract_name} |" >> Requisites.md
                        echo  "| Contract Person Name | {client_contract_person_name} |" >> Requisites.md
                        echo  "| Contract Person Sign | {client_contract_person_sign} |" >> Requisites.md
                        echo  "| Papers Envelope Address | {client_papers_envelope_address} |" >> Requisites.md
                        echo  "| Papers Email | {client_papers_email} |" >> Requisites.md

                        echo  "| FQDN | Kind | First Activated Date | Location | Description | SSH | Tariff |" > Assets.md
                        echo  "| ---- | ---- | -------------------- | -------- | ----------- | --- | ------ |" >> Assets.md
                        {asset_list_text}
                        """
                    ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR,
                        path_with_namespace=path_with_namespace,
                        client_name=client_dict["name"],
                        client_code=client_dict["billing"]["code"],
                        client_start_date=client_dict["start_date"],
                        client_contract_recipient=client_dict["billing"]["contract"]["recipient"],
                        client_contract_requisites=client_dict["billing"]["contract"]["details"].replace("\n", "<br>"),
                        client_contract_name=client_dict["billing"]["contract"]["name"],
                        client_contract_person_name=client_dict["billing"]["contract"]["person"],
                        client_contract_person_sign=client_dict["billing"]["contract"]["sign"],
                        client_papers_envelope_address=client_dict["billing"]["papers"]["envelope_address"].replace("\n", "<br>") if "envelope_address" in client_dict["billing"]["papers"] else "",
                        client_papers_email=client_dict["billing"]["papers"]["email"]["to"],
                        asset_list_text=asset_list_text
                    )
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

                    # Commit changes

                    if args.git_branch is not None:
                        git_branch_branch, = args.git_branch
                        git_branch_text = "git checkout -b {git_branch_branch}".format(git_branch_branch=git_branch_branch)
                    else:
                        git_branch_text = ""

                    if args.git_commit:
                        git_add_text = "git add -A"
                        git_commit_text = "git commit -m 'update by accounting' || true"
                    else:
                        git_add_text = ""
                        git_commit_text = ""

                    if args.git_push:
                        if args.git_branch is not None:
                            git_branch_branch, = args.git_branch
                            git_push_text = "git push --set-upstream origin {git_branch_branch}".format(git_branch_branch=git_branch_branch)
                        else:
                            git_push_text = "git push"
                    else:
                        git_push_text = ""

                    script = textwrap.dedent(
                        """
                        set -e
                        cd {PROJECTS_SUBDIR}/{path_with_namespace}
                        {branch}
                        {add}
                        {commit}
                        {push}
                        """
                    ).format(PROJECTS_SUBDIR=PROJECTS_SUBDIR, path_with_namespace=path_with_namespace, branch=git_branch_text, push=git_push_text, add=git_add_text, commit=git_commit_text)
                    logger.info("Running bash script:")
                    logger.info(script)
                    subprocess.run(script, shell=True, universal_newlines=True, check=True, executable="/bin/bash")

    # Reroute catched exception to log
    except Exception as e:
        logger.exception(e)
        logger.info("Finished {LOGO} with errors".format(LOGO=LOGO))
        sys.exit(1)

    logger.info("Finished {LOGO}".format(LOGO=LOGO))
