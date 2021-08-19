# accounting
DevOps Accounting - Servers, Server Jobs, GitLab Projects, Invoices etc

## Setup
Create private project, e.g. `accounting`.

Add this repo as Git Submodule to a project:
```
git submodule add --name .accounting -b master -- https://github.com/sysadmws/accounting .accounting
```

Add `gitlab-server-job` Submodule:
```
git submodule add --name .gitlab-server-job -b master -- https://github.com/sysadmws/gitlab-server-job .gitlab-server-job
```

Add `salt-project-template` Submodule:
```
git submodule add --name .salt-project-template -b master -- https://github.com/sysadmws/salt-project-template .salt-project-template
```

Make links:
```
ln -s .accounting/Dockerfile
ln -s .accounting/jobs.py
ln -s .accounting/projects.py
ln -s .accounting/accounting.py
ln -s .accounting/services.py
ln -s .accounting/requirements.txt
ln -s .accounting/requirements.txt
ln -s .accounting/requirements.txt
ln -s .accounting/sysadmws_common.py
ln -s .accounting/accounting_db_structure.sql
ln -s .accounting/.gitignore
```

Install python3 requirements:
```
pip3 install -r requirements.txt
```

Add `accounting.yaml` based on `accounting.yaml.example`

Add client yaml based on `clients/example.yaml`.

Copy or symlink `tariffs`.

Add `.gitlab-ci.yaml` based on `.gitlab-ci.yml.example`.

Substitute runner tag placeholders `__dev_runner__` and `__prod_runner__` with real runner tags in `.gitlab-ci.yaml`.

Add those runners to project, both runners should have shell executor with docker command available.

Add following CI-CD vars to project to access GitLab via API.
- `GL_ADMIN_PRIVATE_TOKEN` - Needed for tag cleaning - use admin token with full access
- `GL_USER_PRIVATE_TOKEN` - Pipelines will be run from this user by token, the same as above may be used
- `GL_URL` like `https://gitlab.example.com`

Add `GL_URL` CI-CD var to project to access GitLab via API. Pipelines will be run from this user token.

Make empty `.ssh` for later usage in Dockerfile:
```
mkdir .ssh
touch .ssh/.keep
```

Make empty `.salt-project-private-template/install.sh` (or fill with private data addons):
```
mkdir -p .salt-project-private-template
cat << EOF > .salt-project-private-template/install.sh
#!/bin/bash
true
EOF
chmod +x .salt-project-private-template/install.sh
```

Push project repository to GitLab and make sure pipeline ran, image is built and pushed to registry.

Make `.env` for local tests like:
```
export GL_URL=https://gitlab.example.com
export ACC_WORKDIR=/some/path/accounting
export ACC_LOGDIR=/some/path/accounting/log
export GL_ADMIN_PRIVATE_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxx
export GL_USER_PRIVATE_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxx
```

Make local test dirs:
```
mkdir $ACC_LOGDIR
mkdir ${ACC_WORKDIR}/.jobs
```

Setup client project in GitLab:
```
./projects.py --setup-projects-for-client example
```

Template client project in GitLab:
```
./projects.py --git-push --template-salt-project-for-client example
```

Locally run test.ping pipeline job via `pipeline_salt_cmd.sh`:
```
.gitlab-server-job/pipeline_salt_cmd.sh nowait example/devops/example-salt 60 server1.example.com test.ping
```

Locally run test.ping pipeline job via `jobs.py`:
```
./jobs.py --force-run-job example server1.example.com test_ping
```

Make dirs on prod runner of project:
```
mkdir -p /opt/sysadmws/accounting/.jobs
mkdir -p /opt/sysadmws/accounting/.locks
mkdir -p /opt/sysadmws/accounting/log
```

Add poject CI-CD/Schedules:
- run-jobs
  - Interval Pattern: `*/10 * * * *`
  - Target Branch: master
  - Variables: `RUN_CMD`: `/opt/sysadmws/accounting/jobs.py --debug --run-jobs ALL ALL`
- prune-run-tags
  - Interval Pattern: `30 14 * * *` - some time at day time as jobs mostly run at night time
  - Target Branch: master
  - Variables: `RUN_CMD`: `/opt/sysadmws/accounting/jobs.py --prune-run-tags ALL 30`

Try to run schedules manually. Jobs should run via pipelines by schedule if all good.
