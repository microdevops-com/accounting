# accounting
DevOps Accounting - Servers, Server Jobs, GitLab Projects, Invoices etc

## Setup
Create private project, e.g. `accounting`.

Add this repo as Git Submodule to a project:
```
git submodule add --name .accounting -b master -- https://github.com/sysadmws/accounting .accounting
```

Make links:
```
ln -s .accounting/Dockerfile
ln -s .accounting/jobs.py
ln -s .accounting/requirements.txt
ln -s .accounting/sysadmws_common.py
ln -s .accounting/accounting_db_structure.sql
```

Install python3 requirements:
```
pip3 install -r requirements.txt
```

Add client yaml `clients/xxx.yaml` based on `clients/example.yaml`. You can use `free-1.yaml` tariff as a reference.

Add `.gitlab-ci.yaml` based on `.gitlab-ci.yml.example`.

Substitute runner tag placeholders `__dev_runner__` and `__prod_runner__` with real runner tags.

Add runners with docker via shell to project.

Add GL_ADMIN_PRIVATE_TOKEN cd-cd var to project to access GitLab via API.

Add `accounting.yaml` based on `accounting.yaml.example`
