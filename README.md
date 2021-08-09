# accounting
DevOps Accounting - Servers, Server Jobs, GitLab Projects, Invoices etc

## Setup
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
```

Install python3 requirements:
```
pip3 install -r requirements.txt
```
