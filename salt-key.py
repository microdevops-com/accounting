#!/usr/bin/python3
# EASY-INSTALL-ENTRY-SCRIPT: 'salt==3001.8','console_scripts','salt-key'
__requires__ = 'salt==3001.8'
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.exit(
        load_entry_point('salt==3001.8', 'console_scripts', 'salt-key')()
    )
