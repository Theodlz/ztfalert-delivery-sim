# given a YYYYMMDD date, fetch ZTF alerts and decompress them to a given directory
# format is: https://ztf.uw.edu/alerts/public/ztf_public_20240715.tar.gz

import os
import urllib
import tempfile
import subprocess
import time
from pathlib import Path

import requests # necessary for urllib.request.urlretrieve

def fetch_alerts(date, path_alerts, skip_existing=False):

    # create path if it doesn't exist
    Path(path_alerts).mkdir(parents=True, exist_ok=True)
    # if the path is a file, raise an error
    if Path(path_alerts).is_file():
        raise IsADirectoryError("A file with the same name as the directory exists")
    # if there is data in the directory, raise an error
    if os.listdir(path_alerts):
        if not skip_existing:
            raise FileExistsError("Directory is not empty")
        else:
            print("Directory is not empty, skipping download")
            return
    
    # verify that date is valid
    try:
        time.strptime(date, "%Y%m%d")
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYYMMDD")
    
    print(f"Fetching ZTF alerts for {date}")
    # download to a temp directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        urllib.request.urlretrieve(f"https://ztf.uw.edu/alerts/public/ztf_public_{date}.tar.gz", f"{tmp_dir}/ztf_public_{date}.tar.gz")

        print(f"Decompressing alerts to {path_alerts}")
        # decompress to the desired directory
        subprocess.run(["tar", "-xzf", f"{tmp_dir}/ztf_public_{date}.tar.gz", "-C", path_alerts])

        # check if the alerts were decompressed
        if not Path(path_alerts).exists():
            raise FileNotFoundError("Alerts not found")

        # remove the tarball
        os.remove(f"{tmp_dir}/ztf_public_{date}.tar.gz")

        print("Done!")
