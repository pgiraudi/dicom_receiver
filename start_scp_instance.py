#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import json
from dicom_receiver import DICOMReceiver

CONFIG_FILE = "scp_config.json"

def run_instance(target_ae):
    with open(CONFIG_FILE, "r") as f:
        configs = json.load(f)
    
    for conf in configs:
        if conf["ae_title"] == target_ae:
            scp = DICOMReceiver(
                ae_title=conf["ae_title"],
                port=conf["port"],
                output_dir=conf["output_dir"],
                log_file=conf["log_file"]
            )
            scp.start()
            return
    
    print(f"AE Title '{target_ae}' not found in config.")
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python start_scp_instance.py AE_TITLE")
        sys.exit(1)
    
    run_instance(sys.argv[1])
