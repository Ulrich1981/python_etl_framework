#!/usr/bin/env python3.10

import importlib
import yaml
import sys

job_name = sys.argv[1]
with open(f"jobs/job_default_settings.yaml", "r") as file:
    default_config = yaml.safe_load(file)
with open(f"jobs/{job_name}.yaml", "r") as file:
    config = yaml.safe_load(file)

for step in config["steps"]:
    conf = default_config["universal"] | (default_config[step["name"]] or dict()) | config["universal"] | step
    module = importlib.import_module(".".join([conf["name"],conf["module"]]), package=None)
    if step["name"] == "extract":
        df = module.extract(conf)
    if step["name"] == "transform":
        df = module.transform(conf, df = df)
    if step["name"] == "load":
        module.load(conf, df = df)