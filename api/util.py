#!/usr/bin/env python3

import yaml
import logging
import traceback

def getLogger():
    return logging.getLogger('gunicorn.error')

def loadYamlData(filepath):
    logger = getLogger()
    with open(filepath, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.fatal('Failed to load YAML data from "%s"' % filepath)
            logger.fatal(e)
            logger.fatal(traceback.format_exc())
            return None
