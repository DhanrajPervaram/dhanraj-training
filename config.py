"""
Helper functions for working with the configuration ini file.
"""

# Built In
from configparser import ConfigParser
from pathlib import Path

# 3rd Party
# Owned

PARENT_PATH = Path(__file__).parent.absolute()

def get_config_parser() -> ConfigParser:
    """
    Get a config parse which read the `congif.ini` file in the root of the project.
    """
    config_file_path = PARENT_PATH.parent / 'config.ini'
    config_parser = ConfigParser(interpolation=None)
    config_parser.read(config_file_path)
    return config_parser
