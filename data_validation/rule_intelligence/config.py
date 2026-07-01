"""
data_validation/rule_intelligence/config.py
-----------------------------------------------
Reads the same flat ``key=value`` config file format used by
``data_validation.main``. Deliberately duplicated (not imported) so that this
subpackage never transitively imports ``pyspark`` via ``data_validation.main``.
"""

import os


def load_db_config(conf_path: str) -> dict:
    """Parse a simple ``key=value`` configuration file into a dictionary.

    Args:
        conf_path (str): Absolute path to the configuration file.

    Returns:
        dict: Mapping of configuration key -> value strings.
    """
    config = {}
    with open(conf_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                config[key.strip()] = value.strip()
    return config


def resolve_db_password(config: dict) -> str:
    """Resolve the MySQL password, preferring DB_PASSWORD over the conf file.

    Args:
        config (dict): Parsed config dict (see :func:`load_db_config`).

    Returns:
        str: The password to use.
    """
    return os.environ.get("DB_PASSWORD", config.get("pswd", ""))
