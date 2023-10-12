from collections.abc import Mapping, Sequence
from typing import Any

import sghi.app
from sghi.config import Config, ConfigProxy, SettingInitializer

from ..constants import APP_LOG_LEVEL_REG_KEY, DEFAULT_CONFIG
from .config_file_loaders import (
    CONFIG_FORMATS,
    LoadConfigError,
    load_config_file,
    load_toml_config_file,
    load_yaml_config_file,
)


def setup(
    settings: Mapping[str, Any] | None = None,
    settings_initializers: Sequence[SettingInitializer] | None = None,
    log_level: int | str = "NOTSET",
    disable_default_initializers: bool = False,
) -> None:
    """Prepare the runtime and ready the application for use.

    :param settings: An optional mapping of settings and their values. When not
        provided, the runtime defaults as well as defaults set by the given
        setting initializers will be used instead.
    :param settings_initializers: An optional sequence of setting initializers
        to execute during runtime setup. Default initializers(set by the
        runtime) are always executed unless the `disable_default_initializers`
        param is set to ``True``.
    :param log_level: The log level to set for the root application logger.
        When not set defaults to the value "NOTSET".
    :param disable_default_initializers: Exclude default setting initializers
        from being executed as part of the runtime setup. The default setting
        initializers perform logging and loading of ETL workflows into the
        application registry.

    :return: None.
    """
    settings_dict: dict[str, Any] = dict(DEFAULT_CONFIG)
    settings_dict.update(settings or {})

    initializers: list[SettingInitializer] = list(settings_initializers or [])
    if not disable_default_initializers:
        from .setting_initializers import (
            ETLWorkflowsInitializer,
            LoggingInitializer,
        )

        initializers.insert(0, LoggingInitializer())
        initializers.insert(1, ETLWorkflowsInitializer())

    sghi.app.registry[APP_LOG_LEVEL_REG_KEY] = log_level
    config: Config = Config.of(
        settings=settings_dict,
        setting_initializers=initializers,
    )
    match sghi.app.conf:
        case ConfigProxy():
            sghi.app.conf.set_source(config)
        case _:
            setattr(sghi.app, "conf", config)  # noqa: B010


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "CONFIG_FORMATS",
    "LoadConfigError",
    "load_config_file",
    "load_toml_config_file",
    "load_yaml_config_file",
    "setup",
]
