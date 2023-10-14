import logging
import sys
from logging import Logger
from typing import TYPE_CHECKING, Any, Final, Literal

import click

import sghi.app
from sghi.config import ConfigurationError
from sghi.dispatch import Dispatcher

from . import signals
from .constants import APP_VERBOSITY_REG_KEY
from .setup import CONFIG_FORMATS, LoadConfigError, load_config_file, setup
from .ui import UI, NoUI

if TYPE_CHECKING:
    from collections.abc import Mapping

# =============================================================================
# TYPES
# =============================================================================


Supported_UIs = Literal["none", "simple"]


# =============================================================================
# CONSTANTS
# =============================================================================


_LOGGER: Final[Logger] = logging.getLogger("sghi.ml-pipeline.runtime")


# =============================================================================
# HELPERS
# =============================================================================


def _configure_runtime(
    app_dispatcher: Dispatcher,
    config: str | None,
    config_format: CONFIG_FORMATS,
    log_level: str,
    verbosity: int,
) -> None:
    try:
        sghi.app.registry[APP_VERBOSITY_REG_KEY] = verbosity

        config_contents: Mapping[str, Any] | None = (
            load_config_file(
                config_file_path=config,
                config_format=config_format,
            )
            if config is not None
            else None
        )
        sghi.app.setup = setup
        sghi.app.setup(settings=config_contents, log_level=log_level)
    except LoadConfigError as exp:
        _default_err: str = "Error loading configuration."
        app_dispatcher.send(
            signals.ConfigFailed(exp.message or _default_err, exp),
        )
        sys.exit(2)
    except ConfigurationError as exp:
        _err_msg: str = (
            "Error configuring the runtime. The cause of the error was: "
            f"{exp.message}"
        )
        # This might not be logged as logging may still be un-configured when
        # this error occurs.
        _LOGGER.exception(_err_msg)
        app_dispatcher.send(signals.ConfigFailed(_err_msg, exp))
        sys.exit(3)


def _set_ui(preferred_ui: Supported_UIs) -> UI:
    match preferred_ui:
        case "none":
            return NoUI()
        case "simple":
            from sghi.ml_pipeline.runtime.tui import SimpleUI

            return SimpleUI()
        case _:
            _err_msg: str = "Unsupported UI option given."
            raise ConfigurationError(message=_err_msg)


# =============================================================================
# MAIN
# =============================================================================


@click.group(epilog="Lets do this! ;)", invoke_without_command=True)
@click.option(
    "-c",
    "--config",
    default=None,
    envvar="ML_PIPELINE_CONFIG",
    help=(
        "Set the location of the configuration file to use. Both toml and "
        "yaml file formats are supported."
    ),
    type=click.Path(exists=True, readable=True, resolve_path=True),
)
@click.option(
    "--config-format",
    default="auto",
    envvar="ML_PIPELINE_CONFIG_FORMAT",
    help=(
        "The config format of the configuration file in use. Both toml and "
        "yaml configuration formats are supported. 'auto' determines the "
        "configuration file in use based on the extension of the file. When "
        "that fails, defaults to assuming the configuration file is a toml "
        "file."
    ),
    show_default=True,
    type=click.Choice(choices=("auto", "toml", "yaml")),
)
@click.option(
    "-l",
    "--log-level",
    default="WARNING",
    envvar="ML_PIPELINE_LOG_LEVEL",
    help='Set the log level of the "root application" logger.',
    show_default=True,
    type=click.Choice(
        choices=(
            "CRITICAL",
            "ERROR",
            "WARNING",
            "INFO",
            "DEBUG",
            "NOTSET",
        ),
    ),
)
@click.option(
    "--ui",
    default="none",
    envvar="ML_PIPELINE_UI",
    help="Select a user interface to use.",
    show_default=True,
    type=click.Choice(choices=("none", "simple")),
)
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    default=0,
    envvar="ML_PIPELINE_VERBOSITY",
    help=(
        "Set the level of output to expect from the program on stdout. This "
        "is different from log level."
    ),
)
@click.version_option(package_name="sghi-ml-pipeline", message="%(version)s")
@click.pass_context
def main(
    ctx: click.Context,
    config: str | None,
    config_format: CONFIG_FORMATS,
    log_level: str,
    ui: Supported_UIs,
    verbosity: int,
) -> None:
    """A tool for executing ML workflows.

    \f

    :param ctx: An object holding the CLI's context.
    :param config: An option path to a configuration file.
    :param config_format: The format of the config contents. Can be 'auto'
        which allows the configuration format to be determined from the
        extension of the file name.
    :param log_level: The log level of the "root application" logger.
    :param ui: The preferred user interface to use.
    :param verbosity: The level of output to expect from the application on
        stdout. This is different from log level.

    :return: None.
    """
    app_ui: UI = _set_ui(preferred_ui=ui)
    app_ui.start()

    _configure_runtime(
        app_dispatcher=sghi.app.dispatcher,
        config=config,
        config_format=config_format,
        log_level=log_level,
        verbosity=verbosity,
    )

    ctx.obj = app_ui
    if ctx.invoked_subcommand is None:
        ctx.invoke(run)


@main.command(name="list")
@click.pass_obj
def list_workflows(app_ui: UI) -> None:
    """List all available workflows."""

    app_dispatcher: Dispatcher = sghi.app.dispatcher

    try:
        app_dispatcher.send(signals.AppReady())

        from .tui.simple.printers import print_debug, print_info

        # Delay this import as late as possible to avoid cyclic imports,
        # especially before application setup has completed.
        from .usecases import list_workflows as _list_workflows

        print_info("")
        print_info("=========================================================")
        print_info("|               AVAILABLE WORKFLOWS                     |")
        print_info("=========================================================")
        print_info("")

        for wf_id, workflow in _list_workflows().items():
            print_debug(f"\t- {wf_id}:{workflow.name}")

        print_info("")
        print_info("---------------------------------------------------------")
        app_dispatcher.send(signals.AppStopping())
    except Exception as exp:
        _err_msg: str = (
            "An unhandled error occurred at runtime. The cause of the error "
            f"was: {exp!s}."
        )
        _LOGGER.exception(_err_msg)
        app_dispatcher.send(signals.UnhandledRuntimeErrorSignal(_err_msg, exp))
        sys.exit(5)

    app_ui.stop()


@main.command()
@click.option(
    "-s",
    "--select",
    "selected",
    default=(),
    help=(
        "The id of a specific workflow to run. Can be provided more than "
        "once. If no selection is made, then all available workflows are "
        "executed."
    ),
    multiple=True,
    type=str,
)
@click.pass_obj
def run(app_ui: UI, selected: tuple[str, ...]) -> None:
    """Run the selected workflows. Run all when no selection is made."""

    app_dispatcher: Dispatcher = sghi.app.dispatcher

    try:
        app_dispatcher.send(signals.AppReady())

        # Delay this import as late as possible to avoid cyclic imports,
        # especially before application setup has completed.
        from .usecases import run as _run

        _run(select=selected if selected else None)
        app_dispatcher.send(signals.AppStopping())
    except Exception as exp:
        _err_msg: str = (
            "An unhandled error occurred at runtime. The cause of the error "
            f"was: {exp!s}."
        )
        _LOGGER.exception(_err_msg)
        app_dispatcher.send(signals.UnhandledRuntimeErrorSignal(_err_msg, exp))
        sys.exit(5)

    app_ui.stop()


if __name__ == "__main__":
    main(auto_envvar_prefix="ML_PIPELINE")
