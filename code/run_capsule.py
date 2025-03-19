# stdlib imports --------------------------------------------------- #
import argparse
import dataclasses
import json
import functools
import logging
import pathlib
import time
import types
import typing
import uuid
from typing import Any, Literal

# 3rd-party imports ------------------------ ----------------------- #
import polars as pl
import pydantic
import pydantic_settings
import pydantic_settings.sources

# local modules ---------------------------------------------------- #
import utils

# logging configuration -------------------------------------------- #
# use `logger.info(msg)` instead of `print(msg)` so we get timestamps and origin of log messages
logger = logging.getLogger(
    pathlib.Path(__file__).stem if __name__.endswith("_main__") else __name__
    # multiprocessing gives name '__mp_main__'
)

# define run params here ------------------------------------------- #

class CapsuleParameters(pydantic_settings.BaseSettings):

    session_id: str
    area: str
    n_units: int
    logging_level: str | int = 'INFO'
    test: bool = False

    # set the priority of the sources:
    # ignore the function signature for now: concentrate on the return value
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        *args,
        **kwargs,
    ):
        # the order of the sources is what defines the priority:
        # - first source is highest priority
        # - for each field in the class, the first source that contains a value will be used
        return (
            init_settings,
            pydantic_settings.sources.JsonConfigSettingsSource(settings_cls, json_file=next(pathlib.Path('/data/parameters').glob('*_input_parameters*.json'), None)),
            pydantic_settings.CliSettingsSource(settings_cls, cli_parse_args=True),
        )


# processing function ---------------------------------------------- #
# modify the body of this function, but keep the same signature
def process(params: CapsuleParameters) -> None:

    """Process a single session with parameters defined in `params` and save results + params to
    /results/outputs.
    
    A test mode should be implemented to allow for quick testing of the capsule (required every time
    a change is made if the capsule is in a pipeline) 
    """
    logger.info(f"Processing {params!r}")

    chart = (
        utils.get_df('units', lazy=True)
        .filter(
            pl.col('session_id') == params.session_id,
            pl.col('structure') == params.area,
        )
        .collect()
        .sample(params.n_units)
        .sort('activity_drift')
        .plot
        .bar(
            x='location:N',
            y='activity_drift:Q',
            color='activity_drift:Q',
        )
    )

    pipeline_suffix = f"_{utils.AWS_BATCH_JOB_ID.split('-')[0]}" if utils.is_pipeline() else ""
    output_path = pathlib.Path(f"/results/outputs/{params.session_id}{pipeline_suffix}.html")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing results to {output_path}")
    chart.save(output_path)

# ------------------------------------------------------------------ #

def main():
    t0 = time.time()
    
    utils.setup_logging()

    # get arguments passed from command line or "AppBuilder" interface:
    params = CapsuleParameters()  # anything passed to init will override values from json/CLI
    logger.setLevel(params.logging_level)        

    process(params)

    utils.ensure_nonempty_results_dirs(['/results', '/results/outputs'])
    logger.info(f"Time elapsed: {time.time() - t0:.2f} s")

if __name__ == "__main__":
    main()