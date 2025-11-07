"""
Setup sentry and use some sane defaults
"""

import functools
import os
from typing import TYPE_CHECKING

import sentry_sdk

if TYPE_CHECKING:
    from dagster import OpExecutionContext


def setup_sentry(pipeline_name: str):
    """
    Setup the sentry SDK for Dagster.

    Additionally if IMAGE_TAG is defined it will be set as the release,
    and TRACES_SAMPLE_RATE can be set 0-1 otherwise will default to 0.

    Manually sets up a bunch of the default integrations and disables logging of dagster
    to quiet things down.
    """

    # ignore_logger("dagster")

    SENTRY_DSN = os.environ.get("SENTRY_DSN")
    IMAGE_TAG = os.environ.get("IMAGE_TAG")
    TRACES_SAMPLE_RATE = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", 1))
    PROFILE_SAMPLE_RATE = float(os.environ.get("SENTRY_PROFILE_SAMPLE_RATE", 1))

    environment = "dev" if "dev" in IMAGE_TAG else "production"

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=environment,
        server_name=pipeline_name,
        # integrations=[
        #     AtexitIntegration(),
        #     DedupeIntegration(),
        #     StdlibIntegration(),
        #     ModulesIntegration(),
        #     ArgvIntegration(),
        #     LoggingIntegration(),
        # ],
        # before_send=dagster_before_send,
        # Add request headers and IP for users,
        # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
        send_default_pii=True,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for tracing.
        traces_sample_rate=TRACES_SAMPLE_RATE,
        # To collect profiles for all profile sessions,
        # set `profile_session_sample_rate` to 1.0.
        profile_session_sample_rate=PROFILE_SAMPLE_RATE,
        # Profiles will be automatically collected while
        # there is an active span.
        profile_lifecycle="trace",
        # Enable logs to be sent to Sentry
        enable_logs=True,
    )

    sentry_sdk.set_tag("image_tag", IMAGE_TAG)
    sentry_sdk.set_tag("pipeline_name", pipeline_name)


def dagster_before_send(event, hint):
    """
    Don't send sentry event id error logs to sentry
    """
    logentry = event.get("logentry")
    if logentry and "Sentry captured an exception. Event ID:" in logentry.get(
        "message",
        "",
    ):
        return None
    return event


def log_op_context(context: "OpExecutionContext"):
    """
    Capture Dagster OP context for Sentry Error handling
    """
    sentry_sdk.add_breadcrumb(
        category="dagster",
        message=f"{context.job_name} - {context.op_def.name}",
        level="info",
        data={
            "run_config": context.run_config,
            "job_name": context.job_name,
            "op_name": context.op_def.name,
            "run_id": context.run_id,
            "retry_number": context.retry_number,
        },
    )
    sentry_sdk.set_tag("job_name", context.job_name)
    sentry_sdk.set_tag("op_name", context.op_def.name)
    sentry_sdk.set_tag("run_id", context.run_id)

    sentry_sdk.set_context(
        "dagster_op",
        {
            "run_config": context.run_config,
            "job_name": context.job_name,
            "op_name": context.op_def.name,
            "run_id": context.run_id,
            "retry_number": context.retry_number,
        },
    )


def capture_op_exceptions(func):
    """
    Captures exceptions thrown by Dagster Ops and forwards them to Sentry
    before re-throwing them for Dagster.

    Expects ops to receive Dagster context as the first argument,
    but it will continue if it doesn't (it just won't get as much context).

    It will log a unique ID that can be then entered into Sentry to find
    the exception.

    This should be used as a decorator between Dagster's `@op`,
    and the function to be handled.

    @op
    @sentry.capture_op_exceptions
    def op_with_error(context):
        raise Exception("Ahh!")
    """
    from dagster import get_dagster_logger

    @functools.wraps(func)
    def wrapped_fn(*args, **kwargs):
        logger = get_dagster_logger("sentry")

        try:
            log_op_context(args[0])
        except (AttributeError, IndexError):
            logger.warn("Sentry did not find execution context as the first arg")

        with sentry_sdk.start_transaction(
            op=func.__name__,
            name=f"Dagster op: {func.__name__}",
        ):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                event_id = sentry_sdk.capture_exception(e)
                logger.error(f"Sentry captured an exception. Event ID: {event_id}")
                raise e

    return wrapped_fn
