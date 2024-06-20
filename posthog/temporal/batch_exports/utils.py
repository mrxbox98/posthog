import asyncio
import collections.abc
import contextlib
import typing
import uuid

from posthog.batch_exports.models import BatchExportRun
from posthog.batch_exports.service import aupdate_batch_export_run

T = typing.TypeVar("T")


def peek_first_and_rewind(
    gen: collections.abc.Generator[T, None, None],
) -> tuple[T | None, collections.abc.Generator[T, None, None]]:
    """Peek into the first element in a generator and rewind the advance.

    The generator is advanced and cannot be reversed, so we create a new one that first
    yields the element we popped before yielding the rest of the generator.

    Returns:
        A tuple with the first element of the generator and the generator itself.
    """
    try:
        first = next(gen)
    except StopIteration:
        first = None

    def rewind_gen() -> collections.abc.Generator[T, None, None]:
        """Yield the item we popped to rewind the generator.

        Return early if the generator is empty.
        """
        if first is None:
            return

        yield first
        yield from gen

    return (first, rewind_gen())


@contextlib.asynccontextmanager
async def set_status_to_running_task(
    run_id: str | None, logger, timeout: float = 10.0
) -> typing.AsyncIterator[asyncio.Task]:
    """Manage a background task to set a batch export run to 'RUNNING' status.

    This is intended to be used within a batch export's 'insert_*' activity. These activities cannot afford
    to fail if our database is experiencing issues, as we should strive to not let issues in our infrastructure
    propagate to users. So, we do a best effort update and swallow the exception if we fail.

    Even if we fail to update the status here, the 'finish_batch_export_run' activity at the end of each batch
    export will retry indefinitely and wait for postgres to recover, eventually making a final update with
    the status. This means that, worse case, the batch export status won't be displayed as 'RUNNING' while running.
    """
    if run_id is None:
        # Should never land here except in tests of individual activities
        yield
        return

    background_task = asyncio.create_task(
        aupdate_batch_export_run(uuid.UUID(run_id), status=BatchExportRun.Status.RUNNING)
    )

    def done_callback(task):
        if task.exception() is not None:
            logger.warn(
                "Unexpected error trying to set batch export to 'RUNNING' status. Run will continue but displayed status may not be accurate until run finishes",
                exc_info=task.exception(),
            )

    background_task.add_done_callback(done_callback)

    try:
        yield background_task

    finally:
        if not background_task.done():
            background_task.cancel()
            await asyncio.wait([background_task])
