from __future__ import annotations

import asyncio
import logging
import signal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .database import create_schema
from .jobs import import_existing_data, run_scanner_job


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("wei.worker")


async def main() -> None:
    await create_schema()
    imported = await import_existing_data()
    logger.info("Initial legacy import: %s", imported)

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_scanner_job,
        IntervalTrigger(minutes=5),
        id="strategy_scan",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=240,
        replace_existing=True,
    )
    scheduler.add_job(
        import_existing_data,
        CronTrigger(minute=17),
        id="hourly_reconcile",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.start()
    asyncio.create_task(run_scanner_job())

    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, stopped.set)
        except NotImplementedError:
            pass
    await stopped.wait()
    scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())

