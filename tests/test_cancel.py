"""Regression tests for cancellation and the commit boundary."""

import ast
import asyncio
from contextlib import nullcontext
import importlib.util
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
import unittest

component = Path(__file__).parents[1] / "custom_components/measurement_lts"
spec = importlib.util.spec_from_file_location("measurement_lts_control", component / "control.py")
control = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = control
spec.loader.exec_module(control)


class CancellationTest(unittest.TestCase):
    def test_request_before_commit_prevents_commit(self):
        c = control.Cancellation()
        self.assertTrue(c.request())
        with self.assertRaises(control.BackfillCancelled):
            c.begin_commit()

    def test_request_after_commit_starts_is_rejected(self):
        c = control.Cancellation()
        c.begin_commit()
        self.assertFalse(c.request())
        c.check()

    def test_writer_rolls_back_and_does_not_queue_purge(self):
        cancellation = control.Cancellation()
        reported = []
        queued = []
        with sqlite3.connect(":memory:") as db:
            db.execute("create table statistics (entity text)")

            def write_plan(session, metadata_id, plan, reviewed, replace, cutoff, token):
                session.db.execute("insert into statistics values (?)", (plan,))
                self.assertTrue(token.request())
                return plan

            tree = ast.parse((component / "backfill.py").read_text(encoding="utf-8"))
            writer = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "_BackfillTask")
            run = next(n for n in writer.body if getattr(n, "name", None) == "run")
            env = {
                "Recorder": object,
                "time": lambda: 100000,
                "_build": lambda session, request, *args: (1, request.entity_id),
                "_write_plan": write_plan,
                "_resolve": lambda future, value=None, error=None: reported.append(error),
            }
            exec(compile(ast.Module(body=[run], type_ignores=[]), "<writer>", "exec"), env)
            loop = SimpleNamespace(call_soon_threadsafe=lambda callback, *args: callback(*args))
            session = SimpleNamespace(db=db, begin=lambda: db)
            recorder = SimpleNamespace(
                hass=SimpleNamespace(loop=loop), keep_days=30,
                get_session=lambda: nullcontext(session), queue_task=queued.append,
            )
            request = SimpleNamespace(entity_id="sensor.test", purge_keep_days=1)
            job = SimpleNamespace(
                requests=(request,), reviewed={"sensor.test": None},
                replace_conflicts=False, cancellation=cancellation,
                on_progress=None, future=object(),
            )
            env["run"](job, recorder)
            self.assertEqual(db.execute("select count(*) from statistics").fetchone()[0], 0)
        self.assertEqual(queued, [])
        self.assertIsInstance(reported[0], control.BackfillCancelled)


class JobTest(unittest.IsolatedAsyncioTestCase):
    async def test_closing_progress_waiter_does_not_detach_active_worker(self):
        started = asyncio.Event()
        finish = asyncio.Event()
        notified = []
        errors = []
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _, context: errors.append(context))
        self.addCleanup(loop.set_exception_handler, previous)

        async def work():
            started.set()
            await finish.wait()
            cancellation.check()

        hass = SimpleNamespace(data={}, async_create_task=asyncio.create_task)
        cancellation = control.Cancellation()
        job = control.start_job(hass, cancellation, work(), lambda: notified.append(True))
        waiter = asyncio.create_task(job.wait())
        await started.wait()
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertIs(control.active_job(hass), job)
        self.assertTrue(job.cancel())
        self.assertEqual(notified, [True])
        finish.set()
        with self.assertRaises(control.BackfillCancelled):
            await job.wait()
        self.assertIsNone(control.active_job(hass))
        self.assertEqual(errors, [])
