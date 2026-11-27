import io
import logging
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_logging import (
    LoggingSession,
    configure_worker_output,
    plugin_log_filename,
)
from parallelazy.plugin_manager import PluginManager
from parallelazy.utils import init_debugger


class _LoggingProbePlugin(Plugin):
    def on_start(self):
        self.logger.info("plugin logging probe started")
        print("plugin stdout probe")
        print("plugin stderr probe", file=sys.stderr)

    def main(self):
        self.wait_for_stop()


class _FlushRecordingStream(io.StringIO):
    def __init__(self):
        super().__init__()
        self.flush_count = 0

    def flush(self):
        self.flush_count += 1
        super().flush()


class PluginLoggingTests(unittest.TestCase):
    def test_display_logs_false_suppresses_worker_streams_but_still_captures_them(self):
        log_file = os.path.join(tempfile.gettempdir(), f"hidden-stream-{id(self)}.log")
        terminal_stdout = _FlushRecordingStream()
        terminal_stderr = _FlushRecordingStream()
        logger = logging.getLogger(f"logging-hidden-test.{id(self)}")
        logger.handlers.clear()
        logger.propagate = False

        with patch("sys.stdout", terminal_stdout), patch("sys.stderr", terminal_stderr):
            session = LoggingSession(
                logger=logger,
                log_file=log_file,
                plugin_id="worker-1",
                log_level=logging.INFO,
                display_logs=False,
            )
            capture = configure_worker_output(logger, display_logs=False)
            try:
                print("hidden stdout")
                print("hidden stderr", file=sys.stderr)
            finally:
                capture.close()
                session.close()

        self.assertEqual(terminal_stdout.getvalue(), "")
        self.assertEqual(terminal_stderr.getvalue(), "")
        with open(log_file, encoding="utf-8") as log_stream:
            contents = log_stream.read()
        self.assertIn("[stdout] hidden stdout", contents)
        self.assertIn("[stderr] hidden stderr", contents)
        os.remove(log_file)

    def test_write_logs_false_creates_no_log_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            log_file = os.path.join(temporary_directory, "worker.log")
            logger = logging.getLogger(f"logging-no-file-test.{id(self)}")
            logger.handlers.clear()
            logger.propagate = False
            session = LoggingSession(
                logger=logger,
                log_file=log_file,
                plugin_id="worker",
                log_level=logging.INFO,
                display_logs=False,
                write_logs=False,
            )
            try:
                logger.info("not written")
            finally:
                session.close()

            self.assertFalse(os.path.exists(log_file))

    def test_worker_streams_are_logged_and_still_written_to_terminal(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            log_file = os.path.join(temporary_directory, "worker.log")
            terminal_stdout = _FlushRecordingStream()
            terminal_stderr = _FlushRecordingStream()
            logger = logging.getLogger(f"logging-stream-test.{id(self)}")
            logger.handlers.clear()
            logger.propagate = False

            with patch("sys.stdout", terminal_stdout), patch("sys.stderr", terminal_stderr):
                session = LoggingSession(
                    logger=logger,
                    log_file=log_file,
                    plugin_id="worker-1",
                    log_level=logging.INFO,
                    display_logs=True,
                )
                capture = configure_worker_output(logger)
                try:
                    print("stdout-visible")
                    self.assertGreater(terminal_stdout.flush_count, 0)
                    print("stderr-visible", file=sys.stderr)
                    sys.stderr.write("progress 10%\r")
                    sys.stderr.write("progress 100%\n")
                finally:
                    capture.close()
                    session.close()

            self.assertEqual(terminal_stdout.getvalue(), "stdout-visible\n")
            self.assertEqual(
                terminal_stderr.getvalue(),
                "stderr-visible\nprogress 10%\rprogress 100%\n",
            )
            with open(log_file, encoding="utf-8") as log_stream:
                contents = log_stream.read()
            self.assertIn("[stdout] stdout-visible", contents)
            self.assertIn("[stderr] stderr-visible", contents)
            self.assertIn("[stderr] progress 10%", contents)
            self.assertIn("[stderr] progress 100%", contents)

    def test_debugger_port_is_logged_once_without_direct_print(self):
        debugpy = Mock()
        debugpy.listen.return_value = (None, 46179)

        with patch("parallelazy.utils.get_debugpy", return_value=debugpy):
            with patch("parallelazy.utils._notify_vscode_to_attach"), \
                    patch("builtins.print") as print_mock, \
                    self.assertLogs("parallelazy.utils", level="INFO") as logs:
                init_debugger("test-plugin", wait_for_client=False)

        self.assertEqual(len(logs.output), 1)
        self.assertIn("46179 for test-plugin process", logs.output[0])
        print_mock.assert_not_called()

    def test_descriptor_logging_overrides_round_trip(self):
        descriptor = PluginDescriptor(
            name="worker",
            version="1.0",
            package="example",
            class_name="Worker",
            work_dir="plugin-work",
            log_dir="plugin-logs",
            log_level=logging.DEBUG,
            display_logs=False,
        )

        restored = PluginDescriptor.from_json(descriptor.to_json())

        self.assertEqual(restored, descriptor)

    def test_manager_defaults_and_descriptor_overrides_resolve(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            work_dir = os.path.join(temporary_directory, "experiment")
            manager = PluginManager(
                work_dir=work_dir,
                log_dir="logs",
                log_level=logging.WARNING,
                display_logs=False,
            )
            try:
                settings = manager._plugin_runtime_settings(
                    PluginDescriptor(
                        name="worker",
                        version="1.0",
                        package="example",
                        class_name="Worker",
                        work_dir="plugin-work",
                        log_dir="plugin-logs",
                        log_level=logging.DEBUG,
                    )
                )

                self.assertEqual(settings.work_dir, os.path.join(work_dir, "plugin-work"))
                self.assertEqual(
                    settings.log_dir,
                    os.path.join(
                        work_dir,
                        "plugin-work",
                        "plugin-logs",
                        manager.run_id,
                    ),
                )
                self.assertEqual(settings.log_level, logging.DEBUG)
                self.assertFalse(settings.display_logs)
                self.assertTrue(os.path.isdir(settings.work_dir))
                self.assertTrue(os.path.isdir(settings.log_dir))
                self.assertTrue(os.path.isfile(os.path.join(manager.run_log_dir, "manager.log")))
            finally:
                manager._logging.close()

    def test_changing_work_dir_updates_manager_and_plugin_logs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            initial_work_dir = os.path.join(temporary_directory, "initial")
            work_dir = os.path.join(temporary_directory, "experiment")
            manager = PluginManager(work_dir=initial_work_dir, display_logs=False)
            try:
                manager.work_dir = work_dir
                manager.logger.info("manager log after work directory change")

                self.assertEqual(manager.work_dir, work_dir)
                self.assertEqual(manager.log_dir, os.path.join(work_dir, "logs"))
                self.assertEqual(
                    manager.run_log_dir,
                    os.path.join(work_dir, "logs", manager.run_id),
                )
                with open(
                    os.path.join(manager.run_log_dir, "manager.log"),
                    encoding="utf-8",
                ) as log_stream:
                    self.assertIn(
                        "manager log after work directory change",
                        log_stream.read(),
                    )

                settings = manager._plugin_runtime_settings(
                    PluginDescriptor(
                        name="worker",
                        version="1.0",
                        package="example",
                        class_name="Worker",
                    )
                )
                self.assertEqual(settings.work_dir, work_dir)
                self.assertEqual(settings.log_dir, manager.run_log_dir)
            finally:
                manager._logging.close()

    def test_changing_log_dir_updates_manager_and_plugin_logs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            work_dir = os.path.join(temporary_directory, "initial")
            new_work_dir = os.path.join(temporary_directory, "experiment")
            manager = PluginManager(work_dir=work_dir, display_logs=False)
            try:
                manager.log_dir = "experiment-logs"
                manager.work_dir = new_work_dir
                manager.logger.info("manager log after log directory change")

                self.assertEqual(manager.log_dir, os.path.join(new_work_dir, "experiment-logs"))
                self.assertEqual(
                    manager.run_log_dir,
                    os.path.join(new_work_dir, "experiment-logs", manager.run_id),
                )
                with open(
                    os.path.join(manager.run_log_dir, "manager.log"),
                    encoding="utf-8",
                ) as log_stream:
                    self.assertIn(
                        "manager log after log directory change",
                        log_stream.read(),
                    )

                settings = manager._plugin_runtime_settings(
                    PluginDescriptor(
                        name="worker",
                        version="1.0",
                        package="example",
                        class_name="Worker",
                    )
                )
                self.assertEqual(settings.log_dir, manager.run_log_dir)
            finally:
                manager._logging.close()

    def test_log_dir_cannot_change_after_plugin_loading_starts(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            manager = PluginManager(work_dir=temporary_directory, display_logs=False)
            original_log_dir = manager.log_dir
            manager._loading_plugins.add("worker")
            try:
                with self.assertRaisesRegex(RuntimeError, "log_dir cannot be changed"):
                    manager.log_dir = "new-logs"
                self.assertEqual(manager.log_dir, original_log_dir)
            finally:
                manager._logging.close()

    def test_spawned_plugin_writes_to_its_run_log(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            manager = PluginManager(work_dir=temporary_directory, display_logs=False)
            descriptor = _LoggingProbePlugin.Descriptor(
                version="1.0",
                author="tests",
                name="logging-probe",
                display_logs=False,
            )
            try:
                self.assertTrue(manager.load_plugin(descriptor))
                self.assertTrue(manager.start_plugin(descriptor.name))
                status = manager.plugin_status(descriptor.name)
                self.assertEqual(status.startup_phase, "ready")
                self.assertTrue(manager.stop_plugin(descriptor.name))
                self.assertTrue(manager.unload_plugin(descriptor.name))

                plugin_log = os.path.join(
                    manager.run_log_dir,
                    plugin_log_filename(descriptor.name),
                )
                with open(plugin_log, encoding="utf-8") as log_stream:
                    contents = log_stream.read()
                self.assertIn("plugin logging probe started", contents)
                self.assertIn("[stdout] plugin stdout probe", contents)
                self.assertIn("[stderr] plugin stderr probe", contents)
            finally:
                manager.unload_all_plugins()
                manager._logging.close()

    def test_file_is_recreated_after_its_directory_is_removed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            log_dir = os.path.join(temporary_directory, "run")
            log_file = os.path.join(log_dir, "worker.log")
            logger = logging.getLogger(f"logging-test.{id(self)}")
            logger.handlers.clear()
            logger.propagate = False
            session = LoggingSession(
                logger=logger,
                log_file=log_file,
                plugin_id="worker",
                log_level=logging.INFO,
                display_logs=False,
            )
            try:
                logger.info("before deletion")
                os.remove(log_file)
                shutil.rmtree(log_dir)
                logger.warning("after deletion")

                with open(log_file, encoding="utf-8") as log_stream:
                    contents = log_stream.read()

                self.assertIn("# Log opened:", contents)
                self.assertIn("WARNING after deletion", contents)
                self.assertNotIn("before deletion", contents)
            finally:
                session.close()

    def test_file_and_console_formats_can_be_changed_independently(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            logger = logging.getLogger(f"logging-format-test.{id(self)}")
            logger.handlers.clear()
            logger.propagate = False
            output = io.StringIO()
            with patch("sys.stdout", output):
                session = LoggingSession(
                    logger=logger,
                    log_file=os.path.join(temporary_directory, "worker.log"),
                    plugin_id="worker-1",
                    log_level=logging.INFO,
                    display_logs=True,
                )
                try:
                    session.set_formats(
                        file_format="FILE %(levelname)s %(message)s",
                        console_format="CONSOLE [%(plugin_id)s] %(message)s",
                    )
                    logger.info("ready")
                finally:
                    session.close()

            with open(
                os.path.join(temporary_directory, "worker.log"),
                encoding="utf-8",
            ) as log_stream:
                file_contents = log_stream.read()

            self.assertIn("FILE INFO ready", file_contents)
            self.assertNotIn("worker-1", file_contents)
            self.assertIn("CONSOLE [worker-1] ready", output.getvalue())

if __name__ == "__main__":
    unittest.main()
