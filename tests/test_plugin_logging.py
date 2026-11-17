import io
import logging
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_logging import LoggingSession, plugin_log_filename
from parallelazy.plugin_manager import PluginManager
from parallelazy.utils import init_debugger


class _LoggingProbePlugin(Plugin):
    def on_start(self):
        self.logger.info("plugin logging probe started")

    def main(self):
        self.wait_for_stop()


class PluginLoggingTests(unittest.TestCase):
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
                self.assertTrue(manager.stop_plugin(descriptor.name))
                self.assertTrue(manager.unload_plugin(descriptor.name))

                plugin_log = os.path.join(
                    manager.run_log_dir,
                    plugin_log_filename(descriptor.name),
                )
                with open(plugin_log, encoding="utf-8") as log_stream:
                    contents = log_stream.read()
                self.assertIn("plugin logging probe started", contents)
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
