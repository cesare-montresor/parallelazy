import os
import tempfile
import unittest
from unittest.mock import patch

from parallelazy import Plugin, PluginManager
from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin_data import Queue, QueueDescriptor, QueueReceive, QueueRecive
from parallelazy.plugin_logging import _RecoveringFileHandler, default_runtime_settings
from parallelazy.utils import process_start_spawn


class _NoDocPlugin(Plugin):
    def main(self):
        self.wait_for_stop()

    def check_env_none(self):
        self.env_set("explicit_none", None)
        return {
            "stored_none": self.env_get("explicit_none", default="fallback"),
            "unset": self.env_get("never_set", default="fallback"),
        }


class CoreCorrectnessTests(unittest.TestCase):
    def test_environment_clear_missing_preserves_existing_and_clears_counters(self):
        env = EnvironmentManager()
        env.begin_monitoring()
        env.set("keep", 1)
        env.set("drop", 2)

        env.clear("missing")
        self.assertEqual(env.get("keep"), 1)
        self.assertEqual(env.get("drop"), 2)

        env.clear("drop")
        self.assertNotIn("drop", env)
        self.assertNotIn("drop", env.monitoring_snapshot())
        self.assertIn("keep", env)

        env.clear()
        self.assertEqual(env.get_all(), {})
        self.assertEqual(env.monitoring_snapshot(), {})

    def test_env_get_distinguishes_stored_none_from_unset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                self.assertTrue(
                    manager.load_plugin(_NoDocPlugin.Descriptor("0.1", "test", name="env-none"))
                )
                self.assertTrue(manager.start_plugin("env-none"))

                res = manager.plugin_call("env-none", "check_env_none")
                self.assertIsNone(res["stored_none"])
                self.assertEqual(res["unset"], "fallback")
                self.assertIsNone(manager.env_get("explicit_none", default="fallback"))
                self.assertEqual(manager.env_get("never_set", default="fallback"), "fallback")

    def test_write_logs_false_creates_no_log_dirs_or_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_root = os.path.join(tmpdir, "custom_logs")
            with PluginManager(
                work_dir=tmpdir,
                log_dir=log_root,
                display_logs=False,
                write_logs=False,
            ) as manager:
                self.assertFalse(os.path.exists(log_root))
                self.assertTrue(
                    manager.load_plugin(_NoDocPlugin.Descriptor("0.1", "test", name="nolog"))
                )
                self.assertTrue(manager.start_plugin("nolog"))
                self.assertFalse(os.path.exists(log_root))

    def test_queue_descriptor_str_and_alias(self):
        q = Queue()
        try:
            desc = q.descriptor()
            text = str(desc)
            self.assertIn("send_closed=False", text)
            self.assertIn("receive_closed=False", text)
            self.assertIs(QueueRecive, QueueReceive)
        finally:
            q.close()

    def test_descriptor_empty_docstring_and_subclass_redefinition(self):
        desc = _NoDocPlugin.Descriptor("0.1", "test")
        self.assertEqual(desc.description, "")

        # Redefining a Plugin subclass with the same name must not raise ValueError
        class _RedefPlugin(Plugin):
            pass

        first = _RedefPlugin

        class _RedefPlugin(Plugin):
            pass

        self.assertIs(Plugin._registry["_RedefPlugin"], _RedefPlugin)
        self.assertIsNot(first, _RedefPlugin)

    def test_file_handler_uses_lf_terminator(self):
        self.assertEqual(_RecoveringFileHandler.terminator, "\n")

    def test_process_start_spawn_honors_force(self):
        with patch("parallelazy.utils.mp.get_start_method", return_value="fork"), patch(
            "parallelazy.utils.mp.set_start_method"
        ) as mock_set:
            process_start_spawn(force=True)
            mock_set.assert_called_once_with("spawn", force=True)


if __name__ == "__main__":
    unittest.main()
