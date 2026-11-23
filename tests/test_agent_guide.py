import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from parallelazy.__main__ import (
    get_agent_guide,
    get_agent_guide_path,
    install_skill,
)


class TestAgentGuide(unittest.TestCase):
    def test_packaged_skill_readable_and_has_frontmatter(self):
        content = get_agent_guide()
        self.assertTrue(content.startswith("---\n"))
        self.assertIn("name: parallelazy", content)
        self.assertIn("PluginManager", content)

        path = get_agent_guide_path()
        self.assertTrue(path.is_file())
        self.assertEqual(path.name, "SKILL.md")

    def test_install_skill_copies_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = install_skill(tmpdir)
            expected = Path(tmpdir) / "parallelazy" / "SKILL.md"
            self.assertEqual(dest, expected)
            self.assertTrue(dest.is_file())
            self.assertEqual(dest.read_text(encoding="utf-8"), get_agent_guide())

    def test_cli_agent_guide_and_install_skill(self):
        res_guide = subprocess.run(
            [sys.executable, "-m", "parallelazy", "agent-guide"],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(res_guide.stdout, get_agent_guide())

        res_path = subprocess.run(
            [sys.executable, "-m", "parallelazy", "agent-guide", "--path"],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(Path(res_path.stdout.strip()), get_agent_guide_path())

        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "parallelazy",
                    "install-skill",
                    "--target",
                    tmpdir,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            installed = Path(tmpdir) / "parallelazy" / "SKILL.md"
            self.assertTrue(installed.is_file())
            self.assertEqual(installed.read_text(encoding="utf-8"), get_agent_guide())


if __name__ == "__main__":
    unittest.main()
