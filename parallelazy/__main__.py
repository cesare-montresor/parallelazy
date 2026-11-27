"""CLI utilities for parallelazy (agent guide access and skill installation)."""

from __future__ import annotations

import argparse
import sys
from importlib import resources
from pathlib import Path


def _skill_resource():
    return resources.files("parallelazy").joinpath("SKILL.md")


def get_agent_guide() -> str:
    """Return the packaged parallelazy agent guide (SKILL.md) text."""
    return _skill_resource().read_text(encoding="utf-8")


def get_agent_guide_path() -> Path:
    """Return the resolved filesystem path to the packaged SKILL.md."""
    return Path(str(_skill_resource())).resolve()


def install_skill(target_dir: str | Path = Path(".agents") / "skills") -> Path:
    """Copy the packaged SKILL.md into <target_dir>/parallelazy/SKILL.md."""
    dest = Path(target_dir) / "parallelazy" / "SKILL.md"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(get_agent_guide(), encoding="utf-8")
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m parallelazy",
        description="parallelazy CLI utilities",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    guide_parser = subparsers.add_parser(
        "agent-guide",
        help="Print the packaged AI agent usage guide (or its filesystem path).",
    )
    guide_parser.add_argument(
        "--path",
        action="store_true",
        help="Print the filesystem path to the installed SKILL.md instead of its contents.",
    )

    install_parser = subparsers.add_parser(
        "install-skill",
        help="Install the parallelazy Agent Skill (SKILL.md) into a skills directory.",
    )
    install_parser.add_argument(
        "--target",
        default=str(Path(".agents") / "skills"),
        help="Target skills root directory (default: .agents/skills).",
    )

    args = parser.parse_args(argv)

    if args.command == "agent-guide":
        if args.path:
            print(get_agent_guide_path())
        else:
            sys.stdout.write(get_agent_guide())
        return 0

    if args.command == "install-skill":
        dest = install_skill(args.target)
        print(f"Installed parallelazy skill to {dest}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
