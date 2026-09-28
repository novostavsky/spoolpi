"""Keep the docs honest: they must match the code they describe."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from spool import Spool
from spool.config import _SCHEMA, load

ROOT = Path(__file__).resolve().parent.parent
DOCS = [ROOT / "README.md", ROOT / "CHANGELOG.md", *sorted((ROOT / "docs").glob("*.md"))]
SRC_TEXT = "\n".join(p.read_text() for p in (ROOT / "src").rglob("*.py"))


def blocks(text: str, lang: str) -> list[str]:
    return re.findall(rf"```{lang}\n(.*?)```", text, flags=re.DOTALL)


def test_every_config_key_is_documented_in_its_section() -> None:
    doc = (ROOT / "docs" / "configuration.md").read_text()
    sections = dict(
        re.findall(r"^## \[(\w+)\]\n(.*?)(?=^## |\Z)", doc, flags=re.MULTILINE | re.DOTALL)
    )
    assert set(sections) == set(_SCHEMA), "one '## [section]' heading per config section"
    for section, keys in _SCHEMA.items():
        documented = set(re.findall(r"^\| `(\w+)` \|", sections[section], flags=re.MULTILINE))
        assert documented == set(keys), (
            f"[{section}] documented keys differ: {documented ^ set(keys)}"
        )


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_relative_links_resolve(doc: Path) -> None:
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", doc.read_text()):
        if "://" in target:
            continue
        assert (doc.parent / target).exists(), f"{doc.name} links to missing {target}"


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_python_snippets_compile(doc: Path) -> None:
    for code in blocks(doc.read_text(), "python"):
        compile(code, str(doc), "exec")


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_complete_toml_examples_load(doc: Path, tmp_path: Path) -> None:
    for i, text in enumerate(blocks(doc.read_text(), "toml")):
        if "[retention]" not in text or "[sink]" not in text:
            continue  # a fragment, not a whole config
        cfg = tmp_path / f"example{i}.toml"
        cfg.write_text(text)
        load(cfg)


def test_documented_spool_methods_exist() -> None:
    doc = (ROOT / "docs" / "library.md").read_text()
    names = re.findall(r"^\| `(?:Spool\.)?([a-z_]+)\(", doc, flags=re.MULTILINE)
    assert names, "the method table moved?"
    for name in names:
        assert hasattr(Spool, name), f"library.md documents Spool.{name}, which doesn't exist"


def test_documented_log_messages_exist() -> None:
    doc = (ROOT / "docs" / "operations.md").read_text()
    table = doc.split("### Log messages", 1)[1].split("###", 1)[0]
    messages = re.findall(r"^\| `([^`]+)`", table, flags=re.MULTILINE)
    assert len(messages) >= 8
    for message in messages:
        # The first three words that aren't a placeholder ("N" stands for a number)
        # catch a message that was renamed or removed in the code.
        words = [w for w in message.split() if w != "N"][:3]
        assert " ".join(words) in SRC_TEXT, (
            f"operations.md quotes a log message not in the code: {message!r}"
        )
