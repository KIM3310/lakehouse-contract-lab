"""Regression checks for the static publication, independent of Spark."""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"


class Page(HTMLParser):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.references: list[str] = []
        self.elements: dict[str, dict[str, str | None]] = {}
        self.text: list[str] = []
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        identifier = attributes.get("id")
        if identifier:
            self.elements[identifier] = attributes
        for name in ("href", "src"):
            if attributes.get(name):
                self.references.append(str(attributes[name]))

    def handle_data(self, data: str) -> None:
        self.text.append(data)


@pytest.mark.parametrize("page_path", sorted(SITE.glob("*.html")), ids=lambda path: path.name)
def test_static_links_work_at_root_and_project_subpath(page_path: Path) -> None:
    page = Page(page_path)
    for mount in ("/", "/lakehouse-contract-lab/"):
        base = f"https://static.example{mount}"
        for reference in page.references:
            if urlsplit(reference).scheme or reference.startswith("//"):
                continue
            resolved = urlsplit(urljoin(base + page_path.name, reference))
            assert resolved.path.startswith(mount), (page_path.name, reference, resolved.path)
            relative = unquote(resolved.path.removeprefix(mount))
            target = SITE / relative
            assert target.is_file(), (page_path.name, reference, relative)
            if resolved.fragment and target.suffix == ".html":
                assert resolved.fragment in Page(target).elements, (page_path.name, reference)


def test_scenarios_link_to_existing_synthetic_evidence() -> None:
    source = (SITE / "index.html").read_text(encoding="utf-8")
    match = re.search(r"const scenarios = (\[.*?\]);", source, re.DOTALL)
    assert match is not None
    scenarios = json.loads(match.group(1))
    assert [scenario["evidencePath"] for scenario in scenarios] == [
        "artifacts/lakehouse-proof-pack.json",
        "artifacts/quality-report.json",
        "artifacts/gold-preview.json",
    ]
    for scenario in scenarios:
        assert (ROOT / scenario["evidencePath"]).is_file()
        assert scenario["evidenceLabel"]
    quality = json.loads((ROOT / "artifacts/quality-report.json").read_text(encoding="utf-8"))
    gold = json.loads((ROOT / "artifacts/gold-preview.json").read_text(encoding="utf-8"))
    assert scenarios[0]["metric"] == "12 source, 8 accepted"
    assert scenarios[1]["metric"] == f"{quality['summary']['failedRows']} rejected rows"
    assert scenarios[2]["metric"] == f"{len(gold['rows'])} regional KPI rows"


def test_home_announces_static_evidence_instead_of_live_service() -> None:
    page = Page(SITE / "index.html")
    text = " ".join(page.text)
    assert "Static synthetic example" in text
    assert "This page does not run Spark or query a live API." in text
    assert "Live proof surface" not in text
    assert page.elements["scenario-result"]["role"] == "status"
    assert page.elements["scenario-result"]["aria-atomic"] == "true"
    assert page.elements["scenario-evidence"]["href"] == (
        "https://github.com/KIM3310/lakehouse-contract-lab/blob/main/artifacts/lakehouse-proof-pack.json"
    )


def test_missing_route_page_explains_the_static_boundary() -> None:
    page = Page(SITE / "404.html")
    text = " ".join(page.text)
    assert "Page not found" in text
    assert "This static site does not host pipeline APIs." in text
    assert "https://lakehouse-contract-lab.pages.dev/" in page.references


def test_snapshot_instructions_require_explicit_opt_in() -> None:
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    verification = " ".join(Page(SITE / "verification.html").text)
    for text in (contributing, verification):
        assert "LAKEHOUSE_VALIDATE_PREBUILT_ONLY=1" in text
        assert "snapshot-only" in text
        assert "Java 17" in text
    assert "On machines without Java, the build script validates" not in contributing
