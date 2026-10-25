#!/usr/bin/env python3
"""Maintain the SDK support matrix and check upstream version drift.

Local validation is offline. Refresh/drift checks use official GitHub, npm and
Go proxy endpoints through curl, preserving platform TLS verification.
"""
import argparse
import datetime
import difflib
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MATRIX = ROOT / "docs/parity/features.json"
SNAPSHOT = ROOT / "docs/parity/upstream.json"
RENDERED = ROOT / "docs/parity/feature-support.md"
REPO = "https://github.com/hatchet-dev/hatchet"


def read(path):
    return json.loads(path.read_text())


def fetch(url):
    response = subprocess.run(
        ["curl", "--fail", "--silent", "--show-error", "--location", "--max-time", "45",
         "--header", "Accept: application/json", "--header", "User-Agent: hatchet-rust-sdk-parity", url],
        check=True, capture_output=True, text=True,
    )
    return json.loads(response.stdout)


def upstream():
    main = fetch("https://api.github.com/repos/hatchet-dev/hatchet/commits/main")
    npm = fetch("https://registry.npmjs.org/%40hatchet-dev%2Ftypescript-sdk/latest")
    go = fetch("https://proxy.golang.org/github.com/hatchet-dev/hatchet/@latest")
    release = fetch("https://api.github.com/repos/hatchet-dev/hatchet/releases/latest")
    embedded = fetch("https://api.github.com/repos/hatchet-dev/hatchet-embedded/releases/latest")
    embedded_main = fetch("https://api.github.com/repos/hatchet-dev/hatchet-embedded/commits/main")
    return {
        "schema_version": 1,
        "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "main": {"revision": main["sha"], "date": main["commit"]["committer"]["date"], "url": main["html_url"]},
        "typescript": {"version": npm["version"], "registry": "https://registry.npmjs.org/%40hatchet-dev%2Ftypescript-sdk/latest",
                       "tarball": npm["dist"]["tarball"], "integrity": npm["dist"]["integrity"], "git_head": npm.get("gitHead")},
        "go": {"module": "github.com/hatchet-dev/hatchet", "version": go["Version"], "revision": go["Origin"]["Hash"], "date": go["Time"],
               "registry": "https://proxy.golang.org/github.com/hatchet-dev/hatchet/@latest"},
        "github_latest_release": {"version": release["tag_name"], "published_at": release["published_at"], "url": release["html_url"]},
        "embedded": embedded_snapshot(embedded, embedded_main),
    }


def embedded_snapshot(release, main):
    return {
        "version": release["tag_name"], "published_at": release["published_at"], "url": release["html_url"],
        "main": {"revision": main["sha"], "date": main["commit"]["committer"]["date"], "url": main["html_url"]},
        "assets": {asset["name"]: {"digest": asset.get("digest"), "size": asset["size"]} for asset in release["assets"]},
    }


def comparison(snapshot):
    # Inspection notes and check timestamps do not constitute upstream version drift.
    return {key: snapshot[key] for key in ["main", "typescript", "go", "github_latest_release", "embedded"]}


def validate(matrix, snapshot):
    expected = {"typescript": snapshot["typescript"]["version"], "go": snapshot["go"]["version"], "main": snapshot["main"]["revision"]}
    expected.update(embedded=snapshot["embedded"]["version"], embedded_main=snapshot["embedded"]["main"]["revision"])
    assert matrix["reviewed_against"] == expected, "upstream changed: inspect official implementations before updating reviewed_against"

    import re
    capabilities = {}
    for names, version in re.findall(r'((?:Self::\w+\s*\|\s*)*Self::\w+)\s*=>\s*"([0-9.]+)"', (ROOT / "crates/hatchet-sdk/src/engine_version.rs").read_text()):
        for name in re.findall(r'Self::(\w+)', names):
            capabilities[name] = version
    workflow = (ROOT / ".github/workflows/parity.yml").read_text()
    for target in matrix["engine_targets"].values():
        if target.get("ci"):
            if target.get("kind") == "native_embedded":
                assert target["release"] in workflow and target["checksum"] in workflow, "native embedded CI differs from the support matrix"
                if target["release"] == snapshot["embedded"]["version"]:
                    assert "sha256:" + target["checksum"] == snapshot["embedded"]["assets"][target["asset"]]["digest"], "native embedded CI checksum differs from published artifact"
            else:
                assert target["image"] in workflow, "CI engine image differs from the support matrix"
    seen = set()
    for row in matrix["features"]:
        assert row["id"] not in seen, f"duplicate feature {row['id']}"
        seen.add(row["id"])
        assert row["rust"]["status"] in {"supported", "partial", "missing", "not_applicable"}, row["id"]
        for sdk in ["typescript", "go"]:
            assert row[sdk]["status"] in {"supported", "preview", "absent", "deprecated"}, row["id"]
            if row[sdk]["status"] != "absent":
                assert row[sdk].get("source"), f"{row['id']} needs official {sdk} source"
        requirement = row.get("engine_requirement")
        if requirement:
            assert capabilities[requirement["feature"]] == requirement["version"], f"engine floor mismatch: {row['id']}"
        for source in row["rust"].get("sources", []):
            assert (ROOT / source).is_file(), source
        tests = row["rust"].get("tests", [])
        if row["rust"]["status"] == "supported":
            assert tests and row["rust"].get("sources"), f"supported row {row['id']} needs supplied code and tests"
        for test in tests:
            source = ROOT / test["file"]
            assert source.is_file(), str(source)
            assert f"fn {test['name']}(" in source.read_text(), f"test missing: {test['name']}"
            for engine in test.get("engines", []):
                assert engine in matrix["engine_targets"], engine
    return seen


def render(matrix, snapshot):
    lines = ["# Feature support matrix", "", "<!-- Generated by scripts/parity.py render; edit features.json and refresh upstream.json. -->", "",
             f"Checked {snapshot['checked_at']}. Published TypeScript **{snapshot['typescript']['version']}**; published Go module **{snapshot['go']['version']}**.",
             f"Official main source: [{snapshot['main']['revision'][:12]}]({snapshot['main']['url']}). Go release source: [{snapshot['go']['revision'][:12]}]({REPO}/tree/{snapshot['go']['revision']}/sdks/go).", "",
             f"GitHub's latest-release API reports {snapshot['github_latest_release']['version']}; that label is tracked separately from npm/Go publication and main.", "",
             "Supported means the stated Rust contract has supplied code and named tests; it does not imply every official option or every engine release was exercised. Partial rows name the limits. Engine versions beside live tests identify executed proof targets; support floors are separately documented SDK requirements.", "",
             "OpenTelemetry requires the `opentelemetry` Cargo feature (enabled by default) and an application-configured provider/exporter. Local transports do not emulate engine-owned guarantees.", "",
             "| Feature | TS | Go | Rust | Engine / prerequisite | Rust evidence and limits |", "| --- | --- | --- | --- | --- | --- |"]
    embedded = snapshot["embedded"]
    lines.insert(8, f"Embedded sidecar release: [{embedded['version']}]({embedded['url']}); source main [{embedded['main']['revision'][:12]}]({embedded['main']['url']}). It is tracked independently of TS/Go SDK releases and Docker engine images.")
    lines.insert(9, "")
    for row in matrix["features"]:
        sdk_cells = []
        for sdk, revision, prefix in [("typescript", snapshot["main"]["revision"], "sdks/typescript/"), ("go", snapshot["go"]["revision"], "sdks/go/")]:
            status = row[sdk]["status"]
            sdk_cells.append(f"[{status}]({REPO}/blob/{revision}/{prefix}{row[sdk]['source']})" if row[sdk].get("source") else status)
        tests = row["rust"].get("tests", [])
        evidence_links = []
        for test in tests:
            source = (ROOT / test["file"]).read_text()
            line = source[:source.index(f"fn {test['name']}(")].count("\n") + 1
            label = "live " + ", ".join(test["engines"]) if test.get("engines") else "local/protocol"
            evidence_links.append(f"[{label}](../../{test['file']}#L{line} \"{test['name']}\")")
        evidence = "; ".join(evidence_links)
        notes = row.get("notes", "")
        cells = [f"{row['id']}: {row['feature']}", *sdk_cells, row["rust"]["status"], row.get("engine", "—"), evidence + (". " if evidence and notes else "") + notes]
        lines.append("| " + " | ".join(cell.replace("|", "\\|").replace("\n", " ") for cell in cells) + " |")
    lines += ["", "## Keeping references current", "", "Run `python3 scripts/parity.py check-upstream` to detect changed releases or main. Run `refresh` to update the reference snapshot, inspect changed official implementations, then update feature rows and tests before claiming new support. `render --check` validates the matrix and deterministic Markdown without network access. CI checks the local matrix on changes; a separate scheduled/manual job detects upstream drift and uploads the observed versions. It never changes support claims automatically.", "", "The baseline and current-release CI jobs execute the named Rust live suites against exact engine image digests. External official SDK source references establish the compared contract; these jobs do not execute the TS/Go SDK suites or establish all-language behavioral equivalence.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["refresh", "check-upstream", "render"])
    parser.add_argument("--check", action="store_true", help="fail if generated Markdown differs")
    parser.add_argument("--output", type=pathlib.Path, help="save observed upstream versions for CI artifacts")
    args = parser.parse_args()
    if args.command in {"refresh", "check-upstream"}:
        current = upstream()
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(current, indent=2) + "\n")
        if args.command == "refresh":
            SNAPSHOT.write_text(json.dumps(current, indent=2) + "\n")
            print("Updated reference versions; review source changes before changing support claims.")
        elif comparison(current) != comparison(read(SNAPSHOT)):
            print("Upstream changed; review required. Support claims were not updated.")
            print("\n".join(difflib.unified_diff(json.dumps(comparison(read(SNAPSHOT)), indent=2).splitlines(), json.dumps(comparison(current), indent=2).splitlines(), fromfile="recorded", tofile="observed")))
            return 1
        else:
            print("Recorded official versions and main are current.")
    else:
        matrix, snapshot = read(MATRIX), read(SNAPSHOT)
        validate(matrix, snapshot)
        content = render(matrix, snapshot)
        if args.check:
            assert RENDERED.read_text() == content, "matrix Markdown is stale; run scripts/parity.py render"
        else:
            RENDERED.write_text(content)
        print(f"Validated {len(matrix['features'])} feature rows.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Parity check failed: {error}", file=sys.stderr)
        sys.exit(2)
