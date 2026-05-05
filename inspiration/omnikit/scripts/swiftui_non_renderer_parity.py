#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path


ALLOWED_STATUS = {"supported", "partial", "compile-only", "missing", "unknown"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_manifest(path: Path) -> dict:
    return json.loads(path.read_text())


def gather_source_texts(root: Path, source_roots: list[str]) -> list[tuple[Path, str]]:
    texts: list[tuple[Path, str]] = []
    for rel_root in source_roots:
        base = root / rel_root
        if not base.is_dir():
            continue
        for file_path in sorted(base.rglob("*.swift")):
            texts.append((file_path, file_path.read_text()))
    return texts


def validate_manifest(manifest: dict, root: Path) -> list[str]:
    errors: list[str] = []

    for key in (
        "title",
        "summary",
        "baseline",
        "scope",
        "taxonomy",
        "source_roots",
        "domains",
        "roadmap",
        "commands",
    ):
        if key not in manifest:
            errors.append(f"Missing top-level key: {key}")

    source_roots = manifest.get("source_roots", [])
    for rel_root in source_roots:
        if not (root / rel_root).is_dir():
            errors.append(f"Source root not found: {rel_root}")

    source_texts = gather_source_texts(root, source_roots)
    domain_ids: set[str] = set()

    for domain in manifest.get("domains", []):
        domain_id = domain.get("id")
        title = domain.get("title", domain_id or "<unknown>")
        if not domain_id:
            errors.append(f"Domain missing id: {title}")
            continue
        if domain_id in domain_ids:
            errors.append(f"Duplicate domain id: {domain_id}")
        domain_ids.add(domain_id)

        for status_key in ("api_status", "behavior_status"):
            status = domain.get(status_key)
            if status not in ALLOWED_STATUS:
                errors.append(f"{domain_id}: invalid {status_key} {status!r}")

        for key in (
            "summary",
            "implemented_examples",
            "compile_only_examples",
            "missing_examples",
            "evidence",
            "missing_patterns",
            "swiftui_reference_patterns",
        ):
            if key not in domain:
                errors.append(f"{domain_id}: missing key {key}")

        for evidence in domain.get("evidence", []):
            path = evidence.get("path")
            if not path:
                errors.append(f"{domain_id}: evidence entry missing path")
                continue
            file_path = root / path
            if not file_path.is_file():
                errors.append(f"{domain_id}: evidence file not found: {path}")
                continue
            text = file_path.read_text()
            for pattern in evidence.get("patterns", []):
                if pattern not in text:
                    errors.append(f"{domain_id}: missing pattern {pattern!r} in {path}")

        for pattern in domain.get("missing_patterns", []):
            for file_path, text in source_texts:
                if pattern in text:
                    rel = file_path.relative_to(root)
                    errors.append(f"{domain_id}: missing pattern {pattern!r} unexpectedly found in {rel}")
                    break

    seen_waves: set[int] = set()
    for wave in manifest.get("roadmap", []):
        wave_number = wave.get("wave")
        if not isinstance(wave_number, int):
            errors.append(f"Roadmap wave missing numeric id: {wave}")
            continue
        if wave_number in seen_waves:
            errors.append(f"Duplicate roadmap wave: {wave_number}")
        seen_waves.add(wave_number)

        for domain_id in wave.get("domains", []):
            if domain_id not in domain_ids:
                errors.append(f"Wave {wave_number}: unknown domain id {domain_id}")

    for command in manifest.get("commands", []):
        if "label" not in command or "command" not in command:
            errors.append(f"Command entry missing label or command: {command}")

    return errors


def match_swiftui_title(pattern: str, titles: set[str]) -> str | None:
    if pattern in titles:
        return pattern

    if pattern[:1].islower() or any(token in pattern for token in ("(", ":", ".", "_")):
        return next((title for title in titles if pattern in title), None)

    return next(
        (
            title
            for title in titles
            if title.startswith(f"{pattern}(") or title.startswith(f"{pattern}.")
        ),
        None,
    )


def extract_swiftui_sdk_reference(mode: str) -> tuple[dict | None, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []

    if mode == "off":
        return None, warnings, errors

    if sys.platform != "darwin":
        message = "SwiftUI SDK extraction is only available on macOS."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    if not shutil.which("xcrun") or not shutil.which("swift"):
        message = "`xcrun` and `swift` are required for SwiftUI SDK extraction."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    extractor = subprocess.run(
        ["xcrun", "--find", "swift-symbolgraph-extract"],
        capture_output=True,
        text=True,
        check=False,
    )
    if extractor.returncode != 0 or not extractor.stdout.strip():
        message = "`swift-symbolgraph-extract` is not available in the active Xcode toolchain."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    try:
        sdk_path = subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        target_info = json.loads(subprocess.check_output(["swift", "-print-target-info"], text=True))
        target = target_info["target"]["unversionedTriple"]
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
        message = f"Failed to resolve Swift toolchain metadata: {exc}"
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    with tempfile.TemporaryDirectory(prefix="swiftui-symbolgraph-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        proc = subprocess.run(
            [
                "xcrun",
                "swift-symbolgraph-extract",
                "-module-name",
                "SwiftUI",
                "-target",
                target,
                "-sdk",
                sdk_path,
                "-output-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            message = proc.stderr.strip() or "SwiftUI symbol graph extraction failed."
            if mode == "required":
                errors.append(message)
            else:
                warnings.append(message)
            return None, warnings, errors

        titles: set[str] = set()
        files = sorted(tmp_path.glob("SwiftUI*.json"))
        for file_path in files:
            data = json.loads(file_path.read_text())
            for symbol in data.get("symbols", []):
                title = symbol.get("names", {}).get("title")
                if title:
                    titles.add(title)

    if not titles:
        message = "SwiftUI symbol graph extraction produced no symbol titles."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    return {
        "tool": "xcrun swift-symbolgraph-extract",
        "module": "SwiftUI",
        "target": target,
        "sdk_path": sdk_path,
        "file_count": len(files),
        "symbol_count": len(titles),
        "titles": titles,
    }, warnings, errors


def validate_swiftui_sdk_patterns(manifest: dict, sdk_info: dict | None) -> list[str]:
    if sdk_info is None:
        return []

    errors: list[str] = []
    titles = sdk_info["titles"]
    for domain in manifest["domains"]:
        domain_id = domain["id"]
        for pattern in domain.get("swiftui_reference_patterns", []):
            if match_swiftui_title(pattern, titles) is None:
                errors.append(f"{domain_id}: SwiftUI SDK reference pattern not found: {pattern!r}")
    return errors


def markdown_list(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def render_markdown(manifest: dict, sdk_info: dict | None, sdk_warnings: list[str]) -> str:
    api_counts = Counter(domain["api_status"] for domain in manifest["domains"])
    behavior_counts = Counter(domain["behavior_status"] for domain in manifest["domains"])
    status_order = ["supported", "partial", "compile-only", "missing", "unknown"]

    taxonomy_lines = [
        f"- `{status}` — {manifest['taxonomy'][status]}"
        for status in status_order
        if status in manifest["taxonomy"]
    ]
    included = "\n".join(f"- `{module}`" for module in manifest["scope"]["included_modules"])
    supporting = "\n".join(f"- `{target}`" for target in manifest["scope"]["supporting_targets"])
    excluded = "\n".join(
        f"- `{item['name']}` — {item['reason']}"
        for item in manifest["scope"]["excluded_modules"]
    )
    domain_rows = [
        f"| {domain['title']} | `{domain['api_status']}` | `{domain['behavior_status']}` |"
        for domain in manifest["domains"]
    ]

    domain_sections: list[str] = []
    for domain in manifest["domains"]:
        evidence_lines = "\n".join(
            f"- `{evidence['path']}` — {evidence['notes']}"
            for evidence in domain["evidence"]
        )
        sdk_lines = ""
        if domain.get("swiftui_reference_patterns"):
            sdk_lines = (
                "\n**SwiftUI SDK reference patterns**\n"
                + markdown_list([f"`{pattern}`" for pattern in domain["swiftui_reference_patterns"]])
                + "\n"
            )
        section = f"""### {domain['title']}

- API status: `{domain['api_status']}`
- Behavior status: `{domain['behavior_status']}`
- Summary: {domain['summary']}

**Implemented / verified**
{markdown_list(domain['implemented_examples'])}

**Compile-only / approximated**
{markdown_list(domain['compile_only_examples'])}

**Representative missing APIs**
{markdown_list(domain['missing_examples'])}
{sdk_lines}
**Evidence**
{evidence_lines}
"""
        domain_sections.append(section.strip())

    roadmap_sections: list[str] = []
    domain_name_by_id = {domain["id"]: domain["title"] for domain in manifest["domains"]}
    for wave in manifest["roadmap"]:
        domains = ", ".join(f"`{domain_name_by_id[domain_id]}`" for domain_id in wave["domains"])
        section = f"""### Wave {wave['wave']} — {wave['title']}

- Focus domains: {domains}

**Goals**
{markdown_list(wave['goals'])}

**Acceptance**
{markdown_list(wave['acceptance'])}

**Verification**
{markdown_list(wave['verification'])}
"""
        roadmap_sections.append(section.strip())

    command_lines = "\n".join(
        f"- `{command['command']}` — {command['label']}"
        for command in manifest["commands"]
    )

    api_summary = ", ".join(f"`{status}`: {api_counts.get(status, 0)}" for status in status_order)
    behavior_summary = ", ".join(f"`{status}`: {behavior_counts.get(status, 0)}" for status in status_order)

    sdk_section = ""
    if sdk_info is not None:
        sdk_section = f"""## SwiftUI SDK Baseline

- Source: local `{sdk_info['tool']}` extraction of `{sdk_info['module']}`
- Extracted symbol graph files: {sdk_info['file_count']}
- Unique symbol titles: {sdk_info['symbol_count']}
- Target triple: `{sdk_info['target']}`
"""
    elif sdk_warnings:
        sdk_section = f"""## SwiftUI SDK Baseline

- Local SwiftUI SDK extraction was unavailable for this run.
{markdown_list(sdk_warnings)}
"""

    summary_lines = [
        f"- Baseline: {manifest['baseline']}",
        "- Scope: non-renderer SwiftUI compatibility stack only",
        f"- API status counts: {api_summary}",
        f"- Behavior status counts: {behavior_summary}",
    ]
    if sdk_info is not None:
        summary_lines.append(
            f"- SwiftUI SDK reference: validated representative patterns against {sdk_info['symbol_count']} extracted symbol titles"
        )

    summary_block = "\n".join(summary_lines)
    taxonomy_block = "\n".join(taxonomy_lines)
    domain_table = "\n".join(domain_rows)
    domain_block = "\n\n".join(domain_sections)
    roadmap_block = "\n\n".join(roadmap_sections)

    return f"""# {manifest['title']}

_Generated from `docs/swiftui-non-renderer-parity.json` by `scripts/swiftui_non_renderer_parity.py`._

## Summary

{summary_block}

## Scope

**Included modules**
{included}

**Supporting targets used as evidence**
{supporting}

**Excluded from this audit**
{excluded}

{sdk_section}## Taxonomy

{taxonomy_block}

## Current Status

| Domain | API | Behavior |
| --- | --- | --- |
{domain_table}

## Domain Breakdown

{domain_block}

## Implementation Roadmap

{roadmap_block}

## Repeat the Audit

{command_lines}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and render the SwiftUI non-renderer parity manifest.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("docs/swiftui-non-renderer-parity.json"),
        help="Path to the parity manifest JSON file.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the manifest and print a summary.",
    )
    parser.add_argument(
        "--swiftui-sdk",
        choices=["auto", "off", "required"],
        default="auto",
        help="Validate representative SwiftUI patterns against the local SDK symbol graph.",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Print the rendered Markdown report to stdout.",
    )
    parser.add_argument(
        "--write-markdown",
        type=Path,
        help="Write the rendered Markdown report to the given path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    manifest_path = args.manifest if args.manifest.is_absolute() else root / args.manifest
    manifest = load_manifest(manifest_path)

    errors = validate_manifest(manifest, root)
    sdk_info: dict | None = None
    sdk_warnings: list[str] = []
    if not errors:
        sdk_info, sdk_warnings, sdk_errors = extract_swiftui_sdk_reference(args.swiftui_sdk)
        errors.extend(sdk_errors)
        errors.extend(validate_swiftui_sdk_patterns(manifest, sdk_info))

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    for warning in sdk_warnings:
        print(f"NOTE: {warning}", file=sys.stderr)

    if args.check or (not args.markdown and args.write_markdown is None):
        sdk_summary = "disabled"
        if sdk_info is not None:
            pattern_count = sum(len(domain.get("swiftui_reference_patterns", [])) for domain in manifest["domains"])
            sdk_summary = f"validated {pattern_count} representative patterns against {sdk_info['symbol_count']} SwiftUI symbols"
        elif sdk_warnings:
            sdk_summary = "unavailable"
        print(
            f"Manifest OK: {len(manifest['domains'])} domains, {len(manifest['roadmap'])} roadmap waves, "
            f"baseline={manifest['baseline']}, swiftui_sdk={sdk_summary}"
        )

    if args.markdown or args.write_markdown:
        markdown = render_markdown(manifest, sdk_info, sdk_warnings)
        if args.markdown:
            print(markdown)
        if args.write_markdown:
            output_path = args.write_markdown if args.write_markdown.is_absolute() else root / args.write_markdown
            output_path.write_text(markdown + "\n")
            print(f"Wrote {output_path.relative_to(root)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
