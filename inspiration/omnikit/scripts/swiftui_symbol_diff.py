#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

EXTRA_CLANG_INCLUDE_DIRS = [
    Path("/opt/homebrew/opt/notcurses/include"),
    Path("/usr/local/include"),
    Path("/usr/include"),
]


DEFAULT_LOCAL_MODULES = ["OmniUICore", "OmniSwiftUI", "OmniSwiftData", "OmniUI", "OmniSwiftUISymbolExtras"]
NOISE_EXACT = {
    "Body",
    "body",
    "Content",
    "Context",
    "Coordinator",
    "Element",
    "ID",
    "Index",
    "Indices",
    "Iterator",
    "Label",
    "NSViewType",
    "RawValue",
    "SelectionValue",
    "SubSequence",
    "UIViewType",
    "Value",
    "init()",
    "makeCoordinator()",
    "projectedValue",
    "wrappedValue",
}
OPERATOR_RE = re.compile(r"^[!%&*+\-./<=>?^|~]+")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_checked(command: list[str], cwd: Path | None = None, capture_output: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def resolve_toolchain(mode: str) -> tuple[dict | None, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []

    if mode == "off":
        return None, warnings, errors

    if sys.platform != "darwin":
        message = "Swift symbol extraction is only available on macOS."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    if not shutil.which("xcrun") or not shutil.which("swift"):
        message = "`xcrun` and `swift` are required for symbol extraction."
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    extractor = subprocess.run(
        ["xcrun", "--find", "swift-symbolgraph-extract"],
        text=True,
        capture_output=True,
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
        sdk_path = run_checked(["xcrun", "--show-sdk-path"]).stdout.strip()
        target_info = json.loads(run_checked(["swift", "-print-target-info"]).stdout)
        target = target_info["target"]["triple"]
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as exc:
        message = f"Failed to resolve Swift toolchain metadata: {exc}"
        if mode == "required":
            errors.append(message)
        else:
            warnings.append(message)
        return None, warnings, errors

    return {
        "tool": "xcrun swift-symbolgraph-extract",
        "sdk_path": sdk_path,
        "target": target,
    }, warnings, errors


def ensure_local_surface_built(root: Path, build_target: str) -> Path:
    run_checked(["swift", "build", "--target", build_target], cwd=root, capture_output=True)
    return Path(run_checked(["swift", "build", "--show-bin-path"], cwd=root, capture_output=True).stdout.strip())


def extract_titles_for_module(
    module: str,
    output_dir: Path,
    toolchain: dict,
    include_dirs: list[Path] | None = None,
) -> set[str]:
    command = [
        "xcrun",
        "swift-symbolgraph-extract",
        "-module-name",
        module,
        "-minimum-access-level",
        "public",
        "-target",
        toolchain["target"],
        "-sdk",
        toolchain["sdk_path"],
        "-output-dir",
        str(output_dir),
    ]
    for include_dir in include_dirs or []:
        command.extend(["-I", str(include_dir)])
    for include_dir in EXTRA_CLANG_INCLUDE_DIRS:
        if include_dir.exists():
            command.extend(["-Xcc", f"-I{include_dir}"])

    run_checked(command, capture_output=True)

    titles: set[str] = set()
    for json_path in sorted(output_dir.glob("*.json")):
        data = json.loads(json_path.read_text())
        for symbol in data.get("symbols", []):
            title = symbol.get("names", {}).get("title")
            if title:
                titles.add(title)
    return titles


def is_noise_title(title: str) -> bool:
    if title.startswith("$"):
        return True
    if title in NOISE_EXACT:
        return True
    if OPERATOR_RE.match(title):
        return True
    if title.startswith("subscript("):
        return True
    if title.startswith("_" ):
        return True
    return False


def build_report(root: Path, toolchain: dict, local_modules: list[str], build_target: str, sample_limit: int) -> dict:
    bin_path = ensure_local_surface_built(root, build_target)
    modules_dir = bin_path / "Modules"

    with tempfile.TemporaryDirectory(prefix="swiftui-symbol-diff-") as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)
        swiftui_dir = tmp_dir / "SwiftUI"
        swiftui_dir.mkdir()
        swiftui_titles = extract_titles_for_module("SwiftUI", swiftui_dir, toolchain)

        local_titles_by_module: dict[str, set[str]] = {}
        for module in local_modules:
            module_dir = tmp_dir / module
            module_dir.mkdir()
            cnotcurses_build = bin_path / "CNotcurses.build"
            include_dirs = [modules_dir]
            if cnotcurses_build.exists():
                include_dirs.append(cnotcurses_build)
            local_titles_by_module[module] = extract_titles_for_module(module, module_dir, toolchain, include_dirs=include_dirs)

    local_union: set[str] = set()
    title_to_modules: dict[str, list[str]] = defaultdict(list)
    for module, titles in local_titles_by_module.items():
        local_union |= titles
        for title in titles:
            title_to_modules[title].append(module)

    exact_match_titles = sorted(swiftui_titles & local_union)
    missing_titles = sorted(swiftui_titles - local_union)
    local_only_titles = sorted(local_union - swiftui_titles)

    focused_exact_match_titles = [title for title in exact_match_titles if not is_noise_title(title)]
    focused_missing_titles = [title for title in missing_titles if not is_noise_title(title)]
    focused_local_only_titles = [title for title in local_only_titles if not is_noise_title(title)]

    module_counts = [
        {
            "module": module,
            "title_count": len(titles),
            "exact_match_count": len(swiftui_titles & titles),
        }
        for module, titles in local_titles_by_module.items()
    ]

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "baseline": "Raw exact-title diff between the local SwiftUI SDK symbol graph and the non-renderer OmniUI compatibility modules.",
        "swiftui_sdk": {
            "tool": toolchain["tool"],
            "target": toolchain["target"],
            "sdk_path": toolchain["sdk_path"],
            "title_count": len(swiftui_titles),
        },
        "local_surface": {
            "build_target": build_target,
            "modules": module_counts,
            "union_title_count": len(local_union),
            "module_search_path": str(modules_dir),
        },
        "raw": {
            "exact_match_count": len(exact_match_titles),
            "missing_count": len(missing_titles),
            "local_only_count": len(local_only_titles),
            "exact_match_titles": exact_match_titles,
            "missing_titles": missing_titles,
            "local_only_titles": local_only_titles,
        },
        "focused": {
            "exact_match_count": len(focused_exact_match_titles),
            "missing_count": len(focused_missing_titles),
            "local_only_count": len(focused_local_only_titles),
            "exact_match_titles": focused_exact_match_titles,
            "missing_titles": focused_missing_titles,
            "local_only_titles": focused_local_only_titles,
        },
        "samples": {
            "exact_match_titles": focused_exact_match_titles[:sample_limit],
            "missing_titles": focused_missing_titles[:sample_limit],
            "local_only_titles": focused_local_only_titles[:sample_limit],
        },
        "title_to_modules": {
            title: sorted(modules)
            for title, modules in sorted(title_to_modules.items())
        },
    }


def render_markdown(report: dict, sample_limit: int) -> str:
    raw = report["raw"]
    focused = report["focused"]
    sdk = report["swiftui_sdk"]
    local_surface = report["local_surface"]

    def pct(value: int, total: int) -> str:
        return f"{(value / total * 100):.1f}%" if total else "0.0%"

    module_lines = "\n".join(
        f"- `{entry['module']}` — {entry['title_count']} exact exported titles, {entry['exact_match_count']} exact-title overlaps"
        for entry in local_surface["modules"]
    )

    def sample_block(items: list[str]) -> str:
        if not items:
            return "- None"
        return "\n".join(f"- `{item}`" for item in items[:sample_limit])

    return f"""# SwiftUI Non-Renderer Exact Symbol Diff

_Generated from the local Swift toolchain by `scripts/swiftui_symbol_diff.py`._

## Summary

- Baseline: {report['baseline']}
- SwiftUI SDK titles: {sdk['title_count']}
- Local compatibility titles: {local_surface['union_title_count']}
- Raw exact-title overlap: {raw['exact_match_count']} / {sdk['title_count']} ({pct(raw['exact_match_count'], sdk['title_count'])})
- Focused exact-title overlap: {focused['exact_match_count']} / {focused['exact_match_count'] + focused['missing_count']} ({pct(focused['exact_match_count'], focused['exact_match_count'] + focused['missing_count'])})
- Raw missing titles: {raw['missing_count']}
- Raw local-only titles: {raw['local_only_count']}

## Extraction

- SwiftUI SDK source: `{sdk['tool']}`
- Target triple: `{sdk['target']}`
- Build target for local surface: `{local_surface['build_target']}`
- Module search path: `{local_surface['module_search_path']}`

## Local Modules

{module_lines}

## Focused Exact Matches

- Showing the first {min(sample_limit, len(report['samples']['exact_match_titles']))} focused exact-title matches.
- Full exhaustive lists live in `docs/swiftui-non-renderer-symbol-diff.json`.

{sample_block(report['samples']['exact_match_titles'])}

## Focused Missing Titles

- Showing the first {min(sample_limit, len(report['samples']['missing_titles']))} focused exact-title misses.
- Full exhaustive lists live in `docs/swiftui-non-renderer-symbol-diff.json`.

{sample_block(report['samples']['missing_titles'])}

## Focused Local-only Titles

- Showing the first {min(sample_limit, len(report['samples']['local_only_titles']))} focused exact-title locals that do not exist in SwiftUI.
- Full exhaustive lists live in `docs/swiftui-non-renderer-symbol-diff.json`.

{sample_block(report['samples']['local_only_titles'])}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an exact-title SwiftUI symbol diff against the local non-renderer compatibility modules.")
    parser.add_argument("--swiftui-sdk", choices=["auto", "off", "required"], default="required")
    parser.add_argument("--local-modules", nargs="+", default=DEFAULT_LOCAL_MODULES)
    parser.add_argument("--build-target", default="SwiftUICompatibilityHarness")
    parser.add_argument("--sample-limit", type=int, default=200)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write-json", type=Path)
    parser.add_argument("--write-markdown", type=Path)
    parser.add_argument("--markdown", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    toolchain, warnings, errors = resolve_toolchain(args.swiftui_sdk)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    for warning in warnings:
        print(f"NOTE: {warning}", file=sys.stderr)
    if toolchain is None:
        print("ERROR: no Swift toolchain available for extraction", file=sys.stderr)
        return 1

    report = build_report(root, toolchain, args.local_modules, args.build_target, args.sample_limit)

    if args.check or (not args.markdown and args.write_json is None and args.write_markdown is None):
        print(
            "Exact diff OK: "
            f"swiftui_titles={report['swiftui_sdk']['title_count']}, "
            f"local_titles={report['local_surface']['union_title_count']}, "
            f"raw_overlap={report['raw']['exact_match_count']}, "
            f"focused_overlap={report['focused']['exact_match_count']}"
        )

    if args.write_json:
        output_path = args.write_json if args.write_json.is_absolute() else root / args.write_json
        output_path.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Wrote {output_path.relative_to(root)}")

    markdown = render_markdown(report, args.sample_limit)
    if args.markdown:
        print(markdown)
    if args.write_markdown:
        output_path = args.write_markdown if args.write_markdown.is_absolute() else root / args.write_markdown
        output_path.write_text(markdown + "\n")
        print(f"Wrote {output_path.relative_to(root)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
