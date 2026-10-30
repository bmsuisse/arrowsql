#!/usr/bin/env python3
"""Render benchmark results to benchmarks/RESULTS.md and benchmarks/results/report.html.

    python benchmarks/report.py                       # latest results/*.jsonl
    python benchmarks/report.py results/a.jsonl ...   # merge several runs (later files win per pair)

Stdlib only. Cell = median seconds and speedup vs upstream mssql_python (>1x = faster).
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from common import RESULTS  # noqa: E402
from drivers.registry import SPECS  # noqa: E402

BASELINE = "mssql_python"
CATS = [("read", "Read"), ("write", "Write"), ("latency", "Latency, concurrency and memory")]
PALETTE = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#17becf", "#7f7f7f", "#bcbd22"]


def load(paths: list[Path]):
    header, results = None, {}
    for p in paths:
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r["type"] == "header":
                if header is None:
                    header = r
                else:  # merge driver info from later runs
                    header["drivers"].update({k: v for k, v in r["drivers"].items() if v.get("available")})
            else:
                results[(r["workload"], r["driver"])] = r
    return header, results


def fmt_s(x: float) -> str:
    if x >= 100:
        return f"{x:.0f}s"
    if x >= 1:
        return f"{x:.2f}s"
    if x >= 0.001:
        return f"{x * 1000:.1f}ms" if x < 0.1 else f"{x:.3f}s"
    return f"{x * 1e6:.0f}us"


def order_workloads(results) -> list[str]:
    import workloads  # defines the canonical order

    known = [w.name for w in workloads.WORKLOADS]
    seen = {w for w, _ in results}
    return [w for w in known if w in seen] + sorted(seen - set(known))


def drivers_in(results) -> list[str]:
    present = {d for _, d in results}
    return [d for d in SPECS if d in present]


def cell(results, w, d, md=True):
    r = results.get((w, d))
    if r is None:
        return "n/a"
    if r["status"] != "ok":
        return {"unsupported": "n/a", "error": "ERR", "timeout": "TIMEOUT"}.get(r["status"], r["status"])
    base = results.get((w, BASELINE))
    text = fmt_s(r["median_s"])
    if base and base["status"] == "ok" and d != BASELINE and r["median_s"] > 0:
        sp = base["median_s"] / r["median_s"]
        text += f" ({sp:.1f}x)" if sp >= 0.1 else " (<0.1x)"
    return text


def best(results, w, drivers):
    ok = [(results[(w, d)]["median_s"], d) for d in drivers if (w, d) in results and results[(w, d)]["status"] == "ok"]
    return min(ok) if ok else None


def markdown(header, results) -> str:
    wl = order_workloads(results)
    drivers = drivers_in(results)
    cat_of = {w: next((r["category"] for (ww, _), r in results.items() if ww == w), "read") for w in wl}
    out = ["# Benchmark results", ""]
    load = header["loadavg"]
    out += [
        f"Run `{header['ts']}` on `{header['host']}` ({header['cpus']} cores, {header['mem_available_mb']} MB available), "
        f"git `{header['git']['sha']}` ({header['git']['branch']}), {'quick' if header['quick'] else 'full'} mode.  ",
        f"SQL Server: {header.get('server')}.  ",
        f"Load average at start: {load[0]:.1f} / {load[1]:.1f} / {load[2]:.1f}.",
        "",
    ]
    if header.get("load_warning") or load[0] > 4:
        out += ["> **The box was loaded (load average > 4, other agents and a SQL Server shared its 24 cores and ~4 GB free RAM) "
                "and this is a `--quick` run with reduced row counts. Numbers are indicative only; compare ratios, not absolutes.**", ""]
    elif header["quick"]:
        out += ["> `--quick` run with reduced row counts; use the full run for headline numbers.", ""]
    out += ["Cells are the median wall time over the measured repeats, with the speedup versus upstream `mssql_python` in "
            "parentheses (higher is faster, `1.0x` = same). `n/a` = driver cannot run the workload, `ERR` = it failed "
            "(see the Errors section). Sizes in `--quick` mode: 3M-row datasets run 300k rows, 1M bulk runs 250k, 3M bulk runs 750k.", ""]

    out += ["## Drivers", "", "| driver | version | status |", "|---|---|---|"]
    for d in SPECS:
        info = header["drivers"].get(d)
        if info is None:
            continue
        if info.get("available"):
            out.append(f"| {d} | {info['version']} | ok |")
        else:
            out.append(f"| {d} | | skipped: {info.get('reason')} |")
    out.append("")

    # Headline: fastest driver per category
    out += ["## Headline", "", "| category | workloads | upstream wins | fastest driver (wins) | geomean speedup of rust_port vs upstream |",
            "|---|---|---|---|---|"]
    import math

    for key, title in CATS:
        ws = [w for w in wl if cat_of[w] == key]
        wins: dict[str, int] = {}
        for w in ws:
            b = best(results, w, [d for d in drivers if d != "bcp"])
            if b:
                wins[b[1]] = wins.get(b[1], 0) + 1
        top = ", ".join(f"{d} ({n})" for d, n in sorted(wins.items(), key=lambda kv: -kv[1])[:3])
        ratios = []
        for w in ws:
            a, b = results.get((w, BASELINE)), results.get((w, "rust_port"))
            if a and b and a["status"] == "ok" and b["status"] == "ok":
                ratios.append(a["median_s"] / b["median_s"])
        gm = f"{math.exp(sum(map(math.log, ratios)) / len(ratios)):.2f}x over {len(ratios)}" if ratios else "n/a"
        out.append(f"| {title} | {len(ws)} | {wins.get(BASELINE, 0)} | {top} | {gm} |")
    out += ["", "`bcp` is excluded from the fastest-driver count: it runs inside the server container from local files and is "
            "the speed-of-light reference, not a Python client.", ""]

    for key, title in CATS:
        ws = [w for w in wl if cat_of[w] == key]
        if not ws:
            continue
        cols = [d for d in drivers if any((w, d) in results for w in ws)]
        out += [f"## {title}", "", "| workload | " + " | ".join(cols) + " |", "|---|" + "---|" * len(cols)]
        for w in ws:
            b = best(results, w, cols)
            cells = []
            for d in cols:
                c = cell(results, w, d)
                if b and b[1] == d:
                    c = f"**{c}**"
                cells.append(c)
            out.append(f"| `{w}` | " + " | ".join(cells) + " |")
        out.append("")
        # throughput + RSS for the heavy workloads
        out += [f"<details><summary>{title}: throughput, CPU time and peak RSS</summary>", "",
                "| workload | driver | units/s | MB/s (est. wire) | client CPU s | peak RSS MB |", "|---|---|---|---|---|---|"]
        for w in ws:
            for d in cols:
                r = results.get((w, d))
                if r and r["status"] == "ok":
                    mb = f"{r['mb_per_s']:.0f}" if r.get("mb_per_s") else ""
                    cpu = f"{r['cpu_median_s']:.2f}" if r.get("cpu_median_s") is not None else ""
                    out.append(f"| `{w}` | {d} | {r['units_per_s']:.0f} {r['unit']}/s | {mb} | {cpu} | {r['rss_peak_mb']:.0f} |")
        out += ["", "</details>", ""]

    errs = [(w, d, r) for (w, d), r in sorted(results.items()) if r["status"] in ("error", "timeout")]
    if errs:
        out += ["## Errors", "", "| workload | driver | reason |", "|---|---|---|"]
        for w, d, r in errs:
            out.append(f"| `{w}` | {d} | {html.escape(r.get('reason', '')[:200]).replace('|', '/')} |")
        out.append("")
    out += ["## Notes", "",
            "- Methodology and caveats: see `benchmarks/README.md`. Raw samples: the JSONL next to this file in `benchmarks/results/`.",
            "- MB/s uses the average `DATALENGTH` bytes per row of the dataset (computed once, `benchmarks/results/datasets.json`), "
            "i.e. an estimate of payload size, not TDS bytes on the wire.",
            "- `bcp` times are for `bcp ... queryout/in -n` inside the container; for writes the file is pre-staged outside the timed region.",
            "- Compatibility: `bench_taxi.py` / `compare.py` were folded into `run.py`; the old workloads map to "
            "`read.taxi_100k.*`, `read.narrow_3m.*`, `read.all_3m.*`, `read.point_lookup_x1000`, `write.executemany_10k`, "
            "`latency.connect_close_x50`.", ""]
    return "\n".join(out)


def svg_chart(results, ws, drivers, colors) -> str:
    """One bar group per workload; bars are medians scaled per group so every group stays readable."""
    bar_h, gap, left, width = 12, 14, 250, 760
    plot_w = width - left - 120
    y = 8
    parts = []
    for w in ws:
        rows = [(d, results[(w, d)]["median_s"]) for d in drivers if (w, d) in results and results[(w, d)]["status"] == "ok"]
        if not rows:
            continue
        top = max(v for _, v in rows)
        parts.append(f'<text x="0" y="{y + 11}" class="wl">{html.escape(w)}</text>')
        for i, (d, v) in enumerate(rows):
            bw = max(1.5, plot_w * v / top)
            by = y + i * (bar_h + 2)
            base = results.get((w, BASELINE))
            sp = ""
            if base and base["status"] == "ok" and d != BASELINE and v > 0:
                sp = f"  {base['median_s'] / v:.1f}x"
            parts.append(f'<rect x="{left}" y="{by}" width="{bw:.1f}" height="{bar_h}" fill="{colors[d]}"><title>{d}: {fmt_s(v)}</title></rect>')
            parts.append(f'<text x="{left + bw + 4:.1f}" y="{by + 10}" class="v">{fmt_s(v)}{sp}</text>')
            parts.append(f'<text x="{left - 6}" y="{by + 10}" text-anchor="end" class="d">{d}</text>')
        y += len(rows) * (bar_h + 2) + gap
    return f'<svg viewBox="0 0 {width} {y}" width="100%" role="img">{"".join(parts)}</svg>'


def html_page(header, results) -> str:
    wl = order_workloads(results)
    drivers = drivers_in(results)
    colors = {d: PALETTE[i % len(PALETTE)] for i, d in enumerate(SPECS)}
    cat_of = {w: next((r["category"] for (ww, _), r in results.items() if ww == w), "read") for w in wl}
    legend = "".join(f'<span class="lg"><i style="background:{colors[d]}"></i>{d}</span>' for d in drivers)
    sections = []
    for key, title in CATS:
        ws = [w for w in wl if cat_of[w] == key]
        if ws:
            sections.append(f"<h2>{title}</h2>{svg_chart(results, ws, drivers, colors)}")
    warn = ""
    if header.get("load_warning") or header["loadavg"][0] > 4:
        warn = '<p class="warn">The box was loaded and this is a quick run: numbers are indicative only.</p>'
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>mssql-python-rs benchmarks</title><style>
:root{{--bg:#fff;--fg:#1c1e21;--mut:#666}}
@media(prefers-color-scheme:dark){{:root{{--bg:#16181c;--fg:#e6e6e6;--mut:#9aa0a6}}}}
body{{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:0 auto;max-width:900px;padding:16px}}
h1{{font-size:20px}}h2{{font-size:16px;margin-top:28px}}.mut{{color:var(--mut)}}.warn{{background:#fff3cd;color:#664d03;padding:8px 12px;border-radius:6px}}
.lg{{display:inline-flex;align-items:center;margin:0 12px 4px 0}}.lg i{{width:12px;height:12px;border-radius:2px;margin-right:5px;display:inline-block}}
svg text{{fill:var(--fg);font-size:11px}}svg .wl{{font-weight:600;font-size:12px}}svg .d{{fill:var(--mut)}}
</style></head><body>
<h1>mssql-python-rs benchmarks</h1>
<p class="mut">{html.escape(header['ts'])} on {html.escape(header['host'])}, git {html.escape(header['git']['sha'])}, {'quick' if header['quick'] else 'full'} mode.
Load average {header['loadavg'][0]:.1f}. Bars are median seconds (shorter is faster), scaled within each workload; the trailing number is the speedup vs upstream mssql_python.</p>
{warn}<p>{legend}</p>{''.join(sections)}</body></html>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="*", type=Path)
    ap.add_argument("--md", type=Path, default=HERE / "RESULTS.md")
    ap.add_argument("--html", type=Path, default=RESULTS / "report.html")
    args = ap.parse_args()
    inputs = args.inputs or sorted(RESULTS.glob("*.jsonl"))[-1:]
    if not inputs:
        sys.exit("no results/*.jsonl found; run benchmarks/run.py first")
    header, results = load(inputs)
    args.md.write_text(markdown(header, results) + "\n")
    args.html.parent.mkdir(parents=True, exist_ok=True)
    args.html.write_text(html_page(header, results))
    print(f"wrote {args.md} and {args.html}", file=sys.stderr)


if __name__ == "__main__":
    main()
