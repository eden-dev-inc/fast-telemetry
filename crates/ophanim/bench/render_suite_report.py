#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import html
import pathlib
from typing import Dict, List, Optional


def parse_kv(path: pathlib.Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in path.read_text().splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def read_summary(path: pathlib.Path) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            mode = row["mode"]
            out[mode] = {
                "runs": float(row["runs"]),
                "record_med": float(row["median_record_ops_per_sec"]),
                "total_med": float(row["median_total_ops_per_sec"]),
                "total_min": float(row["min_total_ops_per_sec"]),
                "total_max": float(row["max_total_ops_per_sec"]),
                "export_med_ms": float(row["median_export_avg_ms"]),
            }
    return out


def newest_dirs(results_dir: pathlib.Path, since_epoch: int, prefix: str) -> List[pathlib.Path]:
    dirs = []
    for p in results_dir.glob(f"{prefix}_*"):
        if not p.is_dir():
            continue
        if int(p.stat().st_mtime) >= since_epoch and (p / "summary.csv").exists():
            dirs.append(p)
    return sorted(dirs)


def bar_svg(rows: List[Dict[str, object]], title: str, value_key: str) -> str:
    if not rows:
        return f"<h3>{html.escape(title)}</h3><p>No data</p>"
    max_value = max(float(r[value_key]) for r in rows) or 1.0
    h = 34 * len(rows) + 50
    w = 980
    left = 260
    usable = w - left - 80
    svg = [f"<h3>{html.escape(title)}</h3>", f'<svg viewBox="0 0 {w} {h}" class="chart">']
    y = 28
    for r in rows:
        label = str(r["label"])
        value = float(r[value_key])
        width = (value / max_value) * usable
        svg.append(f'<text x="12" y="{y}" class="label">{html.escape(label)}</text>')
        svg.append(f'<rect x="{left}" y="{y-14}" width="{width:.1f}" height="18" class="bar"/>')
        svg.append(f'<text x="{left + width + 8:.1f}" y="{y}" class="val">{value:,.2f}</text>')
        y += 34
    svg.append("</svg>")
    return "".join(svg)


def speedup_svg(rows: List[Dict[str, object]], title: str, value_key: str) -> str:
    if not rows:
        return f"<h3>{html.escape(title)}</h3><p>No data</p>"
    max_value = max(float(r[value_key]) for r in rows) or 1.0
    h = 34 * len(rows) + 50
    w = 980
    left = 260
    usable = w - left - 100
    svg = [f"<h3>{html.escape(title)}</h3>", f'<svg viewBox="0 0 {w} {h}" class="chart">']
    y = 28
    for r in rows:
        label = str(r["label"])
        value = float(r[value_key])
        width = (value / max_value) * usable
        svg.append(f'<text x="12" y="{y}" class="label">{html.escape(label)}</text>')
        svg.append(f'<rect x="{left}" y="{y-14}" width="{width:.1f}" height="18" class="bar2"/>')
        svg.append(f'<text x="{left + width + 8:.1f}" y="{y}" class="val">{value:.2f}x</text>')
        y += 34
    svg.append("</svg>")
    return "".join(svg)


def dumbbell_svg(rows: List[Dict[str, object]], title: str, fast_key: str, otel_key: str) -> str:
    if not rows:
        return f"<h3>{html.escape(title)}</h3><p>No data</p>"
    points = []
    for r in rows:
        fast = float(r.get(fast_key, 0.0))
        otel = float(r.get(otel_key, 0.0))
        if fast > 0.0 and otel > 0.0:
            points.append((str(r.get("label", "")), fast, otel))
    if not points:
        return f"<h3>{html.escape(title)}</h3><p>No positive-valued points</p>"

    import math
    points = sorted(points, key=lambda p: (p[1] / p[2]) if p[2] else 0.0, reverse=True)
    vals = [v for _, fast, otel in points for v in (fast, otel)]
    min_v, max_v = min(vals), max(vals)
    lo = math.log10(min_v)
    hi = math.log10(max_v)
    if hi == lo:
        hi += 1.0

    w = 980
    h = 36 * len(points) + 100
    left, right, top, bottom = 280, 28, 24, 56
    pw = w - left - right

    def sx(v: float) -> float:
        return left + ((math.log10(v) - lo) / (hi - lo)) * pw

    svg = [f"<h3>{html.escape(title)}</h3>", '<div class="note">Left dot = OTel, right dot = ophanim (log scale).</div>', f'<svg viewBox="0 0 {w} {h}" class="chart">']
    axis_y = h - bottom
    svg.append(f'<line x1="{left}" y1="{axis_y}" x2="{left+pw}" y2="{axis_y}" class="axis"/>')

    tick_exponents = list(range(int(math.floor(lo)), int(math.ceil(hi)) + 1))
    for exp in tick_exponents:
        v = 10**exp
        x = sx(v)
        svg.append(f'<line x1="{x:.2f}" y1="{axis_y}" x2="{x:.2f}" y2="{axis_y+6}" class="axis"/>')
        svg.append(f'<text x="{x-8:.2f}" y="{axis_y+20}" class="tick">1e{exp}</text>')

    y = top + 18
    for label, fast, otel in points:
        x_fast = sx(fast)
        x_otel = sx(otel)
        speedup = fast / otel if otel else 0.0
        svg.append(f'<text x="10" y="{y}" class="label">{html.escape(label)}</text>')
        svg.append(f'<line x1="{x_otel:.2f}" y1="{y-5}" x2="{x_fast:.2f}" y2="{y-5}" class="link"/>')
        svg.append(
            f'<circle cx="{x_otel:.2f}" cy="{y-5}" r="5" class="otel_dot"><title>{html.escape(label)} | otel={otel:,.2f}</title></circle>'
        )
        svg.append(
            f'<circle cx="{x_fast:.2f}" cy="{y-5}" r="5" class="fast_dot"><title>{html.escape(label)} | fast={fast:,.2f}</title></circle>'
        )
        svg.append(f'<text x="{x_fast + 8:.2f}" y="{y}" class="speed">{speedup:.2f}x</text>')
        y += 36

    legend_y = top + 2
    svg.append(f'<circle cx="{left+8}" cy="{legend_y}" r="5" class="otel_dot"/><text x="{left+20}" y="{legend_y+4}" class="legend">otel</text>')
    svg.append(
        f'<circle cx="{left+92}" cy="{legend_y}" r="5" class="fast_dot"/><text x="{left+104}" y="{legend_y+4}" class="legend">ophanim</text>'
    )
    svg.append(f'<text x="{left + pw/2:.1f}" y="{h - 16}" class="axislabel">throughput ops/sec (log10)</text>')
    svg.append("</svg>")
    return "".join(svg)


def render_table(rows: List[Dict[str, object]], headers: List[str], keys: List[str]) -> str:
    out = ["<table><thead><tr>"]
    for h in headers:
        out.append(f"<th>{html.escape(h)}</th>")
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for k in keys:
            v = row.get(k, "")
            if isinstance(v, float):
                if "speedup" in k:
                    cell = f"{v:.2f}x"
                else:
                    cell = f"{v:,.2f}"
            else:
                cell = str(v)
            out.append(f"<td>{html.escape(cell)}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--since-epoch", type=int, required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    results_dir = pathlib.Path(args.results_dir)
    cache_dirs = newest_dirs(results_dir, args.since_epoch, "cache")
    span_dirs = newest_dirs(results_dir, args.since_epoch, "span")

    cache_rows: List[Dict[str, object]] = []
    for d in cache_dirs:
        summary = read_summary(d / "summary.csv")
        sample = next(iter(sorted(d.glob("fast-run-*.txt"))), None)
        meta = parse_kv(sample) if sample else {}
        label = f"{meta.get('entity', 'unknown')}:{meta.get('profile', 'uniform')}"
        fast = summary.get("fast", {}).get("total_med", 0.0)
        otel = summary.get("otel", {}).get("total_med", 0.0)
        atomic = summary.get("atomic", {}).get("total_med", 0.0)
        cache_rows.append(
            {
                "label": label,
                "dir": d.name,
                "fast_total": fast,
                "otel_total": otel,
                "atomic_total": atomic,
                "fast_otel_speedup": (fast / otel) if otel else 0.0,
                "fast_atomic_speedup": (fast / atomic) if atomic else 0.0,
            }
        )

    span_rows: List[Dict[str, object]] = []
    for d in span_dirs:
        summary = read_summary(d / "summary.csv")
        sample = next(iter(sorted(d.glob("fast-run-*.txt"))), None)
        meta = parse_kv(sample) if sample else {}
        scenario = meta.get("scenario", "unknown")
        fast = summary.get("fast", {}).get("total_med", 0.0)
        otel = summary.get("otel", {}).get("total_med", 0.0)
        span_rows.append(
            {
                "label": scenario,
                "dir": d.name,
                "fast_total": fast,
                "otel_total": otel,
                "fast_otel_speedup": (fast / otel) if otel else 0.0,
            }
        )

    now = dt.datetime.now().isoformat(timespec="seconds")
    html_out = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>ophanim Bench Suite Report</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 24px; color: #15212f; background: #f6f8fb; }}
    h1, h2 {{ margin: 0 0 10px 0; }}
    h3 {{ margin: 24px 0 8px 0; }}
    .meta {{ color: #41526a; margin-bottom: 18px; }}
    .card {{ background: white; border: 1px solid #dbe3ef; border-radius: 12px; padding: 16px; margin-bottom: 16px; }}
    .chart {{ width: 100%; height: auto; border: 1px solid #e5ecf5; border-radius: 10px; background: #fcfdff; }}
    .label {{ font-size: 13px; fill: #334a66; }}
    .val {{ font-size: 12px; fill: #0f2740; }}
    .axis {{ stroke: #7f93ad; stroke-width: 1; }}
    .tick {{ font-size: 10px; fill: #556a84; }}
    .axislabel {{ font-size: 12px; fill: #3a4f67; }}
    .link {{ stroke: #9cb0c9; stroke-width: 2; }}
    .fast_dot {{ fill: #2563eb; }}
    .otel_dot {{ fill: #dc2626; }}
    .speed {{ font-size: 11px; fill: #1f3b5b; }}
    .legend {{ font-size: 11px; fill: #334a66; }}
    .note {{ color: #334a66; font-size: 13px; margin: 4px 0 8px 0; }}
    .bar {{ fill: #2563eb; }}
    .bar2 {{ fill: #0ea5a8; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
    th, td {{ border-bottom: 1px solid #e5ecf5; padding: 8px; text-align: left; font-size: 13px; }}
    th {{ background: #f2f6fc; }}
  </style>
</head>
<body>
  <h1>ophanim Bench Suite Report</h1>
  <div class="meta">generated_at={html.escape(now)} | results_dir={html.escape(str(results_dir))}</div>
  <div class="card">
    <h2>Cache Benchmarks</h2>
    {speedup_svg(cache_rows, "fast/otel speedup by case", "fast_otel_speedup")}
    {dumbbell_svg(cache_rows, "fast vs otel throughput by case (dumbbell, log scale)", "fast_total", "otel_total")}
    {bar_svg(cache_rows, "fast median total ops/sec by case", "fast_total")}
    {render_table(
        cache_rows,
        ["case", "run_dir", "fast_total", "otel_total", "atomic_total", "fast/otel", "fast/atomic"],
        ["label", "dir", "fast_total", "otel_total", "atomic_total", "fast_otel_speedup", "fast_atomic_speedup"],
    )}
  </div>
  <div class="card">
    <h2>Span Benchmarks</h2>
    {speedup_svg(span_rows, "fast/otel speedup by scenario", "fast_otel_speedup")}
    {dumbbell_svg(span_rows, "fast vs otel throughput by scenario (dumbbell, log scale)", "fast_total", "otel_total")}
    {bar_svg(span_rows, "fast median total ops/sec by scenario", "fast_total")}
    {render_table(
        span_rows,
        ["scenario", "run_dir", "fast_total", "otel_total", "fast/otel"],
        ["label", "dir", "fast_total", "otel_total", "fast_otel_speedup"],
    )}
  </div>
</body>
</html>
"""

    out = pathlib.Path(args.output)
    out.write_text(html_out)
    print(f"wrote report: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
