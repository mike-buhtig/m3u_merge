#!/usr/bin/env python3
"""
export_groups.py – Extract all unique group-titles per provider
"""

from pathlib import Path
from m3u_merge.fetch import load_config, _cache_filename
from m3u_merge.parse_m3u import read_m3u
import csv

BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "configs" / "providers.yml"
DATA_DIR = BASE / "data"
REPORTS_DIR = DATA_DIR / "reports"
OUT_CSV = REPORTS_DIR / "provider_groups.csv"

cfg = load_config(CONFIG_PATH)

provider_groups = {}

for p in cfg.providers:
    prov_name = p.name.strip() or p.slug
    provider_groups.setdefault(prov_name, set())

    for m3u_url in p.m3u_urls:
        m3u_path = _cache_filename("m3u", p.slug, m3u_url, DATA_DIR)
        if not m3u_path.exists():
            continue

        for ch in read_m3u(m3u_path):
            g = (ch.group_title or "").strip()
            if g:
                provider_groups[prov_name].add(g)

# Make sure reports dir exists
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Write CSV
with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["provider", "group_title"])
    for prov in sorted(provider_groups):
        for g in sorted(provider_groups[prov], key=str.lower):
            w.writerow([prov, g])

print(f"Wrote provider_groups.csv with {sum(len(v) for v in provider_groups.values())} rows")
