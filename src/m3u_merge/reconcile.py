from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

from .fetch import load_config, _cache_filename
from .parse_m3u import read_m3u
from .parse_epg import read_epg_channels

@dataclass
class Suggestion:
    provider: str
    m3u_url: str
    m3u_name: str
    group: Optional[str]
    url: str
    suggested_id: Optional[str]
    reason: str  # "has-tvg-id", "exact-name-match", "ambiguous", "no-match"

def build_suggestions(config_path: Path, data_dir: Path, reports_dir: Path) -> Tuple[Path, Path]:
    """
    Strategy:
      - If tvg-id is present in M3U, use it ("has-tvg-id").
      - Else try EXACT display-name match using (tvg-name if present) else (name), each .strip().
      - If multiple EPG channels share the exact same display-name, mark as "ambiguous".
      - Otherwise, "no-match".
    """
    cfg = load_config(config_path)
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Build EPG lookup per provider: exact display-name (strip) -> unique channel id or None if ambiguous
    epg_lookup: Dict[str, Dict[str, Optional[str]]] = {}
    for p in cfg.providers:
        prov_key = p.name.strip().lower()
        name_map: Dict[str, Optional[str]] = {}
        for epg_url in p.epg_urls:
            epg_path = _cache_filename("epg", p.slug, epg_url, data_dir)
            if not epg_path.exists():
                continue
            channels = read_epg_channels(epg_path)
            for ch in channels.values():
                for disp in (ch.display_names or []):
                    key = (disp or "").strip()
                    if not key:
                        continue
                    if key in name_map and name_map[key] != ch.id:
                        name_map[key] = None  # ambiguous
                    elif key not in name_map:
                        name_map[key] = ch.id
        epg_lookup[prov_key] = name_map

    suggested_csv = reports_dir / "suggested_id_map.csv"
    missing_csv   = reports_dir / "missing_tvgid.csv"

    with suggested_csv.open("w", newline="", encoding="utf-8") as fs, \
         missing_csv.open("w", newline="", encoding="utf-8") as fm:
        ws = csv.writer(fs)
        wm = csv.writer(fm)

        ws.writerow(["provider","m3u_url","m3u_name","group","stream_url","suggested_tvg_id","reason"])
        wm.writerow(["provider","m3u_url","m3u_name","group","stream_url","reason"])

        for p in cfg.providers:
            prov_key = p.name.strip().lower()
            name_map = epg_lookup.get(prov_key, {})

            for m3u_url in p.m3u_urls:
                m3u_path = _cache_filename("m3u", p.slug, m3u_url, data_dir)
                if not m3u_path.exists():
                    continue

                for ch in read_m3u(m3u_path):
                    # prefer tvg-name as the display label for matching; fallback to name
                    m3u_label = (ch.tvg_name or ch.name or "").strip()

                    # If tvg-id exists, we keep it
                    if (ch.tvg_id or "").strip():
                        ws.writerow([prov_key, m3u_url, m3u_label, ch.group_title or "", ch.url, ch.tvg_id.strip(), "has-tvg-id"])
                        continue

                    # Exact display-name match
                    if not m3u_label:
                        wm.writerow([prov_key, m3u_url, "", ch.group_title or "", ch.url, "no-match"])
                        continue

                    epg_id = name_map.get(m3u_label)
                    if epg_id is None:
                        wm.writerow([prov_key, m3u_url, m3u_label, ch.group_title or "", ch.url, "ambiguous"])
                    elif epg_id:
                        ws.writerow([prov_key, m3u_url, m3u_label, ch.group_title or "", ch.url, epg_id, "exact-name-match"])
                    else:
                        wm.writerow([prov_key, m3u_url, m3u_label, ch.group_title or "", ch.url, "no-match"])

    return suggested_csv, missing_csv
