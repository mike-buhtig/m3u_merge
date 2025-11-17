#!/usr/bin/env python3
"""
do_stuff.py  –  end-to-end M3U + EPG update/merge

Usage (from ~/m3u_merge):

    PYTHONPATH=src python3 do_stuff.py
"""

from pathlib import Path
import xml.etree.ElementTree as ET
from collections import Counter

from m3u_merge.fetch import fetch_all, load_config, _cache_filename
from m3u_merge.parse_m3u import read_m3u


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
BASE_DIR     = Path(__file__).resolve().parent
CONFIG_PATH  = BASE_DIR / "configs" / "providers.yml"
DATA_DIR     = BASE_DIR / "data"
OUTPUT_DIR   = Path("/var/www/epg")

MERGED_EPG   = OUTPUT_DIR / "merged_epg.xml"
MERGED_M3U   = OUTPUT_DIR / "merged.m3u"


def step1_fetch():
    print("=== STEP 1: Fetching all providers into cache ===")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fetch_all(CONFIG_PATH, DATA_DIR)
    print("    Done fetch_all().")


def step2_merge_epg():
    print("=== STEP 2: Merging provider EPGs ===")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_config(CONFIG_PATH)
    tv_root = ET.Element("tv")

    epg_channel_count = 0

    for p in cfg.providers:
        slug = p.slug
        for epg_url in p.epg_urls:
            epg_path = _cache_filename("epg", slug, epg_url, DATA_DIR)
            if not epg_path.exists():
                continue

            print(f"  Merging EPG from: {epg_path}")
            tree = ET.parse(epg_path)
            root = tree.getroot()

            # copy only <channel> and <programme> elements
            for elem in root:
                if elem.tag in ("channel", "programme"):
                    tv_root.append(elem)
                    if elem.tag == "channel":
                        epg_channel_count += 1

    tree_out = ET.ElementTree(tv_root)
    tree_out.write(str(MERGED_EPG), encoding="UTF-8", xml_declaration=True)
    print(f"  Wrote merged EPG to {MERGED_EPG}")
    print(f"  Total <channel> elements in merged EPG: {epg_channel_count}")


def step3_merge_m3u():
    print("=== STEP 3: Merging provider M3Us with sorted numbering ===")
    cfg = load_config(CONFIG_PATH)

    # We’ll collect ALL channels first, then sort, then number.
    channels = []

    for p in cfg.providers:
        prov_name = p.name.strip() or p.slug   # e.g. "Samsung", "Pluto", "Plex"
        slug = p.slug

        for m3u_url in p.m3u_urls:
            m3u_path = _cache_filename("m3u", slug, m3u_url, DATA_DIR)
            if not m3u_path.exists():
                continue

            print(f"  Reading M3U from: {m3u_path} ({prov_name})")
            for ch in read_m3u(m3u_path):
                tvg_id      = (ch.tvg_id or "").strip()
                tvg_name    = (ch.tvg_name or ch.name or "").strip()
                display_raw = (ch.name or tvg_name or tvg_id or ch.url).strip()
                group       = (ch.group_title or "").strip()
                url         = ch.url.strip()

                channels.append({
                    "prov_name":  prov_name,
                    "slug":       slug,
                    "tvg_id":     tvg_id,
                    "tvg_name":   tvg_name,
                    "display_raw": display_raw,
                    "group":      group,
                    "url":        url,
                })

    # --- Sort BEFORE numbering ---
    # 1) by display_raw (case-insensitive)
    # 2) then by prov_name, so e.g. "CBS News" (Plex) and "CBS News" (Pluto) group together.
    channels.sort(
        key=lambda c: (c["display_raw"].lower(), c["prov_name"].lower())
    )

    # --- Now assign channel numbers from 100 upwards in that sorted order ---
    lines = []
    lines.append(f'#EXTM3U x-tvg-url="http://epg.mikefarris.biz/{MERGED_EPG.name}"')

    prov_counts = Counter()
    current_chno = 100

    for c in channels:
        prov_name   = c["prov_name"]
        tvg_id      = c["tvg_id"]
        tvg_name    = c["tvg_name"]
        display_raw = c["display_raw"]
        group       = c["group"]
        url         = c["url"]

        visible_name = f"{display_raw} ({prov_name})"
        prov_counts[prov_name] += 1

        chno = current_chno
        current_chno += 1

        # We IGNORE any upstream tvg-chno, and only set our own.
        extinf = '#EXTINF:-1'
        if tvg_id:
            extinf += f' tvg-id="{tvg_id}"'
        if tvg_name:
            extinf += f' tvg-name="{tvg_name}"'
        if group:
            extinf += f' group-title="{group}"'
        extinf += f' tvg-chno="{chno}"'
        extinf += f',{visible_name}'

        lines.append(extinf)
        lines.append(url)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with MERGED_M3U.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")

    print(f"  Wrote merged M3U with provider tags to {MERGED_M3U}")
    print("  Per-provider channel counts:")
    for name, count in sorted(prov_counts.items()):
        print(f"    {name}: {count}")
    print(f"  Total channels written: {sum(prov_counts.values())}")
    print(f"  Final channel number used: {current_chno - 1}")


def main():
    step1_fetch()
    step2_merge_epg()
    step3_merge_m3u()
    print("=== All done. Load merged.m3u / merged_epg.xml in NextPVR. ===")


if __name__ == "__main__":
    main()
