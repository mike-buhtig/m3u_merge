from __future__ import annotations
import argparse, csv
from pathlib import Path
from typing import List
from .fetch import load_config, _cache_filename
from .parse_m3u import read_m3u
from .parse_epg import scan_epg_counts
from .reconcile import build_suggestions

def cmd_map(config: Path, data_dir: Path, reports_dir: Path) -> Path:
    """
    Read cached M3U+EPG files and write a summary CSV:
      provider, kind (m3u/epg), source_url, cached_file,
      channels_found, programmes_found, tvg_id_count, missing_tvg_id_count, notes
    """
    from .fetch import load_config, _cache_filename
    from .parse_m3u import read_m3u
    from .parse_epg import scan_epg_counts
    import csv

    cfg = load_config(config)
    reports_dir.mkdir(parents=True, exist_ok=True)
    out = reports_dir / "channel_map.csv"

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "provider", "kind", "source_url", "cached_file",
            "channels_found", "programmes_found",
            "tvg_id_count", "missing_tvg_id_count", "notes"
        ])

        for prov in cfg.providers:
            prov_name = prov.name.strip().lower()

            # EPG stats
            for epg_url in prov.epg_urls:
                cached = _cache_filename("epg", prov.slug, epg_url, data_dir)
                ch_count = prog_count = 0
                notes = ""
                if cached.exists():
                    try:
                        res = scan_epg_counts(cached)
                        # res may be a tuple or a small dataclass-like object
                        if isinstance(res, tuple):
                            ch_count, prog_count = res
                        else:
                            ch_count = getattr(res, "channels", getattr(res, "channels_found", 0))
                            prog_count = getattr(res, "programmes", getattr(res, "programmes_found", 0))
                    except Exception as e:
                        notes = f"EPG parse error: {e.__class__.__name__}: {e}"
                        ch_count = prog_count = 0
                w.writerow([
                    prov_name, "epg", epg_url, str(cached),
                    ch_count, prog_count, "", "", notes
                ])

            # M3U stats
            for m3u_url in prov.m3u_urls:
                cached = _cache_filename("m3u", prov.slug, m3u_url, data_dir)
                ch_count = tvg_count = missing_count = 0
                notes = ""
                if cached.exists():
                    try:
                        for ch in read_m3u(cached):
                            ch_count += 1
                            if ch.tvg_id:
                                tvg_count += 1
                            else:
                                missing_count += 1
                    except Exception as e:
                        notes = f"M3U parse error: {e.__class__.__name__}: {e}"
                w.writerow([
                    prov_name, "m3u", m3u_url, str(cached),
                    ch_count, "", tvg_count, missing_count, notes
                ])

    return out

def main():
    # Parent with global flags so both subcommands accept them
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--config", "-c", type=Path, default=Path("configs/providers.yml"))
    parent.add_argument("--data", "-d", type=Path, default=Path("data"))

    ap = argparse.ArgumentParser(prog="m3u-merge", description="m3u_merge toolbox")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp_map = sub.add_parser("map", parents=[parent], help="Read cached sources; write reports/channel_map.csv")
    sp_map.add_argument("--reports", "-r", type=Path, default=Path("data/reports"))

    sp_sug = sub.add_parser("suggest-ids", parents=[parent], help="Suggest tvg-id for channels lacking it")
    sp_sug.add_argument("--reports", "-r", type=Path, default=Path("data/reports"))

    args = ap.parse_args()

    if args.cmd == "map":
        out = cmd_map(args.config, args.data, args.reports)
        print(f"wrote: {out}")
    elif args.cmd == "suggest-ids":
        sug, miss = build_suggestions(args.config, args.data, args.reports)
        print(f"wrote: {args.reports}/suggested_id_map.csv")
        print(f"wrote: {args.reports}/missing_tvgid.csv")

if __name__ == "__main__":
    main()
