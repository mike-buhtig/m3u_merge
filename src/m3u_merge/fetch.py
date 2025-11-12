from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yaml


@dataclass
class FetchSettings:
    timeout_secs: int = 20
    retries: int = 3
    backoff: float = 1.5
    honor_etag: bool = True
    honor_last_modified: bool = True


@dataclass
class Provider:
    name: str
    m3u_urls: List[str]
    epg_urls: List[str]
    headers: Dict[str, str]
    auth: Optional[Dict[str, str]]  # {type: 'basic', user: '...', pass: '...'}

    @property
    def slug(self) -> str:
        s = self.name.strip().lower()
        return "".join(c if c.isalnum() or c in "-._" else "-" for c in s)


@dataclass
class ProvidersConfig:
    providers: List[Provider]
    fetch: FetchSettings


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_path: Path) -> ProvidersConfig:
    raw = _load_yaml(config_path)

    fetch = raw.get("fetch", {}) or {}
    settings = FetchSettings(
        timeout_secs=int(fetch.get("timeout_secs", 20)),
        retries=int(fetch.get("retries", 3)),
        backoff=float(fetch.get("backoff", 1.5)),
        honor_etag=bool(fetch.get("honor_etag", True)),
        honor_last_modified=bool(fetch.get("honor_last_modified", True)),
    )

    provs: List[Provider] = []
    for p in raw.get("providers", []):
        provs.append(
            Provider(
                name=str(p.get("name", "")).strip(),
                m3u_urls=[str(u) for u in (p.get("m3u_urls") or [])],
                epg_urls=[str(u) for u in (p.get("epg_urls") or [])],
                headers=dict(p.get("headers") or {}),
                auth=p.get("auth"),
            )
        )

    return ProvidersConfig(providers=provs, fetch=settings)


def _hash_url(url: str) -> str:
    import hashlib
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def _meta_path(dst: Path) -> Path:
    return dst.with_suffix(dst.suffix + ".meta.json")


def _read_meta(meta_file: Path) -> dict:
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _write_meta(meta_file: Path, meta: dict) -> None:
    meta_file.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")


def _auth_tuple(auth: Optional[Dict[str, str]]):
    if not auth:
        return None
    if auth.get("type", "").lower() == "basic":
        return (auth.get("user", ""), auth.get("pass", ""))
    return None


def _conditional_headers(existing_meta: dict, settings: FetchSettings) -> Dict[str, str]:
    h = {}
    if settings.honor_etag and (etag := existing_meta.get("etag")):
        h["If-None-Match"] = etag
    if settings.honor_last_modified and (lm := existing_meta.get("last_modified")):
        h["If-Modified-Since"] = lm
    return h


def _save_response(dst: Path, resp: requests.Response) -> tuple[Path, dict]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as f:
        f.write(resp.content)

    meta = {
        "url": resp.url,
        "status": resp.status_code,
        "size": len(resp.content),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "content_type": resp.headers.get("Content-Type"),
    }
    _write_meta(_meta_path(dst), meta)
    return dst, meta


def _cache_filename(kind: str, provider_slug: str, url: str, data_dir: Path) -> Path:
    h = _hash_url(url)
    ext = ".m3u" if kind == "m3u" else ".xml"
    return data_dir / "cache" / provider_slug / f"{kind}-{h}{ext}"


def _http_date_to_utc_iso(http_date: Optional[str]) -> Optional[str]:
    if not http_date:
        return None
    try:
        return parsedate_to_datetime(http_date).astimezone(tz=None).isoformat()
    except Exception:
        return None


def fetch_url(
    url: str,
    dst: Path,
    headers: Dict[str, str],
    auth,
    settings: FetchSettings,
    logger: logging.Logger,
):
    meta_file = _meta_path(dst)
    existing_meta = _read_meta(meta_file)
    req_headers = dict(headers or {})
    req_headers.update(_conditional_headers(existing_meta, settings))

    attempt = 0
    last_exc = None
    while attempt <= settings.retries:
        try:
            resp = requests.get(url, headers=req_headers, auth=auth, timeout=settings.timeout_secs)
            if resp.status_code == 304:
                if dst.exists():
                    logger.info("304 Not Modified: %s", url)
                    existing_meta["status"] = 304
                    existing_meta.setdefault("not_modified_at", datetime.utcnow().isoformat() + "Z")
                    return dst, existing_meta
                else:
                    logger.warning("304 but no cache for %s; retrying without conditionals.", url)
                    req_headers.pop("If-None-Match", None)
                    req_headers.pop("If-Modified-Since", None)
                    attempt += 1
                    continue

            if 200 <= resp.status_code < 300:
                saved_path, meta = _save_response(dst, resp)
                meta["last_modified_iso"] = _http_date_to_utc_iso(meta.get("last_modified"))
                logger.info("200 OK (%s bytes): %s", meta["size"], url)
                return saved_path, meta

            logger.warning("HTTP %s for %s", resp.status_code, url)

        except requests.RequestException as e:
            last_exc = e
            logger.warning("Network error on %s (attempt %d/%d): %s", url, attempt + 1, settings.retries, e)

        attempt += 1
        if attempt <= settings.retries:
            sleep_s = settings.backoff ** attempt
            time.sleep(sleep_s)

    if dst.exists():
        logger.error("Failed to refresh %s, using cached file: %s", url, dst)
        cached_meta = _read_meta(meta_file)
        cached_meta["warning"] = f"fetch failed after {settings.retries} retries"
        return dst, cached_meta

    logger.error("Fetch failed and no cache exists for %s. Last error: %s", url, last_exc)
    return None, {"error": str(last_exc) if last_exc else "fetch failed"}


def fetch_provider(provider: Provider, settings: FetchSettings, data_dir: Path, logger: logging.Logger) -> dict:
    headers = dict(provider.headers or {})
    auth_tuple = _auth_tuple(provider.auth)

    results = {
        "provider": provider.name,
        "slug": provider.slug,
        "m3u": [],
        "epg": [],
    }

    for url in provider.m3u_urls:
        dst = _cache_filename("m3u", provider.slug, url, data_dir)
        path, meta = fetch_url(url, dst, headers, auth_tuple, settings, logger)
        results["m3u"].append({"url": url, "path": str(path) if path else None, "meta": meta})

    for url in provider.epg_urls:
        dst = _cache_filename("epg", provider.slug, url, data_dir)
        path, meta = fetch_url(url, dst, headers, auth_tuple, settings, logger)
        results["epg"].append({"url": url, "path": str(path) if path else None, "meta": meta})

    return results


def fetch_all(config_path: Path, data_dir: Path, verbose: bool = False):
    logger = logging.getLogger("m3u_merge.fetch")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    if not logger.handlers:
        logger.addHandler(ch)

    cfg = load_config(config_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "cache").mkdir(parents=True, exist_ok=True)

    summaries = []
    for p in cfg.providers:
        if not p.name:
            logger.warning("Skipping provider with no name.")
            continue
        summaries.append(fetch_provider(p, cfg.fetch, data_dir, logger))

    summary_path = data_dir / "cache" / "fetch-summary.json"
    summary_path.write_text(json.dumps(summaries, indent=2, ensure_ascii=False), encoding="utf-8")
    return summaries


def _parse_args():
    ap = argparse.ArgumentParser(description="Fetch M3U/XMLTV sources into cache with ETag/If-Modified-Since.")
    ap.add_argument("--config", "-c", type=Path, default=Path("configs/providers.yml"))
    ap.add_argument("--data", "-d", type=Path, default=Path("data"))
    ap.add_argument("--verbose", "-v", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    fetch_all(args.config, args.data, verbose=args.verbose)


if __name__ == "__main__":
    main()
