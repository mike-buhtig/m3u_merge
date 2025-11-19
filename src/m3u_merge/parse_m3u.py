from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional


@dataclass
class M3UChannel:
    name: str
    url: str
    tvg_id: Optional[str] = None
    tvg_name: Optional[str] = None
    group_title: Optional[str] = None
    raw_attrs: Optional[Dict[str, str]] = None


def _parse_extinf_attrs(attr_str: str) -> Dict[str, str]:
    """
    Parse key="value" pairs from the EXTINF attribute section.
    This is intentionally simple but works for:
      tvg-id="..."
      channel-id="..."
      tvg-name="..."
      tvc-guide-title="..."
      group-title="..."
    """
    attrs: Dict[str, str] = {}
    key: Optional[str] = None
    buf: list[str] = []
    in_quotes = False

    i = 0
    while i < len(attr_str):
        ch = attr_str[i]
        if ch == '"':
            in_quotes = not in_quotes
            if not in_quotes and key is not None:
                attrs[key] = "".join(buf)
                key = None
                buf = []
            i += 1
            continue

        if not in_quotes and ch.isspace():
            i += 1
            continue

        if not in_quotes and ch == "=" and key is None:
            # what we have in buf so far is the key
            key = "".join(buf)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    return attrs


def _normalize_group_title(group: Optional[str]) -> Optional[str]:
    """
    Normalize provider group titles:

      - Trim whitespace
      - Fix common UTF-8 -> Latin-1 mojibake ("En EspaÃ±ol" -> "En Español")
      - Normalize " X + Y " to " X & Y " (but leave 'Anime+' etc. alone)
      - Collapse multiple spaces

    We *do not* currently:
      - Split on ';' (e.g. 'Black Entertainment;Pop Culture') – you explicitly
        asked to keep this as-is for now.
    """
    if not group:
        return None

    g = group.strip()
    if not g:
        return None

    # --- Fix mojibake like "EspaÃ±ol" -> "Español" when it really is
    # UTF-8 bytes mis-decoded as latin1.
    #
    # We only even try this when we see the 'Ã' artifact, to avoid
    # messing with already-correct text.
    if "Ã" in g:
        try:
            # Typical mojibake path: original UTF-8 bytes were interpreted
            # as Latin-1 and turned into "Ã±", etc. Re-encode as Latin-1
            # and decode as UTF-8 to reverse that.
            fixed = g.encode("latin-1").decode("utf-8")

            # Only accept it if it actually removed the 'Ã' garbage.
            if "Ã" not in fixed:
                g = fixed
        except UnicodeError:
            # If this isn't actually that kind of mojibake, keep original.
            pass

    # --- Normalize " X + Y " -> " X & Y "
    # This does NOT touch 'Anime+' because there are no spaces around '+'.
    g = g.replace(" + ", " & ")

    # NOTE: You explicitly asked *not* to treat ';' as a separator yet.
    # Leaving this commented for possible future use:
    #
    # if ";" in g:
    #     parts = [p.strip() for p in g.split(";") if p.strip()]
    #     if parts:
    #         g = parts[0]

    # --- Collapse multiple spaces to a single space
    g = " ".join(g.split())

    return g or None


def read_m3u(path: Path) -> Iterator[M3UChannel]:
    """
    Simple EXTINF parser:

      - Ignores #EXTM3U and other comment lines except #EXTINF.
      - For each #EXTINF line, the very next non-comment, non-empty line
        is treated as the stream URL.

      - Uses tvg-id if present, otherwise channel-id as tvg_id.
      - Uses tvg-name, or tvc-guide-title, or the display name as tvg_name.
      - Keeps group-title and the raw attrs in case we need them later.
    """
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        last_attrs: Dict[str, str] | None = None
        last_name: Optional[str] = None

        for raw in f:
            line = raw.strip()
            if not line:
                continue

            # header
            if line.startswith("#EXTM3U"):
                continue

            # metadata for next channel
            if line.startswith("#EXTINF:"):
                try:
                    header, name = line.split(",", 1)
                except ValueError:
                    header, name = line, ""

                parts = header.split(" ", 1)
                attr_str = parts[1] if len(parts) > 1 else ""
                attrs = _parse_extinf_attrs(attr_str)

                last_attrs = attrs
                last_name = name.strip()
                continue

            # other comment lines
            if line.startswith("#"):
                continue

            # this should be the URL for the last EXTINF
            url = line
            attrs = last_attrs or {}
            name = (last_name or "").strip()

            tvg_id = attrs.get("tvg-id") or attrs.get("channel-id")
            tvg_name = (
                attrs.get("tvg-name")
                or attrs.get("tvc-guide-title")
                or name
            )

            # Apply normalization *only* to group-title (for now).
            group_title_raw = attrs.get("group-title")
            group_title = _normalize_group_title(group_title_raw)

            yield M3UChannel(
                name=name,
                url=url,
                tvg_id=tvg_id,
                tvg_name=tvg_name,
                group_title=group_title,
                raw_attrs=attrs or None,
            )

            # reset for next channel
            last_attrs = None
            last_name = None
