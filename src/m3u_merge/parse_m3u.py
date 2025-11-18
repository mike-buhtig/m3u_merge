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

    Works for things like:
      tvg-id="..."
      channel-id="..."
      tvg-name="..."
      tvc-guide-title="..."
      group-title="Nature, History & Science"
      tvg-chno="2905"

    Commas inside quoted values are preserved (e.g. group-title="A, B & C").
    """
    attrs: Dict[str, str] = {}
    key: Optional[str] = None
    buf: list[str] = []
    in_quotes = False

    i = 0
    while i < len(attr_str):
        ch = attr_str[i]

        if ch == '"':
            # Toggle quote state
            in_quotes = not in_quotes

            # Closing quote: commit value to current key
            if not in_quotes and key is not None:
                value = "".join(buf)
                attrs[key.strip()] = value
                key = None
                buf = []

            i += 1
            continue

        if not in_quotes and ch.isspace():
            # Whitespace between attributes when not in quotes
            i += 1
            continue

        if not in_quotes and ch == "=" and key is None:
            # Everything accumulated so far is the key
            key = "".join(buf).strip()
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    return attrs


def _split_extinf_line(line: str) -> tuple[str, str]:
    """
    Split an #EXTINF line into (header, name) while respecting quoted commas.

    Example:
      '#EXTINF:-1 tvg-id="US130000639" group-title="Nature, History & Science" tvg-chno="2905",PBS Genealogy'

    Should become:
      header = '#EXTINF:-1 tvg-id="US130000639" group-title="Nature, History & Science" tvg-chno="2905"'
      name   = 'PBS Genealogy'
    """
    in_quotes = False
    comma_idx: Optional[int] = None

    for i, ch in enumerate(line):
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == "," and not in_quotes:
            comma_idx = i
            break

    if comma_idx is None:
        # No comma found (very unusual, but be defensive)
        return line, ""

    header = line[:comma_idx]
    name = line[comma_idx + 1 :]
    return header, name


def read_m3u(path: Path) -> Iterator[M3UChannel]:
    """
    Simple EXTINF parser:

      - Ignores #EXTM3U and other comment lines except #EXTINF.
      - For each #EXTINF line, the very next non-comment, non-empty line
        is treated as the stream URL.

      - Uses tvg-id if present, otherwise channel-id as tvg_id.
      - Uses tvg-name, or tvc-guide-title, or the display name as tvg_name.
      - Keeps group-title and the raw attrs in case we need them later.

    Important fix:
      We now split the #EXTINF line on the first comma that is NOT inside
      double quotes, so values like:

        group-title="Nature, History & Science"

      are parsed correctly without mangling the channel name.
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
                header, name = _split_extinf_line(line)

                # header is something like:
                #   '#EXTINF:-1 tvg-id="..." group-title="..." tvg-chno="..."'
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
            group_title = attrs.get("group-title")

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

