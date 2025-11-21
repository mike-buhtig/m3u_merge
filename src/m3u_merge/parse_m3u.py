from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional
# parse_m3u.py version 1.0.6


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
                # closing quote: commit the current buffer as value
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
    Normalize group-title strings.
    """
    if not group:
        return group

    g = group.strip()

    # --- Fix typical UTF-8-as-Latin1 mojibake ---
    if "Ã" in g or "Â" in g:
        try:
            g = g.encode("latin1").decode("utf-8")
        except UnicodeError:
            pass

    # --- Normalize " + " to " & " ---
    g = g.replace(" + ", " & ")

    # --- Collapse multiple spaces ---
    g = " ".join(g.split())

    return g or None


def _split_extinf_line(line: str) -> tuple[str, str]:
    """
    Safely splits an #EXTINF line into (attributes, name).
    It looks for the first comma that is NOT inside quotes.
    """
    # Strip '#EXTINF:' prefix if present
    if line.startswith("#EXTINF:"):
        # Skip the tag itself
        start_idx = 8
    else:
        start_idx = 0

    in_quotes = False
    split_index = -1

    # Scan the string starting after #EXTINF:
    for i in range(start_idx, len(line)):
        ch = line[i]

        if ch == '"':
            in_quotes = not in_quotes
        
        # Found the separator comma?
        if ch == ',' and not in_quotes:
            split_index = i
            break
    
    if split_index != -1:
        # Everything before comma (minus #EXTINF:) is metadata
        # Everything after is the channel name
        # We slice from 0 to keep original indices aligned conceptually, 
        # but effectively we want header part vs name part.
        
        # If there was an #EXTINF prefix, we might want to remove the duration logic here?
        # Actually, easiest is just to return the raw chunks.
        
        header_part = line[:split_index] # Contains "#EXTINF:-1 tvg-id..."
        name_part = line[split_index+1:] # Contains "My Channel Name"
        return header_part, name_part
    else:
        # No comma found? Treat whole thing as header (weird) or empty name
        return line, ""


def read_m3u(path: Path) -> Iterator[M3UChannel]:
    """
    Parses M3U using a robust char-by-char splitter to handle commas in attributes.
    """
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        last_attrs: Dict[str, str] | None = None
        last_name: Optional[str] = None

        for raw in f:
            line = raw.strip()
            if not line:
                continue

            if line.startswith("#EXTM3U"):
                continue

            if line.startswith("#EXTINF:"):
                # === ROBUST SPLIT ===
                # We manually split at the first comma that isn't quoted.
                header_raw, name = _split_extinf_line(line)
                
                # header_raw looks like: "#EXTINF:-1 tvg-id="x""
                # We need to isolate the attributes. 
                # Usually distinct from duration by a space.
                
                # Remove "#EXTINF:" prefix for processing
                clean_header = header_raw.replace("#EXTINF:", "", 1).strip()
                
                # Attempt to separate duration from attributes. 
                # Duration is usually the first token (e.g. -1 or 0).
                # We look for the first space.
                parts = clean_header.split(" ", 1)
                
                if len(parts) > 1:
                    # parts[0] is duration, parts[1] is attr string
                    attr_str = parts[1]
                else:
                    # No space found? Could be "#EXTINF:-1" with no attrs
                    # Or weirdly formatted. Assume no attrs if no space separator.
                    attr_str = ""

                attrs = _parse_extinf_attrs(attr_str)

                # --- DEBUGGING (Optional: Uncomment if a specific group still fails) ---
                # if "News, Weather" in raw:
                #     print(f"DEBUG: Found line: {line}")
                #     print(f"DEBUG: Split Name: {name}")
                #     print(f"DEBUG: Attr Str:   {attr_str}")
                #     print(f"DEBUG: Parsed:     {attrs}")
                # ---------------------------------------------------------------------

                last_attrs = attrs
                last_name = name.strip()
                continue

            if line.startswith("#"):
                continue

            url = line
            attrs = last_attrs or {}
            name = (last_name or "").strip()

            tvg_id = attrs.get("tvg-id") or attrs.get("channel-id")
            tvg_name = (
                attrs.get("tvg-name")
                or attrs.get("tvc-guide-title")
                or name
            )

            raw_group = attrs.get("group-title")
            group_title = _normalize_group_title(raw_group)

            yield M3UChannel(
                name=name,
                url=url,
                tvg_id=tvg_id,
                tvg_name=tvg_name,
                group_title=group_title,
                raw_attrs=attrs or None,
            )

            last_attrs = None
            last_name = None
