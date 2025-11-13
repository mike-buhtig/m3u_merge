from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
from lxml import etree

@dataclass
class EPGChannel:
    id: str
    display_names: List[str]

@dataclass
class EPGCounts:
    channels: int
    programmes: int

def scan_epg_counts(path: Path) -> EPGCounts:
    ch = pr = 0
    for _, elem in etree.iterparse(str(path), events=("end",), tag=("channel","programme")):
        if elem.tag == "channel": ch += 1
        elif elem.tag == "programme": pr += 1
        elem.clear()
    return EPGCounts(channels=ch, programmes=pr)

def read_epg_channels(path: Path) -> Dict[str, EPGChannel]:
    out: Dict[str, EPGChannel] = {}
    for _, elem in etree.iterparse(str(path), events=("end",), tag=("channel",)):
        cid = elem.get("id") or ""
        names = [dn.text.strip() for dn in elem.findall("display-name") if dn.text]
        if cid:
            out[cid] = EPGChannel(id=cid, display_names=names)
        elem.clear()
    return out
