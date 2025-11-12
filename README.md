# m3u_merge

Fetch multiple M3U/XMLTV sources over HTTP, normalize/dedupe, and serve:
- /my_playlist.m3u
- /my-epg.xml

Supports filters (?groups=..., ?providers=..., ?regions=..., ?exclude=...).

Dev quickstart:
- docker compose up --build
