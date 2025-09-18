#!/usr/bin/env python3
import pathlib
SPDX = "SPDX-License-Identifier: GPL-3.0-or-later"
SKIP = {"data","logs",".git","__pycache__","venv",".venv"}
EXT = {".py": "# ",".sh":"# "}
for p in pathlib.Path(".").rglob("*"):
    if p.is_dir(): continue
    if any(s in p.parts for s in SKIP): continue
    if p.suffix not in EXT: continue
    txt = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    if any(SPDX in line for line in txt): continue
    insert_at = 1 if txt and txt[0].startswith("#!") else 0
    txt.insert(insert_at, f"{EXT[p.suffix]}{SPDX}")
    p.write_text("\n".join(txt)+"\n", encoding="utf-8")
    print("Added:", p)
