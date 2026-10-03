#!/usr/bin/env python3
"""Run the CLI over a slice of a LibGen manifest and print one line per book.
usage: show_batch.py MANIFEST DIR START COUNT [--provider P]"""
import json, os, shutil, subprocess, sys
man, d, start, count = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
books = json.load(open(man, encoding="utf-8"))["books"][start:start+count]
work = os.path.join("test-data", "batch")
shutil.rmtree(work, ignore_errors=True); os.makedirs(work)
for b in books: shutil.copy(os.path.join(d, f"{b['id']}.epub"), work)
out = subprocess.run([os.path.join("target","release","book-manager.exe"), work, "--provider", "openlibrary", "--cache-dir", "test-data/ol-cache", "--json"], capture_output=True, text=True, encoding="utf-8")
rep = json.loads(out.stdout); gold = {b["id"]: b for b in books}
for r in rep["records"]:
    k = os.path.basename(r["source"])[:-5]; m = r.get("metadata") or {}; g = gold[k]
    lang = (m.get("content") or {}).get("language") or {}
    why = os.path.basename(r["destination"]) if r.get("destination") else r["reason"][:75]
    print(f"{r['status'].upper():8}| want {g['title'][:28]:28} [{g['languages'][0]}] | file: {(m.get('title') or '')[:30]} {m.get('authors')} | lang {lang.get('language')}/{lang.get('status')} | {why}".encode("ascii","replace").decode())
