#!/usr/bin/env python3
"""Same experiment as passage_probe.py against OpenLibrary 'search inside' (no key, no daily quota).
usage: ol_passage_probe.py REPORT.json CORPUS_DIR MANIFEST"""
import json, os, sys, time, urllib.request, urllib.parse
src = open("tools/passage_probe.py", encoding="utf-8").read().split("def search(phrase):")[0]
ns = {}; os.environ.setdefault("GOOGLE_BOOKS_API_KEY", "unused"); exec(compile(src, "pp", "exec"), ns)
words, passage = ns["words"], ns["passage"]

def search(phrase):
    q = urllib.parse.urlencode({"q": '"' + phrase + '"', "limit": 20})
    for attempt in range(3):
        try:
            req = urllib.request.Request("https://openlibrary.org/search/inside.json?" + q, headers={"User-Agent": "book-manager-test"})
            return json.load(urllib.request.urlopen(req, timeout=60))
        except Exception as e:
            time.sleep(5)
    return {}

def eligible_hit(hit, meta_title, meta_author):
    e = hit.get("edition") or {}
    if not e: return False
    tw = set(words(meta_title.split("$")[0].split(":")[0])); gw = set(words(e.get("title", "")))
    sur = words(meta_author.split(",")[0])[-1:] if meta_author else []
    names = " ".join(a.get("name", "") for a in e.get("authors", []) if isinstance(a, dict))
    return bool(sur) and sur[0] in words(names) and len(tw & gw) / max(1, len(tw | gw)) >= 0.5

report = json.load(open(sys.argv[1], encoding="utf-8")); corpus = sys.argv[2]
man = {str(b["id"]): b for b in json.load(open(sys.argv[3], encoding="utf-8"))["books"]}
cases = []
for r in report["records"]:
    k = os.path.basename(r["source"])[:-5]
    if r["status"] in ("planned", "unchanged") or "omnibus" in r["reason"]: continue
    if not any(c["eligible"] for c in r["candidates"]): continue
    cases.append((k, man[k]))
print(len(cases), "books where the provider agrees with the tags but nothing in the file corroborates")
passages = {k: passage(os.path.join(corpus, k + ".epub")) for k, _ in cases}
hit = miss = fp = tested = 0
for i, (k, b) in enumerate(cases):
    p = passages[k]
    if not p: continue
    res = search(p); hits = res.get("hits", {}).get("hits", [])
    ok = any(eligible_hit(h, b["title"], b["authors"][0]) for h in hits); hit += ok; miss += (not ok)
    other = passages[cases[(i + 7) % len(cases)][0]]
    if other and other != p:
        tested += 1
        if any(eligible_hit(h, b["title"], b["authors"][0]) for h in search(other).get("hits", {}).get("hits", [])): fp += 1
    time.sleep(0.5)
print(f"TRUE passage: claimed book found for {hit} of {hit+miss} ({100*hit/max(1,hit+miss):.0f}%)")
print(f"NEGATIVE control: another book's passage wrongly corroborated the claim {fp} of {tested}")
