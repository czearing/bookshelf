#!/usr/bin/env python3
"""Check a CLI --json report against gold identities. usage: gold_check.py REPORT MANIFEST [--ext]"""
import json, os, sys, unicodedata
def words(t):
    t = "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c))
    return [w for w in "".join(c.lower() if c.isalnum() else " " for c in t).split()]
d = json.load(open(sys.argv[1], encoding="utf-8"))
gold = {str(b["id"]): b for b in json.load(open(sys.argv[2], encoding="utf-8"))["books"]}
ok = sus = rev = wit = 0; bad = []; reasons = {}
for r in d["records"]:
    g = gold.get(os.path.basename(r["source"])[:-5])
    if g is None: continue
    if r["status"] not in ("planned", "unchanged"):
        rev += 1; key = r["reason"][:60]; reasons[key] = reasons.get(key, 0) + 1; continue
    w = r["work"]; gt = set(words(g["title"].split("$")[0])); tt = set(words(w["title"]))
    sur = words(g["authors"][0].split("(")[0].split(",")[0])[-1:]
    a_ok = bool(sur) and sur[0] in words(" ".join(w["authors"])); jac = len(gt & tt) / max(1, len(gt | tt))
    wit += any("full-text" in e for e in w["evidence"])
    if a_ok and jac >= 0.5: ok += 1
    else: sus += 1; bad.append((g["title"][:40], "->", w["title"][:40], w["authors"]))
n = ok + sus + rev
print({"total": n, "auto_correct": ok, "pct": round(100 * ok / max(1, n)), "SUSPECT_WRONG": sus, "review": rev, "via_text_witness": wit, "requests": d["requests"]})
for k, v in sorted(reasons.items(), key=lambda x: -x[1])[:5]: print("  review:", v, k)
for b in bad: print("CHECK", b)
