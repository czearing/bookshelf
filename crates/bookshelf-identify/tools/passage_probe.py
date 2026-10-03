#!/usr/bin/env python3
"""Experiment: can a passage from the book's own text, searched in Google Books full text,
independently corroborate which book a file is? (No regular expressions.)
usage: passage_probe.py REPORT.json CORPUS_DIR MANIFEST"""
import json, os, sys, time, html, zipfile, urllib.request, urllib.parse, unicodedata
import xml.etree.ElementTree as ET

KEY = os.environ["GOOGLE_BOOKS_API_KEY"]

def words(t):
    t = "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c))
    return [w for w in "".join(c.lower() if c.isalnum() else " " for c in t).split()]

def strip_tags(chunk):
    out, inside = [], False
    for c in chunk:
        if c == "<": inside = True
        elif c == ">": inside = False; out.append(" ")
        elif not inside: out.append(c)
    return " ".join(html.unescape("".join(out)).split())

def passage(path, n=14):
    z = zipfile.ZipFile(path)
    opf = next(x for x in z.namelist() if x.endswith(".opf"))
    root = ET.fromstring(z.read(opf)); ns = {"o": "http://www.idpf.org/2007/opf"}
    man = {i.get("id"): i.get("href") for i in root.find("o:manifest", ns)}
    base = os.path.dirname(opf)
    spine = [man[i.get("idref")] for i in root.find("o:spine", ns) if i.get("idref") in man]
    for frac in (0.5, 0.4, 0.6, 0.3, 0.7):
        href = spine[int(len(spine) * frac)]
        try: text = z.read((base + "/" + href) if base else href).decode("utf-8", "replace")
        except KeyError: continue
        pos = 0
        while True:
            a = text.find("<p", pos)
            if a < 0: break
            b = text.find("</p>", a)
            if b < 0: break
            para = strip_tags(text[a:b]); pos = b
            ws = para.split()
            if len(ws) >= 30 and not any(w in para for w in ("Gutenberg", "http", "copyright")):
                # start at a sentence-ish boundary: 4th word on, take n words, drop odd punctuation
                chunk = " ".join(ws[3:3 + n]).replace('"', "").replace("“", "").replace("”", "")
                return chunk
    return None

def search(phrase):
    q = urllib.parse.urlencode({"q": '"' + phrase + '"', "maxResults": 10, "printType": "books", "key": KEY,
                                "fields": "totalItems,items(volumeInfo(title,authors))"})
    for attempt in range(4):
        try:
            return json.load(urllib.request.urlopen("https://www.googleapis.com/books/v1/volumes?" + q, timeout=30))
        except Exception as e:
            if "429" in str(e): time.sleep(30)
            else: return {"error": str(e)[:60]}
    return {"error": "429"}

def same_book(item, meta_title, meta_author):
    vi = item.get("volumeInfo", {})
    tw = set(words(meta_title.split("$")[0].split(":")[0])); gw = set(words(vi.get("title", "")))
    sur = words(meta_author.split(",")[0])[-1:] if meta_author else []
    a_ok = bool(sur) and any(sur[0] in words(a) for a in vi.get("authors", []))
    return a_ok and len(tw & gw) / max(1, len(tw | gw)) >= 0.5

report = json.load(open(sys.argv[1], encoding="utf-8")); corpus = sys.argv[2]
man = {str(b["id"]): b for b in json.load(open(sys.argv[3], encoding="utf-8"))["books"]}
cases = []
for r in report["records"]:
    k = os.path.basename(r["source"])[:-5]
    if r["status"] in ("planned", "unchanged"): continue
    if not any(c["eligible"] for c in r["candidates"]): continue
    if "omnibus" in r["reason"]: continue
    cases.append((k, man[k]))
print(len(cases), "books where metadata and provider agree but nothing corroborates")
hit = miss = noterm = 0; fp = tested = 0; details = []
passages = {}
for k, b in cases:
    p = passage(os.path.join(corpus, k + ".epub")); passages[k] = p
for i, (k, b) in enumerate(cases):
    p = passages[k]
    if not p: noterm += 1; continue
    res = search(p); time.sleep(0.75)
    items = res.get("items", [])
    ok = any(same_book(it, b["title"], b["authors"][0] if b["authors"] else "") for it in items)
    hit += ok; miss += (not ok)
    details.append((b["title"][:30].encode("ascii", "replace").decode(), ok, len(items)))
    # negative control: another book's passage must NOT corroborate this book's claimed identity
    other = passages[cases[(i + 7) % len(cases)][0]]
    if other and other != p:
        res2 = search(other); time.sleep(0.75); tested += 1
        if any(same_book(it, b["title"], b["authors"][0] if b["authors"] else "") for it in res2.get("items", [])): fp += 1
print(f"TRUE passage: correct book found in top 10 for {hit} of {hit+miss}  ({100*hit/max(1,hit+miss):.0f}%), no usable passage for {noterm}")
print(f"NEGATIVE control (another book's passage): claimed book wrongly corroborated {fp} of {tested}")
for d in details[:12]: print("  ", d)
