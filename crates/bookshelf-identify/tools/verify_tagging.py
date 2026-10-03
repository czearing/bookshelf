#!/usr/bin/env python3
"""Bulk, independent check of `book-manager --apply --tag`.

For every book in a corpus manifest it makes a deliberately messy copy (rotating
through: ALL-CAPS title/author, cover declaration stripped, "Last, First" author),
runs the real CLI over the whole folder, then verifies every outcome without using
any of the Rust code:

  * a book the tool did not touch must be byte-identical to its messy copy;
  * a book it tagged must be a valid EPUB (ZIP, mimetype first and stored, OPF
    well-formed, unique ids, no dangling refinements, exactly one declared cover
    whose file exists and is a JPEG/PNG), carry exactly the title and author in its
    new filename, and keep every other archive member byte-identical to the backup;
  * the tagged identity must agree with the catalogue gold (title words and author
    surname), otherwise it counts as a WRONG TAG, the one number that must be zero.

The fixture rewriting uses plain string search (no regular expressions anywhere in
this project); nothing here is part of the product's identification or writing path.

usage: verify_tagging.py MANIFEST CORPUS_DIR WORKDIR [--limit N] [--provider P] [--cache DIR]
"""
import json, os, shutil, subprocess, sys, unicodedata, zipfile, xml.etree.ElementTree as ET

NS = {"dc": "http://purl.org/dc/elements/1.1/", "o": "http://www.idpf.org/2007/opf"}
EXE = os.path.join("target", "release", "book-manager.exe")


def words(text):
    text = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))
    keep = "".join(c.lower() if c.isalnum() else " " for c in text)
    return [w for w in keep.split() if w]


def rewrite_elements(text, tag, change):
    """Apply `change` to the text of every <tag ...>text</tag>, using plain string search."""
    out, pos, close = [], 0, "</" + tag + ">"
    while True:
        start = text.find("<" + tag, pos)
        if start < 0:
            break
        open_end = text.find(">", start)
        end = text.find(close, open_end)
        if open_end < 0 or end < 0:
            break
        out.append(text[pos:open_end + 1])
        out.append(change(text[open_end + 1:end]))
        pos = end
    out.append(text[pos:])
    return "".join(out)


def drop_cover_meta(text):
    """Remove <meta name="cover" .../> entries and the cover-image property."""
    key, out, pos = '<meta name="cover"', [], 0
    while True:
        start = text.find(key, pos)
        if start < 0:
            break
        end = text.find("/>", start)
        if end < 0:
            break
        out.append(text[pos:start].rstrip())
        pos = end + 2
    out.append(text[pos:])
    return "".join(out).replace(' properties="cover-image"', "")


def flip_name(name):
    parts = name.strip().rsplit(" ", 1)
    return parts[1] + ", " + parts[0] if len(parts) == 2 else name


def messy(src, dst, kind):
    zin = zipfile.ZipFile(src)
    zout = zipfile.ZipFile(dst, "w")
    for info in zin.infolist():
        data = zin.read(info.filename)
        if info.filename.endswith(".opf"):
            text = data.decode("utf-8")
            if kind == 0:
                text = rewrite_elements(text, "dc:title", str.upper)
                text = rewrite_elements(text, "dc:creator", str.upper)
            elif kind == 1:
                text = drop_cover_meta(text)
            else:
                text = rewrite_elements(text, "dc:creator", flip_name)
            data = text.encode("utf-8")
        zout.writestr(info, data, compress_type=info.compress_type)
    zout.close()


def old_declared_cover(old):
    opfn = next((n for n in old.namelist() if n.endswith(".opf")), None)
    if not opfn:
        return False
    root = ET.fromstring(old.read(opfn))
    items = list(root.find("o:manifest", NS))
    md = root.find("o:metadata", NS)
    legacy = [m.get("content") for m in md.findall("o:meta", NS) if m.get("name") == "cover"]
    return any("cover-image" in (i.get("properties") or "").split() or i.get("id") in legacy for i in items)


def structural_errors(path, backup):
    errs = []
    try:
        z = zipfile.ZipFile(path)
        old = zipfile.ZipFile(backup)
    except Exception as e:  # noqa: BLE001
        return [f"unreadable: {e}"]
    if z.testzip():
        errs.append("corrupt zip")
    infos = z.infolist()
    if infos[0].filename != "mimetype" or infos[0].compress_type != 0:
        errs.append("mimetype not first/stored")
    opfn = next((n for n in z.namelist() if n.endswith(".opf")), None)
    if not opfn:
        return errs + ["no opf"]
    try:
        root = ET.fromstring(z.read(opfn))
    except ET.ParseError as e:
        return errs + [f"opf not well-formed: {e}"]
    md = root.find("o:metadata", NS)
    ids = [el.get("id") for el in root.iter() if el.get("id")]
    if len(ids) != len(set(ids)):
        errs.append("duplicate ids")
    for m in md.findall("o:meta", NS):
        r = m.get("refines")
        if r and r[1:] not in ids:
            errs.append(f"dangling refines {r}")
    items = list(root.find("o:manifest", NS))
    cover = [i for i in items if "cover-image" in (i.get("properties") or "").split()]
    legacy = [m for m in md.findall("o:meta", NS) if m.get("name") == "cover"]
    declared = cover or [i for i in items if legacy and i.get("id") == legacy[0].get("content")]
    had_cover = bool(old_declared_cover(old))
    if len(declared) != 1 and (had_cover or declared):
        errs.append(f"{len(declared)} declared covers")
    else:
        base = os.path.dirname(opfn)
        href = declared[0].get("href")
        member = (base + "/" + href) if base else href
        if member not in z.namelist():
            errs.append("cover file missing")
        else:
            head = z.read(member)[:8]
            if not (head[:3] == b"\xff\xd8\xff" or head == b"\x89PNG\r\n\x1a\n" or head[:4] == b"RIFF"):
                errs.append("cover is not JPEG/PNG/WebP")
    changed = {n for n in old.namelist() if n in z.namelist() and z.getinfo(n).CRC != old.getinfo(n).CRC}
    lost = [n for n in old.namelist() if n not in z.namelist()]
    if changed - {opfn}:
        errs.append(f"other members changed: {sorted(changed - {opfn})[:3]}")
    if lost:
        errs.append(f"members lost: {lost[:3]}")
    return errs


def main():
    args = sys.argv[1:]
    manifest, corpus, work = args[:3]
    limit = int(args[args.index("--limit") + 1]) if "--limit" in args else 10**9
    provider = args[args.index("--provider") + 1] if "--provider" in args else "openlibrary"
    cache = args[args.index("--cache") + 1] if "--cache" in args else f"test-data/{provider}-cache"
    books = json.load(open(manifest, encoding="utf-8"))["books"]
    books = [b for b in books if os.path.exists(os.path.join(corpus, f"{b['id']}.epub"))][:limit]
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    originals = {}
    for n, b in enumerate(books):
        dst = os.path.join(work, f"book-{b['id']}.epub")
        messy(os.path.join(corpus, f"{b['id']}.epub"), dst, n % 3)
        originals[b["id"]] = open(dst, "rb").read()
    print(f"{len(books)} messy copies; running the tool ...", flush=True)
    out = subprocess.run(
        [EXE, work, "--provider", provider, "--cache-dir", cache, "--apply", "--tag", "--json"],
        capture_output=True, text=True, encoding="utf-8")
    report = json.loads(out.stdout)
    stats = {"tagged": 0, "renamed_untagged": 0, "untouched_ok": 0, "review": 0, "error": 0, "structure_fail": 0, "wrong_tag": 0, "untouched_changed": 0}
    problems = []
    backup_dir = os.path.join(work, ".book-manager-backup")
    journal = {}
    jp = os.path.join(backup_dir, "journal.jsonl")
    if os.path.exists(jp):
        for line in open(jp, encoding="utf-8"):
            e = json.loads(line)
            journal[os.path.basename(e["original"])] = e
    gold = {f"book-{b['id']}.epub": b for b in books}
    for r in report["records"]:
        name = os.path.basename(r["source"])
        g = gold.get(name)
        if g is None:
            continue
        bid = g["id"]
        if r["status"] in ("review", "error"):
            stats[r["status"]] += 1
            if open(os.path.join(work, name), "rb").read() != originals[bid]:
                stats["untouched_changed"] += 1
                problems.append((name, "review/error but file changed"))
            continue
        e = journal.get(name)
        if e is None:  # renamed only (already consistent): content must be identical
            dest = r["destination"]
            if dest and open(dest, "rb").read() != originals[bid]:
                stats["structure_fail"] += 1
                problems.append((name, "renamed-only file differs from original"))
            else:
                stats["renamed_untagged"] += 1
            continue
        stats["tagged"] += 1
        new = e["new"]
        errs = structural_errors(new, e["backup"])
        base = os.path.basename(new)[:-5]
        author, _, title = base.partition(" - ")
        if errs:
            stats["structure_fail"] += 1
            problems.append((name, errs))
        gt = set(words(g["title"].split("$")[0]))
        got = set(words(title))
        gold_author = g["authors"][0].split("(")[0]
        surname = words(gold_author.split(",")[0])[-1:] if "," in gold_author else words(gold_author)[-1:]
        author_ok = bool(surname) and surname[0] in words(author)
        overlap = len(gt & got) / max(1, len(gt | got))
        if not author_ok or overlap < 0.5:
            stats["wrong_tag"] += 1
            problems.append((name, f"WRONG TAG: {base!r} vs gold {g['title']!r} / {g['authors']}"))
    print(json.dumps(stats, indent=1))
    for p in problems[:40]:
        print(" ", p)
    sys.exit(1 if stats["wrong_tag"] or stats["structure_fail"] or stats["untouched_changed"] else 0)


if __name__ == "__main__":
    main()
