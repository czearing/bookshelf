#!/usr/bin/env python3
"""How much could lossless recompression save on real EPUBs? Measures, per file:
  member bytes by type, zip size as shipped, and zip size with every deflated member
  recompressed with zopfli (the smallest standard deflate encoder). Nothing is written.
usage: measure_compression.py DIR [--limit N]"""
import glob, os, sys, zipfile, collections
import zopfli.zlib  # noqa
import zopfli.gzip  # noqa
from zopfli import ZopfliCompressor, ZOPFLI_FORMAT_DEFLATE

def zop(data):
    c = ZopfliCompressor(ZOPFLI_FORMAT_DEFLATE, numiterations=5)
    return c.compress(data) + c.flush()

d = sys.argv[1]; limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 10**9
files = sorted(glob.glob(os.path.join(d, "*.epub")))[:limit]
tot_orig = tot_zop = 0; by_type = collections.Counter(); by_type_zop = collections.Counter()
for f in files:
    z = zipfile.ZipFile(f); orig = os.path.getsize(f)
    new = orig
    for i in z.infolist():
        ext = os.path.splitext(i.filename)[1].lower()
        by_type[ext] += i.compress_size
        if i.compress_type == zipfile.ZIP_DEFLATED and ext in (".xhtml", ".html", ".htm", ".xml", ".opf", ".ncx", ".css", ".svg", ".txt", ".js", ".json"):
            data = z.read(i.filename); zs = len(zop(data))
            new -= max(0, i.compress_size - zs); by_type_zop[ext] += min(zs, i.compress_size)
        else:
            by_type_zop[ext] += i.compress_size
    tot_orig += orig; tot_zop += new
print(f"{len(files)} files: {tot_orig/1e6:.1f} MB -> {tot_zop/1e6:.1f} MB with zopfli on text members  (saves {100*(tot_orig-tot_zop)/tot_orig:.1f}%)")
tot = sum(by_type.values())
for ext, n in by_type.most_common(8):
    print(f"  {ext or '(none)':8} {100*n/tot:5.1f}% of archive  zopfli-able saving {100*(n-by_type_zop[ext])/max(1,n):4.1f}%")
