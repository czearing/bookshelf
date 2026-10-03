#!/usr/bin/env python3
"""Synthetic EPUBs for well-known modern books and series, to test standard-book behaviour
without any copyrighted text: only public facts (title, author, series order) plus generic
prose are used. Each title is written in several realistic metadata styles.

usage: synth_modern.py OUT_DIR MANIFEST
"""
import json, os, shutil, sys, zipfile

PROSE = ("The library stood beside a quiet garden where the children gathered after school. Every afternoon the librarian "
 "opened the windows and placed a new selection of stories on the wooden table. One young reader wanted to discover how "
 "people had travelled across the ocean before engines were invented. Another preferred stories about animals living in "
 "the mountains. Their teacher encouraged them to compare different accounts and ask questions whenever an explanation "
 "seemed uncertain. Outside, rain began to fall, but nobody wanted to leave. They continued reading until the lamps were "
 "lit and the evening train arrived at the station. Before returning home, each child chose a book to share with the family.")

# (series, author, [titles in order]) ; series None means standalone
BOOKS = [
 ("Harry Potter","J. K. Rowling",["Harry Potter and the Philosopher's Stone","Harry Potter and the Chamber of Secrets","Harry Potter and the Prisoner of Azkaban","Harry Potter and the Goblet of Fire","Harry Potter and the Order of the Phoenix","Harry Potter and the Half-Blood Prince","Harry Potter and the Deathly Hallows"]),
 ("The Lord of the Rings","J. R. R. Tolkien",["The Fellowship of the Ring","The Two Towers","The Return of the King"]),
 (None,"J. R. R. Tolkien",["The Hobbit","The Silmarillion"]),
 ("The Hunger Games","Suzanne Collins",["The Hunger Games","Catching Fire","Mockingjay"]),
 ("Dune","Frank Herbert",["Dune","Dune Messiah","Children of Dune","God Emperor of Dune"]),
 ("A Song of Ice and Fire","George R. R. Martin",["A Game of Thrones","A Clash of Kings","A Storm of Swords","A Feast for Crows","A Dance with Dragons"]),
 ("The Chronicles of Narnia","C. S. Lewis",["The Lion, the Witch and the Wardrobe","Prince Caspian","The Voyage of the Dawn Treader","The Silver Chair","The Horse and His Boy","The Magician's Nephew","The Last Battle"]),
 ("His Dark Materials","Philip Pullman",["Northern Lights","The Subtle Knife","The Amber Spyglass"]),
 ("Percy Jackson","Rick Riordan",["The Lightning Thief","The Sea of Monsters","The Titan's Curse","The Battle of the Labyrinth","The Last Olympian"]),
 ("Foundation","Isaac Asimov",["Foundation","Foundation and Empire","Second Foundation"]),
 ("Discworld","Terry Pratchett",["The Colour of Magic","Mort","Guards! Guards!","Going Postal"]),
 ("Millennium","Stieg Larsson",["The Girl with the Dragon Tattoo","The Girl Who Played with Fire"]),
 ("Divergent","Veronica Roth",["Divergent","Insurgent","Allegiant"]),
 ("The Wheel of Time","Robert Jordan",["The Eye of the World","The Great Hunt","The Dragon Reborn"]),
 ("Ender's Game","Orson Scott Card",["Ender's Game","Speaker for the Dead"]),
 (None,"F. Scott Fitzgerald",["The Great Gatsby"]),(None,"George Orwell",["Nineteen Eighty-Four","Animal Farm"]),
 (None,"Harper Lee",["To Kill a Mockingbird"]),(None,"Joseph Heller",["Catch-22"]),
 (None,"Douglas Adams",["The Hitchhiker's Guide to the Galaxy"]),(None,"Margaret Atwood",["The Handmaid's Tale"]),
 (None,"Kurt Vonnegut",["Slaughterhouse-Five"]),(None,"J. D. Salinger",["The Catcher in the Rye"]),
 (None,"Dan Brown",["The Da Vinci Code","Angels and Demons"]),(None,"Gillian Flynn",["Gone Girl"]),
 (None,"Stephen King",["The Shining","It","Misery"]),(None,"Michael Crichton",["Jurassic Park"]),
 (None,"Andy Weir",["The Martian"]),(None,"John Green",["The Fault in Our Stars"]),
 (None,"Neil Gaiman",["American Gods","Neverwhere"]),(None,"Ray Bradbury",["Fahrenheit 451"]),
]

def make(path, title, author, style, series, number):
    # style: 0 clean, 1 "Last, First" author, 2 series suffix in title, 3 calibre series meta, 4 title with subtitle "A Novel"
    t, a = title, author
    extra = ""
    if style == 1:
        parts = author.rsplit(" ", 1); a = parts[1] + ", " + parts[0]
    if style == 2 and series:
        t = f"{title} ({series}, #{number})"
    if style == 3 and series:
        extra = f'<meta name="calibre:series" content="{series}"/><meta name="calibre:series_index" content="{number}"/>'
    if style == 4:
        t = f"{title}: A Novel"
    opf = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0" unique-identifier="id">
<metadata><dc:title>{xml(t)}</dc:title><dc:creator>{xml(a)}</dc:creator><dc:language>en</dc:language><dc:identifier id="id">urn:uuid:synthetic-{abs(hash(title))}</dc:identifier>{extra}</metadata>
<manifest><item id="t" href="title.xhtml" media-type="application/xhtml+xml"/><item id="c" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest>
<spine><itemref idref="t"/><itemref idref="c"/></spine></package>'''
    z = zipfile.ZipFile(path, "w")
    z.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
    z.writestr("META-INF/container.xml", '<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>')
    z.writestr("content.opf", opf)
    z.writestr("title.xhtml", f'<html xmlns="http://www.w3.org/1999/xhtml"><body><h1>{xml(title)}</h1><p>by {xml(author)}</p></body></html>')
    z.writestr("chapter.xhtml", f'<html xmlns="http://www.w3.org/1999/xhtml"><body><h2>Chapter 1</h2>{(f"<p>{PROSE}</p>") * 540}</body></html>')
    z.close()

def xml(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def main(out, manifest):
    shutil.rmtree(out, ignore_errors=True); os.makedirs(out)
    books = []; n = 0
    for series, author, titles in BOOKS:
        for i, title in enumerate(titles, 1):
            style = n % 5; n += 1
            bid = f"m{n:03d}"
            make(os.path.join(out, bid + ".epub"), title, author, style, series, i)
            books.append({"id": bid, "stratum": "modern_" + ("series" if series else "standalone"), "title": title, "authors": [author], "languages": ["en"], "series": series, "number": i, "style": style})
    json.dump({"books": books}, open(manifest, "w", encoding="utf-8"), indent=1)
    print(len(books), "synthetic books")
main(sys.argv[1], sys.argv[2])
