#!/usr/bin/env python3
"""Fetch public-domain classics through the local `libgen` CLI for realistic, messy test files.

Only titles whose authors died before 1930 are listed, and only editions without a
modern commercial publisher are taken (no publisher listed, a year before 1930, or an
open-source producer). Writes corpus/libgen.json with the *intended* work as gold; a
LibGen file that turns out to be a different work or language (it happens) should end
up in review, never renamed, and the verifier treats a wrong rename of it as a failure.
"""
import json, os, subprocess, sys

TITLES = [
 ("Pride and Prejudice","Jane Austen"),("Emma","Jane Austen"),("Persuasion","Jane Austen"),
 ("Great Expectations","Charles Dickens"),("A Tale of Two Cities","Charles Dickens"),("Oliver Twist","Charles Dickens"),
 ("The Adventures of Tom Sawyer","Mark Twain"),("Adventures of Huckleberry Finn","Mark Twain"),
 ("The Time Machine","H. G. Wells"),("The War of the Worlds","H. G. Wells"),("The Invisible Man","H. G. Wells"),
 ("Twenty Thousand Leagues Under the Sea","Jules Verne"),("Around the World in Eighty Days","Jules Verne"),
 ("The Adventures of Sherlock Holmes","Arthur Conan Doyle"),("The Sign of the Four","Arthur Conan Doyle"),
 ("Crime and Punishment","Fyodor Dostoyevsky"),("The Brothers Karamazov","Fyodor Dostoyevsky"),
 ("War and Peace","Leo Tolstoy"),("Anna Karenina","Leo Tolstoy"),
 ("Moby Dick","Herman Melville"),("Jane Eyre","Charlotte Bronte"),("Wuthering Heights","Emily Bronte"),
 ("Dracula","Bram Stoker"),("Frankenstein","Mary Shelley"),("The Picture of Dorian Gray","Oscar Wilde"),
 ("Treasure Island","Robert Louis Stevenson"),("Heart of Darkness","Joseph Conrad"),
 ("The Count of Monte Cristo","Alexandre Dumas"),("Les Miserables","Victor Hugo"),("Don Quixote","Miguel de Cervantes"),
 ("Walden","Henry David Thoreau"),("The Scarlet Letter","Nathaniel Hawthorne"),("Little Women","Louisa May Alcott"),
 ("The Jungle Book","Rudyard Kipling"),("Peter Pan","J. M. Barrie"),("The Wind in the Willows","Kenneth Grahame"),
 ("Middlemarch","George Eliot"),("Vanity Fair","William Makepeace Thackeray"),("The Odyssey","Homer"),("Candide","Voltaire"),
]
OPEN = ("standard ebooks","project gutenberg","feedbooks","planet pdf","wikisource","manybooks")

def surname(a): return a.split()[-1].lower()

def main(out_dir, manifest):
    os.makedirs(out_dir, exist_ok=True)
    books = []
    for title, author in TITLES:
        r = subprocess.run(["libgen","search","-t",title,"-a",author,"-e","epub","-l","english","-n","15","--json"],
                           capture_output=True, text=True, encoding="utf-8")
        try: hits = json.loads(r.stdout)
        except Exception: continue
        picked = 0
        for h in hits:
            pub = (h.get("publisher") or "").lower()
            year = (h.get("year") or "").split(";")[0].strip()
            old = year.isdigit() and int(year) < 1930
            if not (not pub or old or any(o in pub for o in OPEN)): continue
            if surname(author) not in (h.get("authors") or "").lower(): continue
            if title.lower().split()[-1] not in (h.get("title") or "").lower(): continue
            if h.get("size_bytes", 0) > 30_000_000: continue
            d = subprocess.run(["libgen","download",h["md5"],"-d",out_dir,"-o",f"{h['md5']}.epub"], capture_output=True, text=True, encoding="utf-8")
            if os.path.exists(os.path.join(out_dir, f"{h['md5']}.epub")):
                books.append({"id": h["md5"], "stratum": "libgen_pd", "title": title, "authors": [author], "languages": ["en"], "source_title": h.get("title"), "source_authors": h.get("authors")})
                picked += 1
            if picked >= 2: break
        print(f"{title}: {picked}", flush=True)
    json.dump({"books": books}, open(manifest, "w", encoding="utf-8"), indent=1)
    print("total", len(books))

main(sys.argv[1], sys.argv[2])
