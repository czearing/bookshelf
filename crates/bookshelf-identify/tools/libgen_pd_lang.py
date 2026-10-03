#!/usr/bin/env python3
"""Non-English public-domain classics (authors died before 1930) via the local `libgen` CLI.
Gold language is the language searched for; a file whose text is another language should
end up in review (that is the point). Usage: libgen_pd_lang.py OUT_DIR MANIFEST"""
import json, os, subprocess, sys
TITLES = [
 ("Vingt mille lieues sous les mers","Jules Verne","french"),("Les Miserables","Victor Hugo","french"),
 ("Madame Bovary","Gustave Flaubert","french"),("Germinal","Emile Zola","french"),
 ("Faust","Johann Wolfgang von Goethe","german"),("Die Verwandlung","Franz Kafka","german"),
 ("Also sprach Zarathustra","Friedrich Nietzsche","german"),
 ("Don Quijote de la Mancha","Miguel de Cervantes","spanish"),("Marianela","Benito Perez Galdos","spanish"),
 ("La Divina Commedia","Dante Alighieri","italian"),("I promessi sposi","Alessandro Manzoni","italian"),
 ("Dom Casmurro","Machado de Assis","portuguese"),("Max Havelaar","Multatuli","dutch"),
]
CODE = {"french":"fr","german":"de","spanish":"es","italian":"it","portuguese":"pt","dutch":"nl"}
OPEN = ("standard ebooks","project gutenberg","feedbooks","wikisource","manybooks")
def main(out_dir, manifest):
    os.makedirs(out_dir, exist_ok=True); books=[]
    for title, author, lang in TITLES:
        r = subprocess.run(["libgen","search","-t",title,"-a",author.split()[-1],"-e","epub","-l",lang,"-n","15","--json"],capture_output=True,text=True,encoding="utf-8")
        try: hits=json.loads(r.stdout)
        except Exception: print(title,'no results'); continue
        picked=0
        for h in hits:
            pub=(h.get("publisher") or "").lower(); year=(h.get("year") or "").split(";")[0].strip()
            old=year.isdigit() and int(year)<1930
            if not (not pub or old or any(o in pub for o in OPEN)): continue
            if h.get("size_bytes",0)>30_000_000: continue
            subprocess.run(["libgen","download",h["md5"],"-d",out_dir,"-o",f"{h['md5']}.epub"],capture_output=True,text=True,encoding="utf-8")
            if os.path.exists(os.path.join(out_dir,f"{h['md5']}.epub")):
                books.append({"id":h["md5"],"stratum":"libgen_lang_"+CODE[lang],"title":title,"authors":[author],"languages":[CODE[lang]],"source_title":h.get("title"),"source_authors":h.get("authors")}); picked+=1
            if picked>=2: break
        print(f"{title} ({lang}): {picked}",flush=True)
    json.dump({"books":books},open(manifest,"w",encoding="utf-8"),indent=1); print("total",len(books))
main(sys.argv[1], sys.argv[2])
