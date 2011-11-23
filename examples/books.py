"""Example program to demonstrate use of Datastore for storing OL editions.

USAGE:

To load books:

        python books.py --load sqlite:///books.db books.json

    Expects the books.json to contain one JSON record per line.

To query:

    python books.py sqlite:///books.db isbn:9780230013384

"""
from datastore.store import Datastore, View
import sqlalchemy as sa
import simplejson
import time
import logging

logger = logging.getLogger(None)

class IdentifierView(View):
    def get_table(self, metadata):
        return sa.Table("editions_identifiers", metadata,
            sa.Column("value", sa.Unicode, index=True)
        )
        
    def map(self, doc):
        yield {"value": "olid:" + doc['key'].split("/")[-1]}
        
        isbns = doc.get('isbn_10', []) + doc.get('isbn_13', [])
        for isbn in isbns:
            yield {"value": "isbn:" + isbn.strip().replace("-", "")}
            
        for key, values in doc.get("identifiers", {}).items():
            for v in values:
                yield {"value": key + ":" + v.strip()}
                
class PreviewView(View):
    def get_table(self, metadata):
        return sa.Table("editions_preview", metadata,
            sa.Column("publish_year", sa.Integer, index=True),
            sa.Column("ebook", sa.Boolean, index=True),
            sa.Column("borrow", sa.Boolean, index=True),
            sa.Column("buy", sa.Boolean, index=True),
            sa.Column("preview", sa.LargeBinary)
        )
        
    def map(self, doc):
        yield {
            "publish_year": self.get_publish_year(doc),
            "ebook": "ocaid" in doc
        }

class Editions(Datastore):
    tablename = "editions"

    def create_views(self): 
        return {
            "ids": IdentifierView()
        }

def group(seq, n):
    seq = iter(seq)
    while True:
        chunk = list(seq.next() for i in range(n))
        if chunk:
            yield chunk
        else:
            break
            
def read_data(filename):
    return (simplejson.loads(line) for line in open(filename))
    

def load(db_url, filename, chunksize=100):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s")
    
    editions = Editions(db_url)
    chunksize = int(chunksize)
    
    T0 = time.time()
    for i, docs in enumerate(group(read_data(filename), chunksize)):
        t0 = time.time()
        mapping = dict((doc['key'], doc) for doc in docs)
        editions.put_many(mapping)
        t1 = time.time()
        logger.info("%d %0.03f %0.01f", i*chunksize, t1-t0, chunksize/(t1-t0))
    T1 = time.time()
    logger.info("AVG %0.03f %0.01f", T1-T0, (i*chunksize)/(T1-T0))

def query(db_url, values):
    editions = Editions(db_url)
    print editions.query("ids", value=values)

def main():
    import sys
    if '--load' in sys.argv:
        sys.argv.remove("--load")
        load(*sys.argv[1:])
    elif "--help" in sys.argv or len(sys.argv) < 3:
        print __doc__
    else:
        query(sys.argv[1], sys.argv[2:])

if __name__ == '__main__':
    main()