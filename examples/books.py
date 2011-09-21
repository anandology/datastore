"""Example program to demonstrate use of Datastore for storing OL editions.
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

editions = Datastore("editions", views={
    "ids": IdentifierView(),
})

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

def main(filename, db_url, chunksize=100):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s")
    
    editions.bind(db_url)
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
    
    print editions.view("ids", value=["isbn:9780108739453", "goodreads:3140331"])

if __name__ == '__main__':
    import sys
    main(*sys.argv[1:])