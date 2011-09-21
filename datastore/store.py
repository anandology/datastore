import zlib

try:
    import simplejson as json
except ImportError:
    import json

from sqlalchemy import MetaData, Table, Column, \
                    Integer, Unicode, LargeBinary, TIMESTAMP, \
                    create_engine, bindparam
                    
from sqlalchemy.sql import Select
                                    
from sqlalchemy.orm import sessionmaker

class Datastore:
    """Datastore is a simple document database on top of a relational database.
    """
    def __init__(self, name, views):
        self.name = name
        self.views = views
        
        self._engine = None
        self.Session = None
        self._meta = MetaData()
        self.table = self.get_table(self.name, self._meta)
        
        # Make compress_lib and encode_lib attributes so that it is possible to customize them.
        self.compress_lib = zlib
        self.encode_lib = json
    
    def bind(self, db_url):
        """Binds the datastore to a database.
        """
        self._engine = create_engine(db_url, convert_unicode=False, encoding="utf-8")
        self._engine.echo = False
        
        self._meta.bind = self._engine
        
        for view in self.views.values():
            view._init(self)
        
        self._meta.create_all()
        
        self.Session = sessionmaker(bind=self._engine, autoflush=False, autocommit=True)
        
    def get_table(self, name, metadata):
        return Table(name, metadata,
            Column('id', Integer, primary_key = True, autoincrement=True),
            Column('rev', Unicode),
            Column('updated', TIMESTAMP),
            Column('key', Unicode, nullable = False, unique=True),
            Column('data', LargeBinary, nullable=False)
        )
        
    def _encode(self, data):
        edata = self.encode_lib.dumps(data).encode('utf-8')
        return self.compress_lib.compress(edata)
    
    def _decode(self, zdata):
        edata = self.compress_lib.decompress(zdata)
        return self.encode_lib.loads(edata)
        
    def _process_row(self, row):
        """Creates a document by processing a row from the result of db query.
        
        The document is created by decoding `row.data` and special keys
        `_id`, `_key`, `_rev` and `_updated` are added document from row data.
        """
        doc = self._decode(row.data)
        doc['_id'] = row.id
        doc['_rev'] = row.rev
        doc['_key'] = row.key
        doc['_updated'] = row.updated
        return doc
        
    def get(self, key):
        """Returns the document with the given key.
        
        None is returned if no document is found with the given key.
        """
        key = key
        t = self.table
        row = t.select(t.c.key == key).execute().fetchone()
        if row:
            return self._process_row(row)
        
    def put(self, key, doc, updated=None):
        """Adds or updates a document in the datastore.
        
        If a document already exists, it is replaced with the given document.
        """
        t = self.table
        session = self.Session()
        with session.begin():
            # TODO: lock the row and check revision
            q = t.select(t.c.key == key)
            row = session.execute(q).fetchone()
            zdata = self._encode(doc)
            rev = "0"
            if not row:
                q = t.insert().values(key=key, data=zdata, rev=rev)
                _id = session.execute(q).inserted_primary_key[0]
            else:
                q = t.update().where(t.c.key == key).values(data=zdata, rev=rev)
                _id = row.id
                
            doc = dict(doc, _id=_id, _key=key, _rev=None, _updated=None)
            self.update_views([doc], session)
                
        return {"id": _id, "key": key, "rev": None, "updated": None}

    def get_many(self, keys):
        """Returns documents for given keys as a dict.
        """
        t = self.table
        q = t.select(t.c.key.in_(keys))
        rows = q.execute().fetchall()
        return dict((row.key, self._process_row(row)) for row in rows)

    def put_many(self, mapping):
        """Puts multiple documents at once.
        """
        new_mapping = {}
        
        t = self.table
        session = self.Session()
        with session.begin():
            q = t.select(t.c.key.in_(mapping.keys()))
            rows = session.execute(q).fetchall()
            old_keys = set(row.key for row in rows)
            new_keys = set(key for key in mapping if key not in old_keys)
            
            if old_keys:
                q = t.update().where(t.c.key==bindparam('_key')).values(rev=bindparam("_rev"), data=bindparam("_data"))
                params = [dict(_key=key, _data=buffer(self._encode(data)), _rev="0") for key, data in mapping.iteritems() if key in old_keys]
                session.execute(q, params)
                
            if new_keys:
                q = t.insert().values(rev=bindparam("_rev"), data=bindparam("_data"), key=bindparam("_key"))
                params = [dict(_key=key, _data=buffer(self._encode(data)), _rev="0") for key, data in mapping.iteritems() if key in new_keys]
                session.execute(q, params)
            
            q = t.select(t.c.key.in_(mapping.keys())).with_only_columns([t.c.id, t.c.key])
            result = session.execute(q).fetchall()
            for row in result:
                new_mapping[row.key] = dict(mapping[row.key], _id=row.id, _key=row.key)
            
            self.update_views(new_mapping.values(), session)  
                
    def view(self, name, **kwargs):
        v = self.views.get(name)
        return v.query(**kwargs)
        
    def update_views(self, docs, session):
        for view in self.views.values():
            view.update_view(docs, session)

class View:
    """The View defines schema and map function for 
    
    A sample view:
    
        class ISBNView(View):
            def get_table(self, metadata):
                return sa.Table("books_view", metadata,
                    sa.Column("doc_id", sa.Integer),
                    sa.Column("isbn", sa.Unicode, index=True),
                )
            
            def map(self, doc):
                for isbn in doc.get('isbns', []):
                    yield {'isbn': isbn}
    """
    
    def __init__(self):
        self.store = None
    
    def _init(self, store):
        self.store = store
        self.table = self.get_table(store._meta)
        self.table.append_column(Column("_id", Integer, index=True))
        self.table.append_column(Column("_key", Unicode))
        
    def map_docs(self, docs):
        for doc in docs:
            for row in self.map(doc):
                row['_id'] = doc['_id']
                row['_key'] = doc['_key']
                yield row
                
    def update_view(self, docs, session):
        """This function is called by the datastore to update the view for the given docs.
        
        The session argument is the sqlalchemy session in which the updates are supposed to be done.
        """
        if not docs:
            return
            
        ids = [doc['_id'] for doc in docs]
        
        t = self.table
        q = t.delete().where(t.c._id.in_(ids))
        session.execute(q)
        
        bind_params = dict((name, bindparam(name)) for name in t.columns.keys())
        q = t.insert().values(**bind_params)
        
        rows = self.map_docs(docs)
        session.execute(q, list(rows))
        
    def query(self, **kwargs):
        include_docs = kwargs.pop("include_docs", False)
        
        q = self.table.select()
                
        for name, value in kwargs.items():
            if isinstance(value, list):
                q = q.where(self.table.c.get(name).in_(value))
            else:
                q = q.where(self.table.c.get(name) == value)

        result = q.execute()
        keys = result.keys()
        result = [dict(zip(keys, row)) for row in result]
        
        if include_docs:
            mapping = self.store.get_many([row['_key'] for row in result])
            return [dict(row, _doc=mapping[row['_key']]) for row in result]
        else:
            return result
            
    def get_table(self, metadata):
        """Create a :class:`~sqlalchemy.schema.Table` object for this view.
        
        Two additional columns _id and _key are added to this table by the system before use. 
        """
        pass
        
    def map(self, doc):
        """Map function to generate the rows to add to this view.
        
        Subclasses should override this method and yield the rows for this doc as dictionaries.
        """
        return []