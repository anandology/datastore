import zlib

try:
    import simplejson as json
except ImportError:
    import json
    
import sqlalchemy as sa
                    
from sqlalchemy.orm import sessionmaker

class Datastore:
    """Datastore is a simple document database on top of a relational database.
    """
    tablename = "store"
    
    def __init__(self, db_url, echo=False, tablename=None, views=None):
        if tablename is not None:
            self.tablename = tablename
            
        if views is not None:
            self.views = views
        else:
            self.views = self.create_views()
            
        # create SQLAlchemy engine and metadata
        self._engine = sa.create_engine(db_url, convert_unicode=False, encoding="utf-8", echo=echo)
        self._meta = sa.MetaData(bind=self._engine)
        
        # initialize table and views
        self.table = self.get_table(self.tablename, self._meta)
        for view in self.views.values():
            view._init(self)
            
        # create db tables if required
        self._meta.create_all()

        # create Session maker. This is used to create sessions when needed.
        self.Session = sessionmaker(bind=self._engine, autoflush=False, autocommit=True)
        
    
    def create_views(self):
        """This function is called by the constuctor to create the views.
        Subclasses should extend this to provide the views.
        """
        return {}
        
    def add_view(self, name, view):
        self.views[name] = view
        
    def get_table(self, name, metadata):
        return sa.Table(name, metadata,
            sa.Column('id', sa.Integer, primary_key = True, autoincrement=True),
            sa.Column('rev', sa.Unicode),
            sa.Column('updated', sa.TIMESTAMP),
            sa.Column('key', sa.Unicode, nullable = False, unique=True),
            sa.Column('data', JsonDataType(compress=True), nullable=False)
        )
                
    def _process_row(self, row):
        """Creates a document by processing a row from the result of db query.
        
        The document is created by decoding `row.data` and special keys
        `_id`, `_key`, `_rev` and `_updated` are added document from row data.
        """
        doc = row.data
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
            rev = "0"
            if not row:
                q = t.insert().values(key=key, data=doc, rev=rev)
                _id = session.execute(q).inserted_primary_key[0]
            else:
                q = t.update().where(t.c.key == key).values(data=doc, rev=rev)
                session.execute(q)
                _id = row.id
                
            doc = dict(doc, _id=_id, _key=key, _rev=None, _updated=None)
            self.update_views([doc], session)
                
        return {"id": _id, "key": key, "rev": None, "updated": None}
        
    def delete(self, key):
        pass

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
                # name "key" has special meaning in sqlalchemy. Using key_ as name of the bindparam to avoid the clash.
                q = t.update().where(
                        t.c.key==sa.bindparam("_key", type_=t.c.key.type)
                    ).values(
                        rev=sa.bindparam(t.c.rev), 
                        data=sa.bindparam(t.columns.data)
                    )
                    
                params = [dict(_key=key, data=data, rev="0") for key, data in mapping.iteritems() if key in old_keys]
                session.execute(q, params)
                
            if new_keys:
                q = t.insert().values(
                    rev=sa.bindparam(t.c.rev), 
                    data=sa.bindparam(t.c.data), 
                    key=sa.bindparam("_key", type_=t.c.key.type)
                )
                params = [dict(_key=key, data=data, rev="0") for key, data in mapping.iteritems() if key in new_keys]
                session.execute(q, params)
            
            q = t.select(t.c.key.in_(mapping.keys())).with_only_columns([t.c.id, t.c.key])
            result = session.execute(q).fetchall()
            for row in result:
                new_mapping[row.key] = dict(mapping[row.key], _id=row.id, _key=row.key)
            
            self.update_views(new_mapping.values(), session) 
                
    def query(self, name, **kwargs):
        """Queries the view specified by the name.
        
        """
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
        
    @property
    def c(self):
        """Shortcut to acess self.table.columns.
        """
        return self.table.columns
        
    def select(self, **kw):
        """Short cut for calling select on the table of this view.
        """
        return self.table.select(**kw)
    
    def _init(self, store):
        self.store = store
        self.table = self.get_table(store._meta)
        self.table.append_column(sa.Column("_id", sa.Integer, index=True))
        self.table.append_column(sa.Column("_key", sa.Unicode))
        
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
        
        bind_params = dict((c.name, sa.bindparam(c)) for c in t.columns)
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


class JsonDataType(sa.types.TypeDecorator):
    """Column for storing data encoded in JSON with optional compression.
    """
    impl = sa.LargeBinary
    
    def __init__(self, compress=False):
        self.compress = compress
        sa.types.TypeDecorator.__init__(self)

    def process_bind_param(self, value, dialect=None):
        if value is None:
            return None
        else:
            text = json.dumps(value)
            return buffer(zlib.compress(text))

    def process_result_value(self, value, dialect=None):
        if value is None:
            return None
        else:
            text = zlib.decompress(value)
            return json.loads(text)
