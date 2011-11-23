from ..store import Datastore, View, JsonDataType
import sqlalchemy as sa

class TestDatastore:
    def setup_method(self, m):
        self.ds = Datastore("sqlite:///:memory:")
        
    def trim(self, doc):
        return dict((k, v) for k, v in doc.items() if not k.startswith("_"))
        
    def test_put(self):
        self.ds.put("foo", {"name": "foo"})
        assert self.trim(self.ds.get("foo")) == {"name": "foo"}
        
    def test_put_many(self):
        n = 100
        mapping = dict((str(i), {"i": i}) for i in range(n))
        self.ds.put_many(mapping)
        
        for i in range(n):
            assert self.ds.get(str(i))['i'] == i
            
    def test_get_many(self):
        a = {"name": "a"}
        b = {"name": "b"}

        self.ds.put("a", a)
        self.ds.put("b", b)
        
        mapping = self.ds.get_many(["a", "b", "c"])
        assert "a" in mapping
        assert "b" in mapping
        assert "c" not in mapping
            
        assert self.trim(mapping['a']) == a
        assert self.trim(mapping['b']) == b

    def test_views(self):
        class LNameView(View):
            def get_table(self, metadata):
                return sa.Table("lname_view", metadata,
                    sa.Column("lname", sa.Unicode, index=True),
                )
            
            def map(self, doc):
                yield {'lname': doc.get('name', '').lower()}
                
        class NameStore(Datastore):
            def create_views(self):
                return {
                    "lname": LNameView()
                }
                
        self.ds = NameStore("sqlite:///:memory:")
        
        self.ds.put("foo", {"name": "Foo"})
        self.ds.put("bar", {"name": "Bar"})
        
        rows = self.ds.query("lname", lname="foo")
        assert [row['_key'] for row in rows] == ['foo']
        
        rows = self.ds.query("lname", lname="bar")
        assert [row['_key'] for row in rows] == ['bar']
        
        
class TestJsonDataType:
    def test_all(self):
        meta = sa.MetaData()

        t = sa.Table("foo", meta,
            sa.Column('id', sa.Integer, primary_key = True, autoincrement=True),
            sa.Column('key', sa.Unicode),
            sa.Column('data', JsonDataType(compress=True), nullable=False)
        )
        meta.bind = sa.create_engine('sqlite:///:memory:')
        meta.create_all()
        
        t.insert().values(key="foo", data={"name": "Foo"}).execute()
        
        rows = t.select(t.c.key=='foo').with_only_columns([t.c.key, t.c.data]).execute().fetchall()
        assert len(rows) == 1
        assert rows[0] == ("foo", {"name": "Foo"})

