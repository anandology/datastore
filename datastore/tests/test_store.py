from ..store import Datastore

class TestDatastore:
    def setup_method(self, m):
        self.ds = Datastore("docs", views=[])
        self.ds.bind("sqlite:///:memory:")
        
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
