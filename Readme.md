# Datastore

Datastore is a simple document database that runs on any relational database.
It is built with good ideas from CouchDB, FriendFeed Datastore and Open
Library datastore. 

    from datastore import Datastore
    
    store = Datastore("sqlite:///users.db", tablename="users")
    
    store.put("joe", {
        "username": "joe",
        "name": "Joe Hacker"
        "email": "joe@example.com",
    })
    
    print store.get("joe")['email']
    
Under the hood, the Datastore creates a table with given name, which is `users` in this case, for storing the documents.

Document can be any dictionary. The document is json-encoded before adding to the database.

The Datastore uses SQLAlchemy to interact with the database and supports all databases that SQLAlchemy supports.

# Views

Just a document store is no good, unless it has a way to query the data.
Datastore provides views to address this.

A view is a table in the database, that is updated when a doc is added to the
database. The schema of the table and the code to map a document to view rows
is specified by writing a view class.

    from datastore import View
    
    class UsersIndex(View):
        """A view on users store."""
        
        def get_table(self, metadata):
            """Schema of the index view."""
            return sa.Table("users_index", metadata,
                sa.Column("email", sa.Unicode, index=True),
            )
            
        def map(self, doc):
            yield {
                "email": doc['email'].lower()
            }

    store.add_view("index", UsersIndex())
    
The map function emits, one or more rows to be inserted in the view table. 

The Datastore provides a simple API to query the view.

    # find user with given email.
    store.query("index", email="joe@example.com")
    
    # Find user with any one of the emails.
    store.query("index", email=["joe@example.com", "foo@bar.com"])
    
    # It is possible to query on multiple columns comined with AND.
    store.query("book", title="Book Title", author="Author Name")

If the query API is not good enough for your needs, you can always fallback to SQLAlchemy for querying.

It is usually convenient to subclass `Datastore` when you have views.

    class UsersStore(Datastore):
        tablename = "users"
        
        def create_views(self): 
            return {
                "index": UsersIndex()
            }
            
    store = UsersStore("sqlite:///users.db")
    store.query("index", email="joe@example.com")
    
# Adding Constraints

Since the schema of the view is defined by the user, he can decide what
constraints to add on that table. Since the view is updated in the same
transaction as the document, failures to update the view will result in
rolling back the transaction.

For example, adding unique constraint to email is as simple as adding unique attribute to the `email` column.

    sa.Column("email", sa.Unicode, unique=True)

# Warning!

The project is still under very early stage of development and the API is likely to change.

# License

I haven't decided on the license yet, but it will more likely be GPL v3.