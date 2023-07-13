
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text

def drop():
    url = URL.create(
        drivername="postgresql",
        username="<user_name>",
        password="<user_password>",
        host="127.0.0.1",
        database="postgres"
    )

    engine = create_engine(url)
    conn = engine.connect()
    conn.execute(text("commit"))
    conn.execute(text("drop database <database_name>;"))
drop()