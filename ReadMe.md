## How to run project?

Add new file `.env`
```ini
MYSQL_DATABASE=test
MYSQL_USER=test
MYSQL_PASSWORD=test
MYSQL_ROOT_PASSWORD=test
MYSQL_PORT=3306

MONGO_INITDB_ROOT_USERNAME=test
MONGO_INITDB_ROOT_PASSWORD=test
MONGO_INITDB_DATABASE=test
MONGO_PORT=27019
```

Leter, run docker and scripts.
```
docker compose up
python3 mongo_database/schema.py
python3 mysql_database/schema.py
```