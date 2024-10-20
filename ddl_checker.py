from flask import Flask, request
import json


app = Flask(__name__)

class DBConnect:
    def __init__(self):
        self.con = mysql.connector.connect(
             host        = myconfig["db"]["host"],
             user        = myconfig["db"]["username"],
             password    = myconfig["db"]["password"],
             database    = myconfig["db"]["db"],
             port        = myconfig["db"]["port"],
             connection_timeout=86400,
             auth_plugin = 'mysql_native_password',
             autocommit  = True
        )
        self.cur = self.con.cursor(buffered=True)
    def close(self):
        self.cur.close()
        self.con.close()

def get_migration_type(migration):
    type = "not defined"
    migration = migration.lower()
    admin_functions = ["analyze table", "repair table", "optimize table"]
    if   "drop database" in migration:
        return "drop database"
    elif "drop table" in migration:
        return "drop table"
    elif "drop view" in migration:
        return "drop view"
    elif "alter table" in migration:
        return "alter table"
    elif "drop index" in migration:
        return "drop index" #"alter table"
    elif "rename table" in migration:
        return "rename table"
    elif "truncate table" in migration:
        return "truncate table"
    elif any(admin_func in migration for admin_func in admin_functions):
        return "admin func"
    else:
        return "not defined"


@app.route('/check_migration')
def check_migration():
    reqdata            = request.get_data().decode()
    reqobj             = json.loads(reqdata)
    migration: str     = reqobj.get("migration",None)
    migration_type     = get_migration_type(migration)
    return migration_type


if __name__ == '__main__':
    app.run(debug=True, port=5555)