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
        return "drop database", 10
    elif "drop table" in migration:
        return "drop table", 9
    elif "drop view" in migration:
        return "drop view", 3
    elif "alter table" in migration:
        if "add column" in migration:
            if "after" in migration:
                return "adding column after specific column requires table rebuild"
            if " first" in migration:
                return "adding column at 0 pos requires table rebuild"
            if "not null" in migration:
                return "adding a value to existing rows requires table rebuild"
    elif "drop index" in migration:
        return "drop index", 5 #"alter table"
    elif "rename table" in migration:
        return "rename table", 5
    elif "truncate table" in migration:
        return "truncate table", 8
    elif any(admin_func in migration for admin_func in admin_functions):
        return "admin func", 10
    else:
        return "not defined", -1


@app.route('/check_migration')
def check_migration():
    reqdata            = request.get_data().decode()
    reqobj             = json.loads(reqdata)
    migration: str     = reqobj.get("migration",None)
    
    migration_type, risk     = get_migration_type(migration)
    if 
    
    return migration_type, risk, None


if __name__ == '__main__':
    app.run(debug=True, port=5555)