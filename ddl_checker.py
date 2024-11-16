from flask import Flask, request
import json
import mysql.connector
import configparser
import re

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

def read_ini(file_path):
    global myconfig
    config = configparser.ConfigParser()
    config.read(file_path)
    for section in config.sections():
        section_lower           = section.lower()
        myconfig[section_lower] = {}
        for key in config[section]:
            myconfig[section_lower][key] = config[section][key]

def get_migration_type(migration: str)->tuple[str, int, bool]:
    """
    Parameters:
    migration, str
    Returns:size_dep
    type of migration, str;
    risk assessment, signed int -1..10;
    size_dependence, bool.
    """
    admin_functions = ["analyze table", "repair table", "optimize table"]
    if   "drop database" in migration:
        return "drop database", 10, False
    elif "drop table" in migration:
        return "drop table", 9, True
    elif "drop view" in migration:
        return "drop view", 3, False
    elif "alter table" in migration:
        if "add column" in migration:
            if "not null" in migration:
                return "adding a value to existing rows requires table rebuild", 7, True
            elif "after" in migration:
                return "adding column after specific column requires table rebuild", 7, True
            elif " first" in migration:
                return "adding column at 0 pos requires table rebuild", 7, True
            else:
                return "could be done safely", 3, False
    elif "drop index" in migration:
        return "drop index", 5, True # TODO check this!
    elif "rename table" in migration:
        return "rename table", 5
    elif "truncate table" in migration:
        return "truncate table", 8
    elif any(admin_func in migration for admin_func in admin_functions):
        return "admin func", 10
    else:
        return "not defined", -1


def get_affected_db_table(migration: str)->tuple[str, str]:
    db    = None
    table = None
    altering_obj = None
    on_match = re.search(r'\bon\s+(\w+)', migration, re.IGNORECASE)
    if on_match:
        altering_obj = on_match.group(1)
    alter_table_match = re.search(r'\bALTER\s+TABLE\s+(\w+)', 
                                  migration,
                                  re.IGNORECASE)
    if alter_table_match:
        altering_obj = alter_table_match.group(1)
    
    if altering_obj is None:
        return None, None

    if "." in altering_obj:
        dbname, tablename = altering_obj.split(".", 1)
        return dbname, tablename
    else:
        dbname    = None
        tablename = altering_obj
        return dbname, tablename


@app.route('/check_migration')
def check_migration():
    reqdata            = request.get_data().decode()
    reqobj             = json.loads(reqdata)
    migration: str     = reqobj.get("migration",None).lower()
    
    migration_type, risk, size_dep  = get_migration_type(migration)

    if size_dep:
        db, aff_table = get_affected_db_table(migration)
    
    return migration_type, risk, None


if __name__ == '__main__':
    myconfig = {}
    app.run(debug=True, port=5555)