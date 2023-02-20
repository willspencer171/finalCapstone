# Imports
import sqlite3
from uuid import uuid4
import pandas as pd
from tabulate import tabulate
from datetime import datetime as dt
import os
from contextlib import suppress

"""
This file will be used as a module to refer to simple SQL commands
These will be to:
    Add new books to the db
    Update book info
    Delete books from the db
    Search the db for a book (by name, author or id)

It will also be able to create a new table in a database
These functions will be held in a Cursor object that is derived
from the sqlite3 Cursor object
"""

# Making output paths
output_paths = ["./outputs/updates",
                "./outputs/queries",
                "./outputs/deletes",
                "./outputs/insertions",]

sql_to_py_datatypes = {"INTEGER": int,
"TEXT": str,
"NULL": None,
"REAL": float}

# This generates a path for the output files to go into
for path in output_paths:
    if not os.path.isdir(path):
        os.makedirs(path)

class MyCursor(sqlite3.Cursor):
    def __init__(self, __cursor: sqlite3.Connection):
        super().__init__(__cursor)
        self.db = __cursor
        self.curr_tab = None
        self.total_changes = self.execute("SELECT total_changes()").fetchall()
        self.changes = self.execute("SELECT changes()").fetchall()
    
    
    keywords = ("AND", "OR", "NOT", "BETWEEN", "IS NULL",
                "LIKE", "IN", "EXISTS", "DISTINCT")

    def create_table(self, name=""):

        # Check for name of table
        if not len(name):
            print("TypeError (create_table): Name parameter missing")
            return
        
        try:
            self.execute(f"""CREATE TABLE {name} (id INTEGER PRIMARY KEY,
        name TEXT,
        author TEXT,
        qty INTEGER)""")
            self.db.commit()
            self.curr_tab = name
        except sqlite3.OperationalError as e:
            self.db.rollback()
            if e.args[0] == f"table {name} already exists":
                self.curr_tab = name
            print(e)
            return
        

    
    def add_entry(self, *args: tuple):
        # Check that a table has been made
        if not self.curr_tab:
            print("Error: No active table, please create one")
            return
        
        self.changes = 0
        
        if not args:
            print("Arguments missing, cannot build record from nothing")
            return
        
        id = 0
        name = ""
        author = ""
        qty = 0 
            
        # Unpacked tuple of tuples
        for index, record in enumerate(args):
            match len(record):
                case 4:
                    id, name, author, qty = record
                case 3:
                    id, name, author = record
                case 2:
                    id, name = record
                case 1:
                    id = record[0]

            try:
                self.execute(f"""INSERT INTO {self.curr_tab} (id, name, author, qty)
                VALUES(?,?,?,?)""", (id, name, author, qty))
                self.db.commit()
                self.update_changes()
                self.changes = index + 1
                print(f"Record with unique ID {id} created in table {self.curr_tab}")
            except sqlite3.IntegrityError:
                self.db.rollback()
                self.changes = 0
                print(f"Record with unique ID {id} already exists")
        
        input_ = f"""INSERT INTO {self.curr_tab} (id, name, author, qty)
        VALUES {'''
        '''.join([", ".join([str(item) for item in record]) for record in args])}"""

        id_tuple = [str(record[0]) for record in args]
        id_tuple = "(" + ", ".join(id_tuple) + ")"
        query = pd.read_sql_query(f"SELECT * FROM {self.curr_tab} WHERE id IN {id_tuple}", self.db)

        output = tabulate(query, headers=query.columns, showindex=False)
        print(output)
        now = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file = f"{output_paths[3]}/{now}-insertion.txt"

        dup_count = 0
        if os.path.exists(out_file):
            files = os.listdir(output_paths[3])
            for file in files:
                if file.find(now) != -1:
                    dup_count += 1

            out_file = out_file[:-4] + f"_{dup_count}" + out_file[-4:]
        with suppress(OSError):
            with open(out_file, "w") as f:
                f.write(input_ + "\n")
                f.write(output)
        
        print(f"Changes this operation: {self.changes}")
        print(f"Changes in session: {self.total_changes}\n")
    
    def update_book(self, cols: tuple, vals: tuple, table: str, *conditions):

        if conditions:
            conditions = "WHERE " + " ".join(conditions) + "\n"

        else:
            print("With no conditions set, all records will be updated")
            
            if input("Are you sure you'd like to continue? (Y/N) ").lower() not in ["yes", "y"]:
                print("Not continuing with update")
                print("To update a particular record, use id=*Record Number* in your input, or another search query")
                return

            conditions = ""
        

        try:
            for i in range(len(cols)):
                self.execute(f"""UPDATE {table} SET {cols[i]}={vals[i]}
        {conditions}""")

            print(f"Record(s) updated with following conditions:")
            print(conditions) if conditions != "" else print("No conditions set")

            for index, col in enumerate(cols):
                print(f"{col}: {vals[index]}")
            
            self.db.commit()
            self.update_changes()

        except sqlite3.DatabaseError as e:
            self.db.rollback()
            self.changes = 0
            print(e)
            return
        
        print(f"Changes in session: {self.total_changes}")
        print(f"Changes this operation: {self.changes}\n")

        # If there has been no change made, don't make a file
        if self.changes == 0:
            return

        input_ = f"""UPDATE {table} SET {", ".join(cols)}={", ".join([str(val) for val in vals])}
        {conditions}"""

        query = pd.read_sql_query(f"SELECT * FROM {table} {conditions}", self.db)
        output = tabulate(query, headers=query.columns, showindex=False)
        print(output)
        now = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file = f"{output_paths[0]}/{now}-update.txt"

        dup_count = 0
        if os.path.exists(out_file):
            files = os.listdir(output_paths[0])
            for file in files:
                if file.find(now) != -1:
                    dup_count += 1

            out_file = out_file[:-4] + f"_{dup_count}" + out_file[-4:]
        with suppress(OSError):
            with open(out_file, "w") as f:
                f.write(input_ + "\n")
                f.write(output)

    def search(self, cols, table, *conditions):
        # cols can be put into the sql query simply as a string, comma separated
        # table is a single item (string)
        # conditions are optional, so WHERE statement is optional.
        # get the user to input their conditions one at a time
        # Use if statement to determine if there are conditions and what they are
        # give option for either AND or OR
        # Conditions should be given as a string, really

        # Conditions parameter can be 0 but if it is, it needs to be put into
        # the SQL query as an empty string, without a WHERE statement

        if len(conditions):
            if conditions[-1].upper() in self.keywords:
                print("\u001b[33mError: Last phrase in conditions cannot be a keyword:")
                for item in self.keywords:
                    if self.keywords.index(item) % 2 == 0:
                        print(item, end="\t\t")
                    else:
                        print(item)
                print("\u001b[0m")
                return

            conditions = "WHERE " + " ".join(conditions) + "\n"
        else:
            conditions = ""
        
        # Selected columns should already be a list of 1 or more by this point
        if len(cols) == 1:
            cols = cols[0]
        else:
            cols = ", ".join(cols)

        input_ = f"""SELECT {cols} FROM {table}
        {conditions}"""
        try:
            query = pd.read_sql_query(input_, self.db)

        except pd.errors.DatabaseError as e:
            if "no such table:" in e.args[0]:
                print(f"Table {table} does not exist")
            elif "incomplete input" in e.args[0]:
                print(f"Incomplete WHERE statement, please include a condition to meet")
            elif "no such column" in e.args[0]:
                name = e.args[0].split(": ")[-1]
                print(f"No column '{name}' in table {table}")
            else:
                print(e)
            
            self.db.rollback()
            return

        output = tabulate(query, headers=query.columns, showindex=False)
        print(output)
        now = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file = f"{output_paths[1]}/{now}-search.txt"

        dup_count = 0
        if os.path.exists(out_file):
            files = os.listdir(output_paths[1])
            for file in files:
                if file.find(now) != -1:
                    dup_count += 1

            out_file = out_file[:-4] + f"_{dup_count}" + out_file[-4:]

        with suppress(OSError):
            with open(out_file, "w") as f:
                f.write(input_ + "\n")
                f.write(output)

    def delete(self, table, conditions: tuple):

        if not len(conditions):
            print("Error, delete conditions must be provided")
            return
        
        conditions = " ".join(conditions)

        query = f"""DELETE FROM {table} WHERE {conditions}"""

        try:
            output = pd.read_sql_query(f"SELECT * FROM {table} WHERE {conditions}", self.db)
            self.execute(query)
            self.db.commit()
            self.update_changes()
        except sqlite3.Error | pd.errors.DatabaseError as e:
            self.db.rollback()
            self.changes = 0
            print(e)
            return

        output = tabulate(output, headers=output.columns, showindex=False)
        print(output)

        now = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_file = f"{output_paths[2]}/{now}-delete.txt"

        dup_count = 0
        if os.path.exists(out_file):
            files = os.listdir(output_paths[2])
            for file in files:
                if file.find(now) != -1:
                    dup_count += 1

            out_file = out_file[:-4] + f"_{dup_count}" + out_file[-4:]
        with suppress(OSError):
            with open(out_file, "w") as f:
                f.write(query + "\n"*2)
                f.write(output)

        print(f"Changes this operation: {self.changes}")
        print(f"Changes this session: {self.total_changes}")

    def update_changes(self):
        self.total_changes = self.execute("SELECT total_changes()").fetchall()[0][0]
        self.changes = self.execute("SELECT changes()").fetchall()[0][0]

    def pretty_print(self, table:str):
        print(table.upper())
        query = pd.read_sql_query(f"SELECT * FROM {table}", self.db)
        print(tabulate(query, headers=query.columns, showindex=False))
