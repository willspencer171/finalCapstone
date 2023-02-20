# Imports
import sqlite3
import queries as q
# used to iterate over a function to test file naming system as well
# as return script efficiency
import timeit
from re import split

def get_conditions():
    conditions = []
    print('''Please enter the search conditions for records that you would like
to delete.
Conditions are written with the format:

field_name comparison_operator value

where comparison_operator can be chosen from this list:
=   (equal to)                      <>  (not equal to)
>   (greater than)                  <   (less than)
>=  (greater than or equal to)      <= (less than or equal to)
BETWEEN (value) AND (value)         IS NULL   (value is empty)

If you're done entering conditions, enter a backslash (\\)''')

    while True:

        condition = input("Enter search conditions to edit records: ")

        if condition == "\\":  # end loop, no further conditions
            break
        # split the condition at any of the operators above. If it doesn't split twice, don't allow it
        elif len(split("(=|<|>|<=|>=|<>|BETWEEN|IS NULL)", condition)) < 3:
            print("Please enter a complete condition")
            continue
        else:   # continue loop, set iterator to True
            conditions.append(condition)

        if len(conditions) > 1:
            # Separate conditions with the user's choice
            connect = input(
                "Will these conditions be joined via AND or OR? (a/o) ").lower()
            if connect not in ["a", "and", "o", "or"]:
                print("Enter AND or OR to continue")
                continue

            elif connect in ["a", "and"]:
                connect = "AND"
            else:
                connect = "OR"
            conditions.insert(-2, connect)
            break

    return tuple(conditions)

# Create / connect to database
db = sqlite3.connect("data/ebookstore.db")
# Create instance of my cursor with added functions
cursor = q.MyCursor(db)

# PRAGMA gets information about the database itself
# This returns a list of all tables in database main
tables = cursor.execute("PRAGMA main.table_list").fetchall()
tables = [table for table in tables if table[1] != "sqlite_schema"]

if not len(tables):
    while True:
        name = input("No tables in database, please enter a name for a new table: ")
        if len(name):
            break
        print("Please enter a name")
    cursor.create_table(name)

# used in queries.py to indicate the name of the table being searched 
# defaults to first table
cursor.curr_tab = tables[0][1]

# This just shows the table without creating a query record
cursor.pretty_print("books")

# User 'interface'
print('''
======== BOOKSTORE DATABASE MANAGEMENT ========
Welcome, bookstore clerk. You're here because you
need to access, update or query the database, right?

You have a few options regarding what you can do
here. Enter your choice from the list of numbers
below:''')

while True:
    print('''
1 - Add a book to the database
2 - Update an existing book
3 - Delete a book from the database
4 - Search the database for a book
5 - View Database
0 - Exit
''')
    choice = input("Your choice: ")
    if not choice.isdigit():
        print("Please enter a number")
        continue
    elif int(choice) > 5:
        print("Please enter a choice from the list (0-5)")
        continue
    
    choice = int(choice)
    match choice:
        case 1: # Add entry
            # Some code validating inputs for id, title, author and qty
            # get the user to enter how many entries they're making
            while True:
                entries_num = input("How many entries are we making? ")
                if not entries_num.isdigit():
                    print("Please enter an integer (positive)")
                    continue
                entries_num = int(entries_num)
                break
            
            entries = []
            for i in range(entries_num):
                # for each entry, enter all attributes
                # validate for ID
                while True:
                    id = input(f"Enter the unique ID of entry {i + 1}: ")
                    if not id.isdigit():
                        print("Please enter a whole number as an ID")
                        continue
                    id = int(id)
                    break
                
                # title can be anything, no validation required
                name = input(f"Enter the name of entry {i + 1}: ")

                # author can be anything, no validation required
                author = input(f"Enter the author of entry {i + 1}: ")

                while True:
                    qty = input(f"How many of entry {i + 1}? ")
                    if qty == "":
                        qty = 0
                    elif not qty.isdigit():
                        print("Please enter a whole number")
                        continue

                    qty = int(qty)
                    break

                # This goes in as an argument to add_entry
                # There can be as many of these as we like
                entries.append((id, name, author, qty))
                
            # asterisk used to unpack list into args
            cursor.add_entry(*entries)

        case 2: # Update entry
            # update_book requires cols, vals to be entered as a tuple, regardless of length
            cols = []
            vals = []

            print('''You are updating an entry in the database
====================''')
            print("These are the tables available in this database:")
            
            # Get table name first so we can show the user 
            # what fields are available to them
            while True:
                for table in tables:
                    print(table[1])

                table = input("Which table are you looking to update? ")
                if not [tab for tab in tables if table == tab[1]]:
                    print("Table not in database, please pick one from the list")
                    continue
                break
            
            # PRAGMA statement gets the information about the table,
            # yielding field names
            choices = cursor.execute(f'''PRAGMA table_info({table})''').fetchall()

            # Eliminate id field to imitate read-only nature of primary key
            choices = [choice for choice in choices if choice[1] != "id"]
            print(f"Fields in {table}:\n")
            for col in choices:
                print(f"{col[1]}\t({col[2]})")
            
            while True: # fields
                print()
                col = input("Which field would you like to edit? (enter a backslash '\\' to finish choices) ")
                # Ensure the number of fields to edit is greater than 0
                if col[0] == "\\" and len(cols):
                    break
                elif col[0] == "\\":
                    print("Please enter a column name first")
                
                # Then check if the table contains the entered field
                elif col not in [choice[1] for choice in choices]:
                    print("Column not in table, pick from the list")
                    continue
                
                # This prevents duplicate entries, but then again, 
                # converting to tuple eliminates duplicates
                elif col not in cols:
                    cols.append(col)
            cols = tuple(cols)
            choices = [col for col in choices if col[1] in cols]

            for col in choices: # values
                # This allows reiteration without stepping 
                # forward in the for loop
                while True:
                    val = input(f"What is the value of {col[1]}? ")
                    if val.isdigit() and col[2] == "INTEGER":
                        val = int(val)
                    elif col[2] == "TEXT":
                        val = "'" + val + "'"
                    
                    # using the dictionary in queries.py, we can map the
                    # sql datatypes to python datatypes
                    if type(val) == q.sql_to_py_datatypes[col[2]]:
                        vals.append(val)
                    else:
                        print(f"val does not match column datatype (should be {col[2]})")
                        continue
                    break
            vals = tuple(vals)

            # conditions
            conditions = get_conditions()

            # Finally, run the update function with the above inputs
            cursor.update_book(cols, vals, table, *conditions)

        case 3: # Delete entry(s)
            # the \u001b[Xm is used to change the terminal output colour. 33m gives a yellow warning colour, 
            # 0m switches back to standard
            print('''\u001b[33m
==========================WARNING=============================
SHOULD YOU PROCEED, YOU WILL BE DELETING DATA FROM THE DATABASE
\u001b[0m''')

            # Table
            print("Tables available:")
            for table in tables:
                print(table[1])

            while True:
                table = input("Please choose which table you'll be editing from the above list: ")
                if table not in [table[1] for table in tables]:
                    print("Table not in list of available tables")
                    continue
                break
            
            # Conditions
            conditions = get_conditions()

            cursor.delete(table, conditions)

        case 4: # Search entries
            cols = ["id"]
            conditions = []

            # Give the user a list of tables available
            while True:
                for table in tables:
                    print(table[1])
                print()

                table = input("Which table are you searching in? ")
                if not [tab for tab in tables if table == tab[1]]:
                    print("Table not in database, please pick one from the list")
                    continue
                break

            # PRAGMA statement gets the information about the table,
            # yielding field names
            choices = cursor.execute(f'''PRAGMA table_info({table})''').fetchall()

            # Eliminate id field to imitate read-only nature of primary key
            choices = [choice for choice in choices if choice[5] == 0]
            print(f"Fields in {table}:\n")
            for col in choices:
                print(f"{col[1]}\t({col[2]})")
            print("all")

            while True:
                col = input("Which of the above fields should be included in your search results? (enter a backslash '\\' to finish choices) ")
                if col == "\\" and len(cols) == 1:
                    print("Must have one or more entries before finishing choices")
                    continue
                elif col == "\\":
                    break
                elif col == "all":
                    cols = ["*"]
                    break
                elif col not in [choice[1] for choice in choices] + ["all"]:
                    print("Please choose a field to return in search (case sensitive)")
                    continue
                    
                cols.append(col)
            
            cols = tuple(cols)


            loop = False

            if input("Would you like to add search filters? (y/n) ") in ["y", "yes"]:
                conditions = get_conditions()
            
            cursor.search(cols, table, *conditions)

        case 5: # Print all tables (if there are more than one)
            for table in tables:
                cursor.pretty_print(table[1])

        case 0: # Exit
            print("Exiting DBMS...")
            break

db.close()
