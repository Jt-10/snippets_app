# Import modules to access PostgreSQL from Python and log application activity
import psycopg2
import logging

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

# Connect to the database
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")


def put(name, snippet):
    """Store snippet with name as its keyword. If a message is already associated with
       the given name, update the existing message with snippet."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("INSERT INTO snippets VALUES (%s, %s)", (name, snippet))
    except psycopg2.IntegrityError:
        with connection, connection.cursor() as cursor:
            cursor.execute("UPDATE snippets SET message=%s WHERE keyword=%s", (snippet, name))
    logging.debug("Snippet stored successfully.")
    return name, snippet # Is this line necessary?


def get(name):
    """Retrieve the snippet with keyword = name. If there is no such snippet,
    return '404: Snippet Not Found.' Return the snippet."""
    logging.info("Retrieving snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT message FROM snippets WHERE keyword=%s", (name,))
        row = cursor.fetchone()
        # Row should be a one-item tuple containing the message associated with the name (keyword) provided
    if not row:
        return "404: Snippet Not Found"
    logging.debug("Snippet retrieved successfully.")
    return row[0] # Does "row[0]" need to be specified or will "row" suffice?


def catalogue():
    """List all keywords from the snippets table. If the table is empty, return
       '404: Snippets table is empty. Try the put() function.'"""
    logging.info("Retrieving keywords from snippets table")
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT keyword, message FROM snippets ORDER BY keyword")
        keywords = cursor.fetchall() # Fetchall() returns a list of tuples
    if not keywords:
        return "404: Snippets table is empty. Try the put() function."
    logging.debug("Catalogue retrieved successfully.")
    return keywords[0:]

def search(search_text):
    """Allows the user to list snippets that contain the given string anywhere in
       their messages. If no message contains the search text, return '404: No snippet
       found.'"""
    logging.info("Searching for snippets with text {!r}".format(search_text))
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT keyword, message FROM snippets WHERE message LIKE '%{}%'".format(search_text))
        snippets_found = cursor.fetchall() # Fetchall() returns a list of tuples
    if not snippets_found:
        return "404: No snippet found containing {!r}".format(search_text)
    logging.debug("Searched for snippets.")
    return snippets_found[0:]

def delete(name):
    """Delete the snippet with the given keyword from the snippets table. If no message has a
       keyword = name, return '404: Snippet not in table.'"""
    logging.info("Deleting snippet with keyword {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("DELETE FROM snippets WHERE keyword=%s", (name,))
    logging.debug("Snippet deleted successfully.")
    return name

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve snippet")
    get_parser.add_argument("name", help="Name of the snippet")

    # Subparser for the catalogue command
    logging.debug("Constructing catalogue subparser")
    catalogue_parser = subparsers.add_parser("catalogue",
         help="List keywords, messages in snippets table")

    # Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search",
         help="List the (keyword, message) pairs from snippets table containing the search text")
    search_parser.add_argument("search_text", help="Text to be searched for in the message column")

    # Subparser for delete command
    logging.debug("Constructing delete subparser")
    delete_subparser = subparsers.add_parser("delete", help="Delete snippet")
    delete_subparser.add_argument("name", help="Name of the snippet")

    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))

    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))

    elif command == "delete":
        snippet = delete(**arguments)
        print("Deleted snippet: {!r}".format(snippet))

    elif command == "catalogue":
        keywords = catalogue()
        print("Retrieved catalogue:")
        for keyword in keywords:
            print("{:<10} {:<10}".format(keyword[0], keyword[1]))

    elif command == "search":
        snippets = search(**arguments)
        print("Snippets found:")
        for snippet in snippets:
            print("{:<10} {:<10}".format(snippet[0], snippet[1]))


if __name__=="__main__":
    import argparse
    main()




