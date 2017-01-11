import psycopg2
import logging

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

# Connect to the database
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")


def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    cursor = connection.cursor()
    with connection, connection.cursor() as cursor:
        if psycopg2.IntegrityError:
            cursor.execute("update snippets set message=%s where keyword=%s", (snippet, name))
        else:
            cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
    return name, snippet
    logging.debug("Snippet stored successfully.")



def get(name):
    """Retrieve the snippet with a given name. If there is no such snippet,
    return '404: Snippet Not Found.' Returns the snippet."""
    logging.info("Retrieving snippet {!r}".format(name,))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    if not row:
        # No snippet was found with that name.
        return "404: Snippet Not Found"
    logging.debug("Snippet retrieved successfully.")
    return row[0]

def catalogue():
    """Allows the user to to query the keywords from the snippets table."""
    logging.info("Retrieving keywords")
    cursor = connection.cursor()
    cursor.execute("select * from snippets order by keyword")
    row = cursor.fetchall()
    connection.commit()
    logging.debug("Keywords retrieved successfully.")
    return row[0:]

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")

    # Subparser for the catalogue command
    logging.debug("Constructing catalogue subparser")
    catalogue_parser = subparsers.add_parser("catalogue", help="List keywords in snippers table")

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
    elif command == "catalogue":
        catalogue()
        print("Retrieved catalogue")


if __name__=="__main__":
    import argparse
    main()




