from falkordb import FalkorDB
import argparse
import re
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

parser = argparse.ArgumentParser(description="FalkorDB CLI")
parser.add_argument(
    "mode",
    help="The script to be run. Choose read to read the db operations. Choose write to write the operations to the db",
    choices=["read", "write"],
)
parser.add_argument("--uri", help="The URI of the database", required=True)
parser.add_argument(
    "--file", help="The file to be read or written to the database", required=True
)
parser.add_argument(
    "--start-write-from-line",
    required=False,
    help="The line to start writing from. The value will be searched in the file, and the first line after the one that contains the value will be the starting point",
)

args = parser.parse_args()

write_op = "CREATE|DELETE|SET|REMOVE|MERGE"

db: FalkorDB = None


def write_to_file(file, data):
    with open(file, "a", encoding="utf8") as f:
        f.write(data + "\n")


def read_db(db: FalkorDB, file: str):

    if not os.path.exists(file):
        with open(file, "w", encoding="utf8") as f:
            f.write("")

    with db.connection.monitor() as m:
        logging.info("Monitoring started")
        for cmd in m.listen():
            line = cmd["command"]
            if len(re.findall("GRAPH.QUERY", line, re.MULTILINE | re.IGNORECASE)) > 0:
                m = re.match(
                    "GRAPH.QUERY (.*?) (.*) --compact",
                    line,
                    re.MULTILINE | re.IGNORECASE,
                )
                query = m.group(2)
                if len(re.findall(write_op, query, re.MULTILINE | re.IGNORECASE)) > 0:
                    write_to_file(file, line)
                    logging.info(f"Writing to file: {line}")


def write_db(db: FalkorDB, file: str):
    logging.info("Starting writing to db")
    with open(file, "r", encoding="utf8") as f:
        lines = f.readlines()
        found_starting_point = not args.start_write_from_line
        for line in lines:
            if not found_starting_point:
                if args.start_write_from_line in line:
                    logging.info(f"Found starting line: {line}")
                    found_starting_point = True
                continue

            m = re.match(
                "GRAPH.QUERY (.*?) (.*) --compact",
                line,
                re.MULTILINE | re.IGNORECASE,
            )

            graph = m.group(1)
            query = m.group(2).replace('\n', ' ').replace('\\n', ' ')
            logging.info(f"Executing query: {graph} - {query}")
            db.select_graph(graph).query(query)

    logging.info("Finished writing to db")

def main():
    global db

    try:
        logging.info("Connecting to database")
        db = FalkorDB.from_url(args.uri)
        logging.info("Connected to database")

        if args.mode == "read":
            read_db(db, args.file)
        elif args.mode == "write":
            write_db(db, args.file)

    except KeyboardInterrupt:
        logging.info("Exiting")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if db:
            db.connection.close()
        exit(0)


if __name__ == "__main__":
    main()
