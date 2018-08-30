__author__ = 'Omer'

from protocol import SQLServer
import sys
    

def main(db_name="ORM"):
    server = SQLServer(("0.0.0.0", 53326), db_name)
    print("STARTING TO LISTEN")
    server.listen()


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except IndexError:
        main()
