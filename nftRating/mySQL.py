import mysql.connector
from mysql.connector import errorcode

ratings_table = 'ratings'
db_name = 'NFT'
mysql_user = 'naya'
mysql_password = 'NayaPass1!'
cnx = mysql.connector.connect(user=mysql_user, password=mysql_password)
cursor = cnx.cursor()


def create_database(curs):
    try:
        curs.execute("CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8';".format(db_name))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


def create_ratings_table(curs):
    try:
        query = """
            USE NFT;
            CREATE TABLE IF NOT EXISTS {} (
            uuid VARCHAR(100) NOT NULL,
            item_name VARCHAR(255) NOT NULL,
            twitter VARCHAR(100),
            url VARCHAR(255) NOT NULL,
            rating DECIMAL (10,2)
            )        
        """.format(ratings_table)

        curs.execute(query, multi=True)
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


# Create mySQL database if it doesn't exist
create_database(cursor)

# Create mySQL table if it doesn't exist
create_ratings_table(cursor)
