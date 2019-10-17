import string
import random
import time
import sqlite3
import pandas as pd
from sqlite3 import Error


NUM_CARDS = 5000
NUM_USERS = 100
NUM_VERIFICATIONS = 20000
VERIFICATIONS_NEEDED = 200

'''
This Python Script is an exercise to demonstrate a three way table join and discrimination based on verification frequency

(1) Cards are created for various entities and stored in a Pandas DataFrame and an SQLite table
(2) The cards are verified (by someone) these verifications are stored in a Pandas DataFrame and an SQLite table
(3) A List of Users (of the system) is stored in a Pandas DataFrame and an SQLite table

There are generator methods for each of the Cards, Verifications and User tables with a random string generator

A new table for Enriched Cards is created by a 3-way inner join to bring together verifications and users for each card

The users with more than a threshold (VERIFICATIONS_NEEDED) of verifications are the output of this exercise. Only keep users
who have performed a verification action more than a certain number of times.

This inner join exercise is done using SQLite (no ORM)  with stright embeded SQL and also with Pandas dataframes...
'''


class Card:
    def __init__(self, card_id, title, creation_date, created_by, verification_interval):
        self.card_id = card_id
        self.title = title
        self.creation_date = creation_date
        self.created_by = created_by    # this is a user_id
        self.verification_interval = verification_interval  # time since last verification


class Verification:
    def __init__(self, card_id, verification_date, verified_by):
        self.card_id = card_id
        self.verification_date = verification_date
        self.verified_by = verified_by   # this is a user_id


class User:
    def __init__(self, user_id, user_name):
        self.user_id = user_id
        self.user_name = user_name


def randomString(len):
    '''
    generate random string for names
    '''
    rand_string = random.choice(string.ascii_letters)
    
    for _ in range(len - 1):
        rand_string += random.choice(string.ascii_letters)
    
    return rand_string


def sqlite3_connect(table_name):
    try:
        con = sqlite3.connect(table_name)
        return con
    except Error:
        print(Error)


def generate_users(num_users):
    '''
    generates list of users for streaming joins
    '''
    users = []
    for i in range(num_users):
        users.append(User(i, randomString(random.randint(6, 10))))

    return users


def generate_cards(num_cards, num_users):
    '''
    generates list of cards for streaming joins
    '''
    creation_date = time.time() - num_cards * 3600
    cards = []
    for i in range(1, num_cards + 1):
        card_id = i
        title = randomString(random.randint(6, 12))
        creation_date += random.randint(3400, 3800) 
        created_by = random.randint(1, num_users + 1)    # this is a user_id
        verification_interval = time.time() - creation_date
        cards.append(Card(card_id, title, creation_date, created_by, verification_interval))

    return cards


def generate_verifications(num_verifications, num_users, cards):
    '''
    generates list of users for streaming joins
    '''
    verifications = []
    for _ in range(num_verifications):
        num_cards = len(cards)
        idx = random.randint(0, num_cards - 1)
        card = cards[idx]
        card_id = card.card_id
        verification_date = card.creation_date + 3600 + random.randint(7200, 36000)
        verified_by = random.randint(0, num_users - 1)  # this is a user_id
        verifications.append(Verification(card_id, verification_date, verified_by))
        
    return verifications


def pandas_verifications_by_users(users, cards, verifications, verifications_needed):
    '''
    Use pandas to list all user names of those who verified more than k cards where k = verifications_needed
    '''
    users = pd.DataFrame([u.__dict__ for u in users])
    cards = pd.DataFrame([c.__dict__ for c in cards])
    verifications = pd.DataFrame([v.__dict__ for v in verifications])

    # add user_id to cards by left joining cards with
    join1 = cards.merge(verifications, left_on='card_id', right_on='card_id')
    join2 = join1.merge(users, left_on='verified_by', right_on='user_id')
    user_counts = join2.user_name.value_counts()
    print("Users with greater than {} verifications include:".format(verifications_needed))
    ctr = 0
    for name, counts in user_counts.items():
        if counts > verifications_needed:
            print("Name: {} ----- {}".format(name, counts))
            ctr += 1
    print("Pandas - Total Users with greater than {} verifications: {}".format(verifications_needed, ctr))


def sql_verifications_by_users(users, cards, verifications, verifications_needed):
    '''
    Use python SQL calls to list all user names of those who verified more than k cards where k = verifications_needed
    Currently using sqlite3 but could easily change to other DB's
    (Could write method with sqlalchemy but this demonstrates 'naked' SQL usage more appropriately)
    '''
    # connect to sqlite3 and create cards, users and verification tables
    con  = sqlite3_connect('mydb.db')
    cursor = con.cursor()
    cursor.execute("DROP TABLE IF EXISTS Cards")
    cursor.execute("CREATE TABLE Cards(card_id integer PRIMARY KEY, title text, creation_date long, created_by integer, verification_interval long)")
    cursor.execute("DROP TABLE IF EXISTS Verifications")
    cursor.execute("CREATE TABLE IF NOT EXISTS Verifications(id integer PRIMARY KEY, card_id integer, verification_date long, verified_by integer)")
    cursor.execute("DROP TABLE IF EXISTS Users")
    cursor.execute("CREATE TABLE IF NOT EXISTS Users(user_id integer PRIMARY KEY, user_name text)")

    # insert records into tables
    for c in cards:
        cursor.execute("INSERT INTO Cards VALUES({}, '{}', {}, {}, {})".format(c.card_id, c.title, c.creation_date, c.created_by, c.verification_interval))
    id = 1
    for v in verifications:
        cursor.execute("INSERT INTO Verifications VALUES({}, {}, {}, {})".format(id, v.card_id, v.verification_date, v.verified_by))
        id += 1
    for u in users:
        cursor.execute("INSERT INTO Users VALUES({}, '{}')".format(u.user_id, u.user_name))
    con.commit()

    # create table with inner join of three tables
    cursor.execute("DROP TABLE IF EXISTS EnrichedCards")
    make_joined_table = "CREATE TABLE EnrichedCards AS \
                        SELECT Cards.*, Verifications.verification_date, Verifications.verified_by, Users.* \
                        FROM Cards \
                        INNER JOIN Verifications \
                        ON Cards.card_id=Verifications.card_id \
                        INNER JOIN Users \
                        ON Verifications.verified_by=Users.user_id"
    cursor.execute(make_joined_table)

    # perform verification count by user and only return users with more the k verifications
    count = "SELECT user_name, count(*) FROM EnrichedCards GROUP BY user_id HAVING count(*) > {}".format(VERIFICATIONS_NEEDED)
    cursor.execute(count)
    viewData = cursor.fetchall()
    # print(viewData)

    ctr = 0
    for name, counts in viewData:
        if counts > verifications_needed:
            print("Name: {} ----- {}".format(name, counts))
            ctr += 1
    print("SQL - Total Users with greater than {} verifications: {}".format(verifications_needed, ctr))

    con.commit()
    

def main():
    users = generate_users(NUM_USERS)
    cards = generate_cards(NUM_CARDS, NUM_USERS)
    verifications = generate_verifications(NUM_VERIFICATIONS, NUM_USERS, cards)

    # perform joins and counts of verification by user with Pandas
    pandas_verifications_by_users(users, cards, verifications, VERIFICATIONS_NEEDED)

    # perform joins and counts of verification by user with SQL calls
    sql_verifications_by_users(users, cards, verifications, VERIFICATIONS_NEEDED)

if __name__ == "__main__":
    main()