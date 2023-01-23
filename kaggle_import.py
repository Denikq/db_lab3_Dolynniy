import psycopg2
import pandas as pd
import numpy as np


username = 'postgres'
password = '00000000'
database = 'dol01_DB'
host = 'localhost'
port = '5432'


query_0 = """
DELETE FROM Book;
DELETE FROM Author;
DELETE FROM Rating;
"""

query_author_1 = """
INSERT INTO Author(author_id, author_name_1) VALUES ('%s', '%s')
"""
query_author_2 = """
INSERT INTO Author(author_id, author_name_1, author_name_2) VALUES ('%s', '%s', '%s')
"""
query_author_3 = """
INSERT INTO Author(author_id, author_name_1, author_name_2, author_name_3) VALUES ('%s', '%s', '%s', '%s')
"""

query_rating = """
INSERT INTO Rating(rating_id, rating_count, rating_average) VALUES ('%s', '%s', '%s')
"""


query_book_a1 = """
INSERT INTO Book(book_id, book_name, book_pages, book_date, author_id, rating_id) VALUES ('%s', '%s', '%s', '%s', 
(SELECT author_id FROM Author 
WHERE author_name_1 = '%s' AND author_name_2 IS NULL AND author_name_3 IS NULL),
(SELECT rating_id FROM Rating WHERE rating_count = '%s' AND rating_average = '%s'))
"""
query_book_a2 = """
INSERT INTO Book(book_id, book_name, book_pages, book_date, author_id, rating_id) VALUES ('%s', '%s', '%s', '%s', 
(SELECT author_id FROM Author 
WHERE author_name_1 = '%s' AND author_name_2 = '%s' AND author_name_3 IS NULL),
(SELECT rating_id FROM Rating WHERE rating_count = '%s' AND rating_average = '%s'))
"""
query_book_a3 = """
INSERT INTO Book(book_id, book_name, book_pages, book_date, author_id, rating_id) VALUES ('%s', '%s', '%s', '%s', 
(SELECT author_id FROM Author 
WHERE author_name_1 = '%s' AND author_name_2 = '%s' AND author_name_3 = '%s'),
(SELECT rating_id FROM Rating WHERE rating_count = '%s' AND rating_average = '%s'))
"""


data = pd.read_csv(r'books.csv') #encoding="ISO-8859-1"

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur.execute(query_0)

    df = pd.DataFrame(data, columns=['title', 'authors', 'average_rating', 'ratings_count', 'num_pages', 'publication_date'])
    df = df.astype(object).replace(np.nan, 'NULL')  # заміна nan на 'NULL'

    cur1 = conn.cursor()
    authors = df['authors'].tolist()

    authors_apart = []
    for el in authors:
        authors_apart.append(el.replace('\'', '').split('/'))

    authors_apart_u = []

    for el in authors_apart:
        el = el[0:3]
        if el not in authors_apart_u:
            authors_apart_u.append(el)

    i = 0
    for el in authors_apart_u:
        if len(el) == 1:
            query = query_author_1 % (i, el[0])
        elif len(el) == 2:
            query = query_author_2 % (i, el[0], el[1])
        else:
            query = query_author_3 % (i, el[0], el[1], el[2])

        cur1.execute(query)
        i += 1
    conn.commit()


    cur2 = conn.cursor()

    ratings_count = df['ratings_count'].tolist()
    average_rating = df['average_rating'].tolist()

    unique = []
    for i in range(len(ratings_count)):
        el = [ratings_count[i], average_rating[i]]
        if el not in unique:
            unique.append(el)


    for i in range(len(unique)):
        query = query_rating % (i, unique[i][0], unique[i][1])
        cur2.execute(query)
    conn.commit()


    cur3 = conn.cursor()
    title = df['title'].tolist()
    num_pages = df['num_pages'].tolist()
    publication_date = df['publication_date'].tolist()

    publication_date_reverse = []   # пострібно поміняти день і місяць місцями для sql

    for el in publication_date:
        x = el.split('/')
        x_0 = x[0]
        x[0] = x[1]
        x[1] = x_0
        el = '/'.join(x)
        publication_date_reverse.append(el)

    unique = []

    for i in range(len(title)):
        title[i] = title[i].replace('\'', '')
        el = [title[i], num_pages[i], publication_date_reverse[i], authors_apart[i], ratings_count[i], average_rating[i]]
        if el not in unique:
            unique.append(el)

    for i in range(len(unique)):
        if len(unique[i][3]) == 1:
            query = query_book_a1 % (i, unique[i][0], unique[i][1], unique[i][2],
                                     unique[i][3][0],
                                     unique[i][4], unique[i][5])
        elif len(unique[i][3]) == 2:
            query = query_book_a2 % (i, unique[i][0], unique[i][1], unique[i][2],
                                     unique[i][3][0], unique[i][3][1],
                                     unique[i][4], unique[i][5])
        else:
            query = query_book_a3 % (i, unique[i][0], unique[i][1], unique[i][2],
                                     unique[i][3][0], unique[i][3][1], unique[i][3][2],
                                     unique[i][4], unique[i][5])
        cur3.execute(query)
    conn.commit()
