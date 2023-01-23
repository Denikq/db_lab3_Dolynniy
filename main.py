import psycopg2
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

username = 'postgres'
password = '00000000'
database = 'dol01_DB'
host = 'localhost'
port = '5432'

query_1 = '''
CREATE VIEW RatingByBooks AS
SELECT TRIM(book_name), rating_average 
FROM book, rating
WHERE book.rating_id = rating.rating_id
ORDER BY rating_average DESC
'''
query_2 = '''
CREATE VIEW RatingCountByAuthor AS
SELECT book.author_id, SUM(rating_count) 
FROM book, author, rating
WHERE book.author_id = author.author_id 
AND book.rating_id = rating.rating_id
GROUP BY book.author_id
'''
query_3 = '''
CREATE VIEW CountBookEveryYear AS
SELECT EXTRACT(YEAR FROM book_date) AS book_date, COUNT(*) AS number_of_books 
FROM book
GROUP BY EXTRACT(YEAR FROM book_date)
ORDER BY EXTRACT(YEAR FROM book_date)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
with conn:
    cur = conn.cursor()

    cur.execute('DROP VIEW IF EXISTS RatingByBooks')
    cur.execute(query_1)
    cur.execute('SELECT * FROM RatingByBooks')
    b_name = []
    r_average = []

    for row in cur:
        b_name.append(row[0])
        r_average.append(row[1])

    fig, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)


    bar_ax.set_title('Найпопулярніші книги')
    bar = bar_ax.bar(b_name, r_average)
    labels = []
    labels.append(b_name[0])  # назв забагато, виведемо тільки першу
    for i in range(len(b_name)-1):
        labels.append('')

    bar_ax.set_xticklabels(labels)
    bar_ax.yaxis.set_major_locator(ticker.MultipleLocator(1))


    cur.execute('DROP VIEW IF EXISTS RatingCountByAuthor')
    cur.execute(query_2)
    cur.execute('SELECT * FROM RatingCountByAuthor')
    a_id = []
    r_count = []

    for row in cur:
        a_id.append(row[0])
        r_count.append(row[1])

    max_number = 10
    ten = []
    for i in range(len(a_id)):
        if i < max_number:
            ten.append([a_id[i], r_count[i]])

        else:
            if i == max_number:
                ten.append(["Інші", 0])

            if r_count[i] > min([el[1] for el in ten[0:-1]]):
                index_el = [el[1] for el in ten[0:-1]].index(min([el[1] for el in ten[0:-1]]))
                ten[-1][1] += ten[index_el][1]
                ten[index_el] = [a_id[i], r_count[i]]

            else:
                ten[-1][1] += r_count[i]

    pie_ax.pie([el[1] for el in ten], labels=[el[0] for el in ten], autopct='%1.1f%%')
    pie_ax.set_title('Кількість оцінок роботи кожного автора/групи авторів (10)')


    cur.execute('DROP VIEW IF EXISTS CountBookEveryYear')
    cur.execute(query_3)
    cur.execute('SELECT * FROM CountBookEveryYear')
    year = []
    count = []

    for row in cur:
        year.append(int(row[0]))
        count.append(row[1])

    graph_ax.plot(year, count, marker='o')
    graph_ax.set_title('Кількість книг за кожний рік')
    graph_ax.yaxis.set_major_locator(ticker.MultipleLocator(20))
    graph_ax.xaxis.set_major_locator(ticker.MultipleLocator(10))


plt.show()
