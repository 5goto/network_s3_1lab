import sqlite3
import pandas as pd


def database_init():
    con = sqlite3.connect("library.sqlite")

    f_damp = open('library.sql', 'r', encoding='utf-8-sig')
    damp = f_damp.read()
    f_damp.close()

    con.executescript(damp)
    con.commit()

    cursor = con.cursor()

    cursor.execute("SELECT * FROM author")
    print(cursor.fetchall())

    cursor.execute("SELECT * FROM reader")
    print(cursor.fetchall())

    con.close()


def second_task(p_genre: str, p_year: int):
    con = sqlite3.connect("library.sqlite")
    cursor = con.cursor()

    cursor.execute('''
     SELECT
     title,
     publisher_name,
     year_publication
     FROM book
     JOIN genre USING (genre_id)
     JOIN publisher USING (publisher_id)
     WHERE genre_name = :p_genre AND year_publication > :p_year
     order by year_publication desc, title
    ''', {"p_genre": p_genre, "p_year": p_year})

    data = cursor.fetchall()
    print(data)
    con.close()


def third_task(p_genre, p_year):
    con = sqlite3.connect("library.sqlite")
    cursor = con.cursor()

    cursor.execute('''
     SELECT
     title AS Название,
     publisher_name AS Издательство,
     year_publication AS Год
     FROM book
     JOIN genre USING (genre_id)
     JOIN publisher USING (publisher_id)
     WHERE genre_name = :p_genre AND year_publication > :p_year
     order by year_publication desc, title
    ''', {"p_genre": p_genre, "p_year": p_year})

    data = cursor.fetchall()

    names = []
    pubs = []
    years = []

    for item in data:
        names.append(item[0])
        pubs.append(item[1])
        years.append(item[2])

    print(pd.DataFrame({'Название': names,
                        'Издательство': pubs,
                        'Год': years}))
    con.close()