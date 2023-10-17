import sqlite3
import pandas as pd


def task_1():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
     select title as Название,
        reader_name as Читатель,
        julianday(return_date) - julianday(borrow_date) + 1 as Количество_дней
    from book_reader
        join reader using (reader_id)
        join book using (book_id)
    where return_date IS NOT NULL and Количество_дней > 14
    order by Название, Количество_дней desc, Читатель
    ''', con)

    print(df)
    con.close()


def task_2():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
         select author_name as Автор, count(author_id) as Количество from author as tmp
        join book_author using (author_id)
        join book using (book_id)
            where available_numbers = 0
                group by author_name
    union
    select author_name as Автор, 'нет' as Количество from author
        join book_author using (author_id)
        join book using (book_id)
            where available_numbers > 0 and author.author_name not in (
                    select author_name as Количество from author as tmp
                        join book_author using (author_id)
                        join book using (book_id)
                            where available_numbers = 0
                                group by author_name
                )
                group by author_name
                order by Автор
    ''', con)

    print(df)
    con.close()


def task_3():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
        select title as Название, publisher_name as Издательсво, year_publication as Год, popularity as Количество
       from book
    join publisher using (publisher_id)
    join (select book_id, count(book_id) as popularity
          from book_reader
          group by book_id
          having popularity = (select max(popularity) as max_value
                               from (select book_id, count(book_id) as popularity
                                     from book_reader
                                     group by book_id)))
    using (book_id)
    order by Название, Издательсво, Год desc
    ''', con)

    print(df)
    con.close()


def init_new_book_table():
    con = sqlite3.connect("library.sqlite")

    con.executescript('''
            DROP TABLE IF EXISTS new_book;
            CREATE TABLE new_book (
            title VARCHAR(80),
            publisher_name VARCHAR(80),
            year_publication INT,
            amount INT
            );

            INSERT INTO new_book(title, publisher_name, year_publication, amount)
            VALUES
            ("Вокруг света за 80 дней", "ДРОФА", 2019, 2),
            ("Собачье сердце", "АСТ", 2020, 3),
            ("Таинственный остров","РОСМЭН", 2015, 1),
            ("Евгений Онегин", "АЛЬФА-КНИГА", 2020, 4),
            ("Герой нашего времени", "АСТ", 2017, 1);
        ''')
    con.commit()
    con.close()


def get_existed_books():
    con = sqlite3.connect("library.sqlite")
    cursor = con.cursor()
    cursor.execute('''
             select * from new_book
                except
                select * from new_book
                where title in
                (select title from
                    (select title, publisher_name, year_publication from new_book
                        except
                    select title, publisher_name, year_publication from book
                        join publisher using (publisher_id)))
            ''')
    return cursor.fetchall()


def get_not_existed_books():
    con = sqlite3.connect("library.sqlite")
    cursor = con.cursor()
    cursor.execute('''
        select * from new_book
            where title in
            (select title from
                (select title, publisher_name, year_publication from new_book
                    except
                select title, publisher_name, year_publication from book
                    join publisher using (publisher_id)))
            ''')
    return cursor.fetchall()


def update_book_table(con, task):
    sql = ''' UPDATE book
              SET available_numbers = ?
              WHERE title = ?'''
    cur = con.cursor()
    cur.execute(sql, task)
    con.commit()


def create_book(con, task):
    sql = ''' INSERT INTO book(title, genre_id, publisher_id, year_publication, available_numbers)
              VALUES(?, ?, ?, ?, ?) '''
    cur = con.cursor()
    cur.execute(sql, task)
    con.commit()

    return cur.lastrowid


def get_publisher_book_id(con, task):
    sql = ''' SELECT publisher_id FROM PUBLISHER
                    WHERE publisher_name = ?'''
    cur = con.cursor()
    cur.execute(sql, task)
    return cur.fetchall()[0][0]


def task_4():
    con = sqlite3.connect("library.sqlite")
    init_new_book_table()

    existed_data = get_existed_books()
    not_existed_data = get_not_existed_books()

    df = pd.read_sql('''
        SELECT title, genre_id, publisher_id, year_publication, available_numbers FROM book
    ''', con)
    print('before updating books:')
    print(df)

    with con:
        for new_book in existed_data:
            update_book_table(con, (new_book[3], new_book[0]))
        for new_book in not_existed_data:
            create_book(con, (new_book[0],
                              '',
                              get_publisher_book_id(con, (new_book[1],)),
                              new_book[2],
                              new_book[3]))

    df = pd.read_sql('''
            SELECT title, genre_id, publisher_id, year_publication, available_numbers FROM book
        ''', con)
    print('after updating books:')
    print(df)

    con.close()


def task_5():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
        select title as Название,
        genre_name as Жанр,
        publisher_name as Издатель,
            'меньше на ' || avg(abs(available_numbers - (select round(avg(available_numbers)) from book)))
            over (partition by title) as Отклонение
        from book
            join publisher using (publisher_id)
            join genre using (genre_id)
        where (available_numbers - (select round(avg(available_numbers)) from book)) < 0
union
select title as Название,
        genre_name as Жанр,
        publisher_name as Издатель,
            'больше на ' || avg(abs(available_numbers - (select round(avg(available_numbers)) from book)))
            over (partition by title) as Отклонение
        from book
            join publisher using (publisher_id)
            join genre using (genre_id)
        where (available_numbers - (select round(avg(available_numbers)) from book)) > 0
union
select title as Название,
        genre_name as Жанр,
        publisher_name as Издатель,
            'равно среднему'
        from book
            join publisher using (publisher_id)
            join genre using (genre_id)
        where (available_numbers - (select round(avg(available_numbers)) from book)) = 0
    ''', con)

    print(df)
    con.close()
