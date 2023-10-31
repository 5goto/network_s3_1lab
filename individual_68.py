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
    WITH AuthorsWithZeroAvailability AS (
        select author_name as Количество from author as tmp
            join book_author using (author_id)
            join book using (book_id)
                where available_numbers = 0
                    group by author_name
        )
    
     select author_name as Автор, count(author_id) as Количество from author as tmp
            join book_author using (author_id)
            join book using (book_id)
                where available_numbers = 0
                    group by author_name
        union
    select author_name as Автор, 'нет' as Количество from author
            join book_author using (author_id)
            join book using (book_id)
                where available_numbers > 0 and author.author_name not in AuthorsWithZeroAvailability
                    group by author_name
                    order by Автор
    ''', con)

    print(df)
    con.close()


def task_3():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
    WITH Popularity AS (
    select book_id, count(book_id) as popularity
          from book_reader
          group by book_id
          having popularity = (select max(popularity) as max_value
                               from (select book_id, count(book_id) as popularity
                                     from book_reader
                                     group by book_id))
        )

    select title as Название, publisher_name as Издательсво, year_publication as Год, popularity as Количество
       from book
    join publisher using (publisher_id)
    join Popularity
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


def task_4():
    con = sqlite3.connect("library.sqlite")
    cursor = con.cursor()
    init_new_book_table()

    df = pd.read_sql('''
        SELECT title, genre_id, publisher_id, year_publication, available_numbers FROM book
    ''', con)
    print('before updating books:')
    print(df)

    cursor.execute('''
        UPDATE book
        SET available_numbers = available_numbers + ( -- вложенным запросом находим кол-во экземпляров
            SELECT amount
            FROM new_book
            NATURAL JOIN publisher
            WHERE book.title = new_book.title
            AND publisher_name = new_book.publisher_name
            AND book.year_publication = new_book.year_publication
        )
        WHERE EXISTS (  -- условный подзапрос на существование книги в библиотеке
            SELECT *  -- вернет хотя бы одну строку если книга существует
            FROM new_book
            NATURAL JOIN publisher
            WHERE book.title = new_book.title
            AND publisher_name = new_book.publisher_name
            AND book.year_publication = new_book.year_publication
        );
    ''')

    cursor.execute('''
        INSERT INTO book (title, genre_id, publisher_id, year_publication, available_numbers)
        SELECT
            new_book.title,
            NULL,
            publisher_name,
            new_book.year_publication,
            new_book.amount
        FROM new_book
        NATURAL JOIN publisher -- для таблицы publisher
        LEFT JOIN book ON
            book.title = new_book.title
            AND publisher_name = new_book.publisher_name
            AND book.year_publication = new_book.year_publication
        WHERE book.book_id IS NULL;
    ''')

    df = pd.read_sql('''
        SELECT title, genre_id, publisher_id, year_publication, available_numbers FROM book
        ''', con)
    print('after updating books:')
    print(df)

    con.close()


def task_5():
    con = sqlite3.connect("library.sqlite")

    df = pd.read_sql('''
    WITH BookAvg AS (
        SELECT
            bookTable.title,
            bookTable.genre_id,
            bookTable.publisher_id,
            bookTable.available_numbers,
            AVG(bookTable.available_numbers) OVER () AS avg_available
        FROM book bookTable
    )
    
    SELECT
        title,
        genreTable.genre_name,
        pubTable.publisher_name,
        CASE
            WHEN ROUND(available_numbers) = ROUND(avg_available) THEN 'равно среднему'
            WHEN ROUND(available_numbers) > ROUND(avg_available) THEN 'больше на ' || (ROUND(available_numbers) - (avg_available))
            ELSE 'меньше на ' || (ROUND(avg_available) - ROUND(available_numbers))
        END AS Отклонение
    FROM BookAvg ba
    JOIN genre genreTable ON ba.genre_id = genreTable.genre_id
    JOIN publisher pubTable ON ba.publisher_id = pubTable.publisher_id
    ORDER BY title, Отклонение;
    ''', con)

    print(df)
    con.close()
