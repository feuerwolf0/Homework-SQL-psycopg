import psycopg2 as psy
import json

def create_tables(connect):
    print('--------- Создание таблиц ---------')
    cur = connect.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS client (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(64) NOT NULL,
        last_name VARCHAR(64) NOT NULL,
        email VARCHAR(128) NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phone (
        id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES client(id),
        phone_number VARCHAR(30) UNIQUE CHECK (phone_number ~ '^[0-9]+$')
        );
    """)
    connect.commit()
    print('Таблица "client" и "phone" созданы')


def drop_tables(connect):
    print('--------- Удаление таблиц ---------')
    cur = connect.cursor()
    cur.execute("""
    DROP TABLE IF EXISTS phone;
    DROP TABLE IF EXISTS client;
    """)
    connect.commit()
    print('Таблицы "client" и "phone" удалены')

def add_client(connect, first_name, last_name, email):
    # Функция добавляет клиента
    print('--------- Добавление клиента ---------')
    cur = connect.cursor()
    cur.execute("""
        INSERT INTO client(first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id, first_name, last_name, email
    """, (first_name, last_name, email))
    print(*cur.fetchone(),'- клиент успешно добавлен')

def add_phone(connect, client_id, phone=None):
    # Функция добавляет номер телефона для существующего клиента
    print('--------- Добавление номера телефона ---------')

    # Получаю список имеющихся телефонов у клиента
    phones = get_phones(connect, client_id)
    # Проверяю есть ли такой телефон в добавленных. Если есть - завершаю функцию
    for ph in phones:
        if str(phone) == ph[2]:
            return print('[ОШИБКА]Такой номер телефона уже есть у клиента')
    if get_client(connect, client_id) != None:
        cur = connect.cursor()

        cur.execute("""
        INSERT INTO phone(client_id, phone_number)
        VALUES (%s, %s) RETURNING client_id, phone_number;
        """, (client_id, phone))

        print(*cur.fetchone(), '- номер телефона добавлен')
    else:
        print('[ОШИБКА]Клиента с id={} не существует'.format(client_id))

def update_client(connect, client_id, first_name=None, last_name=None, email=None, phones=None):
    # Функция обновляет данные существующего клиента

    print('--------- Обновление данных клиента ---------')
    # Проверка существует ли клиент
    if get_client(connect, client_id) != None:
        # Создаю словарь переданных аргументов
        params = {
            'first_name': first_name,
            'last_name': last_name,
            'email': email
        }
        query = []
        # Создаю список аргументов не NONE
        for key, val in params.items():
            if val != None:
                query.append(f"{key} = '{val}'") 
        # Формирую строку SET 
        query = ', '.join(query)

        cur = connect.cursor()
        # Формирую запрос. В SET передаю ранее созданную строку.
        execute = """
        UPDATE client 
        SET {} 
        WHERE id={} 
        RETURNING id, first_name, last_name, email""".format(query, client_id)
        cur.execute(execute)

        print(*cur.fetchone(), '- информация обновлена')

    # Проверка есть ли номера для редактирования
    if phones != None:
        # Удаляю старые номера клиента перед добавлением
        delete_phones(connect, client_id)
        # Цикл добавления новых телефонов для клиента
        for phone in phones:
            cur.execute("""
                INSERT INTO phone(client_id, phone_number)
                VALUES (%s, %s)
                RETURNING id, client_id, phone_number
            """, (client_id, str(phone)))
            print(cur.fetchone(), '- телефон добавлен')

def get_phones(connect, client_id):
    # Функция получает список номеров клиента
    # Возвращает список вида [(id, client_id, phone_number)] 

    cur = connect.cursor()
    cur.execute("""
    SELECT *
    FROM phone
    WHERE client_id = %s
    """, (client_id,))
    out = cur.fetchall()
    return out


def delete_phones(connect, client_id):
    # Функция удаляет все номера телефона клиента

    # Проверяю существует ли такой клиент
    if get_client(connect,client_id) != None:
        cur = connect.cursor()
        cur.execute("""
        DELETE FROM phone
        WHERE client_id = %s
        """,(client_id,))
        connect.commit()
        print(f'Телефоны клиента {client_id} удалены')
    else:
        print('[ОШИБКА]Клиента с id={} не существует'.format(client_id))


def delete_phone(connect, client_id, phone):
    # Функция удаляет 1 телефон клиента

    print('--------- Удаление номера телефона клиента ---------')
    # Проверяю существует ли такой клиент
    if get_client(connect,client_id) != None:
        cur = connect.cursor()
        phones = get_phones(connect, client_id)

        list_phones = []
        for ph in phones: list_phones.append(ph[2])

        if str(phone) in list_phones:
            cur.execute("""
            DELETE FROM phone
            WHERE (client_id = %s) AND (phone_number = %s)
            """,(client_id, str(phone)))
            connect.commit()
            print('Для клиента {} номер телефона: {} удален'.format(client_id, phone))
        else:
            print('У клиента {} нет такого номера телефона'.format(client_id))
    else:
        print('[ОШИБКА]Клиента с id={} не существует'.format(client_id))


def get_client(connect, client_id):
    # Функция получает информацию о клиенте
    # Возвращает кортеж вида (id, first_name, last_name, email) или None
    cur = connect.cursor()

    cur.execute("""
    SELECT *
    FROM client
    WHERE id = %s
    """, (client_id,))
    return cur.fetchone()


def delete_client(connect, client_id):
    # Функция удаляет клиента
    print('--------- Удаление клиента ---------')

    # Проверяю существует ли клиент
    if get_client(connect, client_id) != None:
        
        # Удаляю номера телефона клиента
        delete_phones(connect, client_id)

        cur = connect.cursor()
        cur.execute("""
        DELETE FROM client
        WHERE (id = %s)
        """, (client_id,))
        print('Клиента с id={} успешно удален'.format(client_id))
    else:
        print('[ОШИБКА]Клиента с id={} не существует'.format(client_id))


def find_client(connect, first_name=None, last_name=None, email=None, phone=None):
    # Функция ищет клиента по имени, фамилиии, email или телефону
    print('--------- Поиск клиента ---------')
     # Создаю словарь переданных аргументов
    params = {
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'p.phone_number': phone
    }
    where = []
    # Создаю список аргументов не NONE
    for key, val in params.items():
        if val != None:
            where.append(f"({key} = '{val}')")
    # Объединяю список в строку условия WHERE
    where = ' AND '.join(where)
    cur = connect.cursor()
    # Формирую запрос
    execute = """
        SELECT c.id, first_name, last_name, email, p.phone_number
        FROM client c
        LEFT JOIN phone p ON p.client_id = c.id
        WHERE {}
    """.format(where)
    # Делаю запрос
    cur.execute(execute)
    out = cur.fetchall()

    # Печатаю полученный результат построчно
    if out == []:
        print('Клиент не найден')
    else:
        for row in out:
            print(row)


def fill_data(connect):
    # Функция автоматически заполняет таблицу значениями из data.json
    with open('data.json', 'r') as json_file:
        data = json.load(json_file)
    id = 1
    for row in data:
        add_client(connect, row['first_name'], row['last_name'], row['email'])
        for a in row['phones']:
            add_phone(connect, id, a)
        id += 1

# Коннект для подключения к бд
with psy.connect(database='homework4', user='postgres', password='1234') as conn:
    # Удаляю ранее созданные таблицы
    drop_tables(conn)
    # Задание 1. Создаю новые таблицы
    create_tables(conn)
    # Заполняю таблицу тестовыми данными
    fill_data(conn)
    # Задание 2. Добавляю пользователя 
    add_client(conn, 'Кирилл', 'Полетаев', 'poletaev777@gmail.com')
    # Задание 3. Добавляю пользователю телефон
    add_phone(conn, 34, 892349120120)
    # Задание 3. Добавляю пользователю второй телефон
    add_phone(conn, 34, 4923449120120)
    # Задание 3. Пробую повторно добавить такой-же номер телефона. Выдаст ошибку
    add_phone(conn, 34, 4923449120120)
    # Получаю список телефонов пользователя с id 34
    print('--------- Список телефонов пользователя ---------')
    print(get_phones(conn, 34))
    # Задание 4. Обновляю данные о клиенте
    update_client(conn, 1, first_name='One', last_name='Two', phones=[13131313, 4234355, 3343])
    # Задание 5. Удаляю телефон клиента
    delete_phone(conn, 1, 4234355)
    # Задание 5. Пробую удалить несуществующий номер
    delete_phone(conn, 1, 137)
    # Задание 6. Удаляю клиента
    delete_client(conn, 1)
    # Задание 7. Ищу клиента по заданным критериям ( например 5 вариантов)
    find_client(conn, first_name='Ethan')
    find_client(conn, last_name='Wilson', phone=7770001111)
    find_client(conn, phone='8889990000')
    find_client(conn, first_name='Liam')
    find_client(conn, first_name='NULL')