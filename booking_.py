import psycopg2
import csv
import pandas as pd
import logging


def extract_data(file_path):

    # Проверка данных
    csv_data = pd.read_csv(file_path)
    try:
        validate_data(csv_data)
        print("Данные корректны")
    except ValueError as e:
        print(f"Ошибка валидации данных: {e}")

    # Чтение данных из CSV файла
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок
        data = [tuple(row) for row in reader]

    return data


def create_tables(cursor):

    cursor.execute("CREATE SCHEMA IF NOT EXISTS booking")

    # booking_origin таблица
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS booking.booking_origin (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
        """
    )

    # trip_type таблица
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS booking.trip_type (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
        """
    )

    # flight_day таблица
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS booking.flight_day (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
        """
    )

    # route таблица
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS booking.route (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
        """
    )

    # bookings_details таблица
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS booking.bookings_details (
            id SERIAL PRIMARY KEY,
            num_passengers INT,
            sales_channel VARCHAR(255),
            trip_type_id INT REFERENCES booking.trip_type(id),
            purchase_lead INT,
            length_of_stay INT,
            flight_hour INT,
            flight_day_id INT REFERENCES booking.flight_day(id),
            route_id INT REFERENCES booking.route(id),
            booking_origin_id INT REFERENCES booking.booking_origin(id),
            wants_extra_baggage BOOLEAN,
            wants_preferred_seat BOOLEAN,
            wants_in_flight_meals BOOLEAN,
            flight_duration FLOAT,
            booking_complete BOOLEAN
        )
        """
    )


def get_or_create(cursor, table, name):
    cursor.execute(f"SELECT id FROM booking.{table} WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        f"INSERT INTO booking.{table} (name) VALUES (%s) RETURNING id", (name,))
    result = cursor.fetchone()
    return result[0]


def validate_data(df):

    # Проверка на пустые значения
    if df.isnull().values.any():
        raise ValueError("Данные содержат пустые значения")

    # Проверка на дублирующиеся записи
    if df.duplicated().any():
        raise ValueError("Найдены дублирующиеся записи")


def save_data_tables(cursor, data):

    for row in data:
        trip_type_id = get_or_create(cursor, "trip_type", row[2])
        flight_day_id = get_or_create(cursor, "flight_day", row[6])
        route_id = get_or_create(cursor, "route", row[7])
        booking_origin_id = get_or_create(cursor, "booking_origin", row[8])

        cursor.execute(
            """
            INSERT INTO booking.bookings_details (
                num_passengers, sales_channel, trip_type_id, purchase_lead, length_of_stay, flight_hour, flight_day_id, 
                route_id, booking_origin_id, wants_extra_baggage, wants_preferred_seat, wants_in_flight_meals, flight_duration, booking_complete
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (row[0], row[1], trip_type_id, row[3], row[4], row[5], flight_day_id,
             route_id, booking_origin_id, row[9], row[10], row[11], row[12], row[13])
        )


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler("etl_process.log"),
                                  logging.StreamHandler()])

    file_path = r'D:\Teach\ВКР\PythonВКР\vkr\Kod\customer_booking.csv'

    # Подключение к базе данных PostgreSQL
    try:
        conn = psycopg2.connect(
            host="dev1c-5",
            database="booking",
            user="uak",
            password="uak"
        )
        logging.info("Соединение с базой данных установлено")
    except Exception as e:
        logging.error(f"Не удалось установить соединение с базой данных: {e}")

    cursor = conn.cursor()

    try:
        create_tables(cursor)
        data = extract_data(file_path)
        save_data_tables(cursor, data)
        logging.info("Данные успешно загружены в базу данных")
    except Exception as e:
        logging.error(f"Не удалось загрузить данные в базу данных: {e}")

    conn.commit()
    cursor.close()
    conn.close()
