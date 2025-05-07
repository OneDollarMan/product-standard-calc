from db import create_db_and_tables, session_maker
from service import aggregate_data, calculate_standards


def main():
    create_db_and_tables()
    with session_maker() as session:
        aggregate_data(session)
        calculate_standards(session)
        session.commit()


if __name__ == '__main__':
    main()