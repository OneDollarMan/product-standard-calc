from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
from db import engine


def import_data_from_files(session: AsyncSession):
    equip_q = f"copy equipments FROM 'static/capacity.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL '')"


    session.execute(text(equip_q))
    session.commit()

def aggregate_data(session: AsyncSession):
    # Агрегация продуктов и оборудования
    trunc_agg_query = "DELETE FROM agg_data;"
    query = """
        INSERT INTO agg_data (
            storage_id,
            category_id,
            product_id,
            rating,
            sale_pcs,
            sale_share,
            cumulative_share,
            equipment_type,
            capacity
        )
        SELECT
            p.storage_id,
            p.category_id,
            p.product_id,
            p.rating,
            p.sale_pcs,
            p.sale_share,
            p.cumulative_share,
            c.equipment_type,
            e.capacity
        FROM products p
        JOIN categories c
            ON p.category_id = c.category_id
        JOIN equipments e
            ON p.storage_id = e.storage_id AND c.equipment_type = e.equipment_type;
    """

    session.execute(text(trunc_agg_query))
    session.execute(text(query))
    session.flush()


def calculate_standards(session: AsyncSession):
    # Вычисление эталонов
    equipments_df = pd.read_sql("SELECT * FROM equipments", engine)
    agg_data_df = pd.read_sql("SELECT * FROM agg_data", engine)
    selected_df = select_products(equipments_df, agg_data_df)

    trunc_selected_query = "TRUNCATE TABLE selected_products;"
    session.execute(text(trunc_selected_query))
    session.flush()

    selected_df.to_sql(
        name='selected_products',
        con=engine,
        if_exists='append',
        index=False
    )

    trunc_standards_query = "TRUNCATE TABLE product_standards;"
    calc_standards_query = """
        INSERT INTO product_standards (
            storage_id, category_id, amount
        )
        SELECT storage_id, category_id, COUNT(*) FROM selected_products GROUP BY storage_id, category_id;
    """

    session.execute(text(trunc_standards_query))
    session.execute(text(calc_standards_query))
    session.flush()


def select_products(equipments_df: pd.DataFrame, agg_data_df: pd.DataFrame) -> pd.DataFrame:
    # Выбор продуктов под оборудование
    selected_rows = []

    # Итерируемся по каждому оборудованию
    for _, equipment in equipments_df.iterrows():
        equip_type = equipment["equipment_type"]
        storage_id = equipment["storage_id"]
        capacity = equipment["capacity"]

        # Фильтрация товаров по данному оборудованию
        equip_products = agg_data_df[
            (agg_data_df["equipment_type"] == equip_type) &
            (agg_data_df["storage_id"] == storage_id)
            ].copy()

        # Добавим колонку флага выбора
        equip_products["selected"] = False

        selected_count = 0
        while selected_count < capacity:
            # Выбираем из каждой группы (category_id) по товару с максимальным рейтингом, который ещё не выбран
            top_per_group = (
                equip_products[~equip_products["selected"]]
                .sort_values(["category_id", "rating"], ascending=[True, False])
                .drop_duplicates(subset=["category_id"])
            )

            if top_per_group.empty:
                break

            # Из отобранных выбираем товар с минимальной cumulative_share
            chosen = top_per_group.loc[top_per_group["cumulative_share"].idxmin()]

            # Обновляем метку выбранного товара
            equip_products.loc[
                (equip_products["category_id"] == chosen["category_id"]) &
                (equip_products["product_id"] == chosen["product_id"]),
                "selected"
            ] = True

            selected_rows.append(chosen)
            selected_count += 1

    return pd.DataFrame(selected_rows)
