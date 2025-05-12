import duckdb
import polars as pl
from tqdm import tqdm
import pandas as pd

def optimize_query():
    """
    Оптимизированный SQL-запрос с использованием более эффективных техник DuckDB
    """
    query = """
WITH avg_sales_data AS (
    SELECT * REPLACE(
        CAST(REPLACE(Avg_sales_pc, ',', '.') AS DECIMAL(10,2)) AS Avg_sales_pc
    )
    FROM read_csv_auto(
        '../static/avg_sales.csv', 
        header=True,
        delim=';'
    )
),
category_data AS (
    SELECT 
        final_item_id, 
        cat.type, 
        "Группа 4" AS cat4 
    FROM read_csv_auto(
        '../static/map_category_sku.csv', 
        header=True
    ) AS cat
    LEFT JOIN read_csv_auto(
        '../static/mapping_cat4.csv', 
        header=True
    ) AS cat_4 ON cat_4.Category4 = cat.type
),
capacity AS (
   SELECT *
   FROM  read_csv_auto(
        '../static/capacity.csv', 
        header=True
    )
),
cat_equip AS (
   SELECT *
   FROM  read_csv_auto(
        '../static/cat_equip.csv', 
        header=True
    )
)
SELECT a.*, ce."Тип оборудования" as equip_type, "Квота" as Capacity FROM (
SELECT 
    a.*, 
    c.type, 
    c.cat4, 
    ROW_NUMBER() OVER (PARTITION BY a.Store, c.type ORDER BY a.Avg_sales_pc DESC) AS Row_num,
    SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type) AS Sum_sales,
    a.Avg_sales_pc / SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type) AS part_sales
FROM avg_sales_data a
LEFT JOIN category_data c ON c.final_item_id = a.Item) as a
LEFT JOIN cat_equip ce ON a.cat4 = "Группа 4" 
LEFT JOIN capacity cap ON ce."Тип оборудования" = cap."Тип оборудования" AND Store = "Код Склада" 
"""
    return query


def select_etalon_optimized(conn, df):
    """
    Оптимизированный алгоритм выбора эталона с использованием Polars
    """
    # Преобразуем Pandas DataFrame в Polars для эффективности
    pl_df = pl.from_pandas(df)

    # Группируем уникальные магазины и оборудование
    stores_equip = (pl_df
                    .select(['Store', 'equip_type', 'Capacity'])
                    .unique()
                    )

    # Подготовим пустой массив для результатов
    etalon_results = []

    for row in tqdm(iterable=stores_equip.iter_rows(named=True), total=len(stores_equip)):
        store, equip, capacity = row['Store'], row['equip_type'], row['Capacity']
        if capacity is None:
            continue

        # Фильтруем данные текущего магазина и оборудования
        subset = pl_df.filter(
            (pl.col('Store') == store) &
            (pl.col('equip_type') == equip)
        )

        # Сортируем с приоритетом групп и порядка
        sorted_subset = (subset
        .sort(['cat4', 'Row_num'])
        .with_row_index('rank')
        .with_columns([
            pl.col('part_sales').cum_sum().alias('cumulative_part')
        ])
        )

        # Выбираем эталоны с учетом квоты
        etalons = (sorted_subset
                   .filter(pl.col('cumulative_part') <= capacity)
                   .select('Item')
                   .to_series()
                   .to_list()
                   )

        # Добавляем результаты
        store_etalons = [(store, item) for item in etalons]
        etalon_results.extend(store_etalons)

    etalons_df = pl.DataFrame(etalon_results, schema=['Store', 'Item'], orient='row')
    print(len(etalons_df))
    print(len(pl_df))
    result = (
        pl_df.join(
            etalons_df.unique().with_columns(pl.lit(1).alias('etalon')),
            on=['Store', 'Item'],
            how='left'
        )
        .with_columns(
            pl.col('etalon').fill_null(0).cast(pl.Int8)
        )
    )
    return result.to_pandas()


def main():
    # Подключение к DuckDB с оптимизацией
    conn = duckdb.connect(database=':memory:', config={'threads': 8})

    # Преобразуем CSV в более быстрый Parquet заранее
    # csv_to_parquet() # Раскомментировать при первом запуске

    # Выполняем оптимизированный запрос
    query = optimize_query()
    df = conn.execute(query).df()

    # Применяем оптимизированный алгоритм выбора эталона
    result_df = select_etalon_optimized(conn, df)

    print('saving')
    # Сохраняем результат
    result_df.to_csv('etalon_selection_result.csv', index=False, sep=';', encoding='utf-8-sig')

    conn.close()


def csv_to_parquet():
    """
    Утилита для конвертации CSV в Parquet для ускорения чтения
    """
    conn = duckdb.connect()
    csvs = [
        'avg_sales.csv',
        'map_category_sku.csv',
        'cat_equip.csv',
        'capacity.csv'
    ]
    for csv_file in csvs:
        parquet_file = csv_file.replace('.csv', '.parquet')
        query = f"""
        COPY (
            SELECT * FROM read_csv_auto('../static/{csv_file}', header=True)
        ) TO '../static/{parquet_file}' (FORMAT 'parquet')
        """
        conn.execute(query)

    conn.close()


if __name__ == "__main__":
    main()
