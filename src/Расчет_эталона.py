import duckdb
import polars as pl
from polars import col
from tqdm import tqdm

# Подключение к DuckDB
conn = duckdb.connect()

# Ваш исходный запрос
query = """
        WITH avg_sales_data AS (SELECT * REPLACE(
        CAST(REPLACE(Avg_sales_pc, ',', '.') AS DECIMAL(10,2)) AS Avg_sales_pc
    ) \
                                FROM read_csv_auto( \
                                        '../static/avg_sales.csv', \
                                        header = True, \
                                        delim = ';' \
                                     )),
             category_data AS (SELECT final_item_id, \
                                      cat.type, \
                                      "Группа 4" AS cat4 \
                               FROM read_csv_auto( \
                                            '../static/map_category_sku.csv', \
                                            header = True \
                                    ) AS cat \
                                        LEFT JOIN read_csv_auto( \
                                       '../static/mapping_cat4.csv', \
                                       header = True \
                                                  ) AS cat_4 ON cat_4.Category4 = cat.type),
             capacity AS (SELECT * \
                          FROM read_csv_auto( \
                                  '../static/capacity.csv', \
                                  header = True \
                               )),
             cat_equip AS (SELECT * \
                           FROM read_csv_auto( \
                                   '../static/cat_equip.csv', \
                                   header = True \
                                ))
        SELECT a.*, ce."Тип оборудования" as equip_type, "Квота" as Capacity \
        FROM (SELECT a.*, \
                     c.type, \
                     c.cat4, \
                     ROW_NUMBER() OVER (PARTITION BY a.Store, c.type ORDER BY a.Avg_sales_pc DESC) AS Row_num, \
                     SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type)                       AS Sum_sales, \
                     a.Avg_sales_pc / SUM(a.Avg_sales_pc) OVER (PARTITION BY a.Store, c.type)      AS part_sales \
              FROM avg_sales_data a \
                       LEFT JOIN category_data c ON c.final_item_id = a.Item) as a \
                 LEFT JOIN cat_equip ce ON a.cat4 = "Группа 4" \
                 LEFT JOIN capacity cap ON ce."Тип оборудования" = cap."Тип оборудования" AND Store = "Код Склада" \
        """

# Получаем исходные данные
df = pl.from_arrow(conn.execute(query).arrow())
df_equip = pl.from_arrow(conn.execute("SELECT * FROM read_csv_auto('../static/capacity.csv',header = True )").arrow())


# Алгоритм выбора эталона
def select_etalon(df, df_equip):
    # Инициализация полей
    df = df.with_columns([
        pl.lit(0).cast(pl.Int8).alias('etalon'),
        pl.lit(0.0).alias('accumulated_part')
    ])

    # Уникальные пары магазин-оборудование
    df_equip_unique = df_equip.unique()

    for row in tqdm(iterable=df_equip_unique.iter_rows(named=True), total=len(df_equip_unique)):
        store = row['Код Склада']
        equip = row['Тип оборудования']
        capacity = row['Квота']
        if capacity is None:
            continue

        # Фильтруем данные по текущему магазину и оборудованию
        subset = df.filter(
            (col('Store') == store) &
            (col('equip_type') == equip)
        )

        # Инициализация
        total_etalon = 0
        selected_items = set()

        total_iterations = 0
        # Пока не достигнута квота и есть товары для выбора
        while total_etalon < capacity and len(selected_items) < len(subset):
            total_iterations += 1
            # Находим категорию с минимальной накопленной суммой
            cat4_stats = (
                subset.filter(~col('Item').is_in(selected_items))
                .filter(col('cat4').is_not_null())
                .group_by('cat4')
                .agg(col('accumulated_part').sum().alias('sum_accumulated'))
                .sort('sum_accumulated')
            )

            if cat4_stats.is_empty():
                break

            cat4_min = cat4_stats['cat4'][0]

            # Выбираем товар с лучшим рангом из найденной категории
            candidate = (
                subset.filter(
                    (col('cat4') == cat4_min) &
                    (~col('Item').is_in(selected_items))
                )
                .sort('Row_num')
                .head(1)
            )

            if candidate.is_empty():
                break

            item = candidate['Item'][0]
            part_sales = candidate['part_sales'][0]

            # Проверяем, не превысит ли добавление квоту
            if total_etalon + part_sales <= capacity:
                # Обновляем etalon и accumulated_part
                df = df.with_columns(
                    pl.when(
                        (col('Store') == store) &
                        (col('Item') == item)
                    ).then(1).otherwise(col('etalon')).alias('etalon')
                )

                df = df.with_columns(
                    pl.when(
                        (col('Store') == store) &
                        (col('cat4') == cat4_min)
                    ).then(col('accumulated_part') + part_sales)
                    .otherwise(col('accumulated_part'))
                    .alias('accumulated_part')
                )

                total_etalon += part_sales
                selected_items.add(item)
            else:
                break
    return df


# Применяем алгоритм

result_df = select_etalon(df, df_equip)

# Показываем результат
print(result_df.head(20))

# Сохраняем результат
result_df.to_csv('etalon_selection_result.csv', index=False, sep=';', encoding='utf-8-sig')

# Закрываем соединение
conn.close()