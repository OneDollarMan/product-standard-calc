import sys
import duckdb
import polars as pl
from polars import col
from tqdm import tqdm


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
        total_etalon_skus = 0
        selected_items = set()

        if len(subset) <= capacity:
            # Помечаем все товары в subset как эталоны
            df = df.with_columns(
                pl.when(
                    (col('Store') == store) &
                    (col('equip_type') == equip)
                ).then(1).otherwise(col('etalon')).alias('etalon')
            )

            # Обновляем accumulated_part для всех категорий в subset
            cat4_sums = (
                subset
                .group_by('cat4')
                .agg(pl.col('part_sales').sum().alias('total_part_sales'))
            )

            # Добавляем accumulated_part для каждой категории
            for cat4_row in cat4_sums.iter_rows(named=True):
                df = df.with_columns(
                    pl.when(
                        (col('Store') == store) &
                        (col('cat4') == cat4_row['cat4'])
                    ).then(col('accumulated_part') + cat4_row['total_part_sales'])
                    .otherwise(col('accumulated_part'))
                    .alias('accumulated_part')
                )
            continue

        # Пока не достигнута квота и есть товары для выбора
        while total_etalon_skus < capacity:
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
            if total_etalon_skus < capacity:
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

                total_etalon_skus += 1
                selected_items.add(item)
            else:
                break
    return df.to_pandas()


def main():
    conn = duckdb.connect()
    df_data = pl.from_arrow(conn.execute(query).arrow())
    df_equip = pl.from_arrow(
        conn.execute("SELECT * FROM read_csv_auto('../static/capacity.csv',header = True )").arrow()
    )
    result_df = select_etalon(df_data, df_equip)
    result_df.to_csv('etalon_selection_result.csv', index=False, sep=';', encoding='utf-8-sig')
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
