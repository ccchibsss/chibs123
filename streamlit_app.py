import os
import time
import polars as pl
import duckdb
import streamlit as st
from pathlib import Path
import warnings
import logging

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

# Константы
DATA_DIR = Path("./auto_parts_data")
DB_PATH = DATA_DIR / "catalog.duckdb"
EXCEL_ROW_LIMIT = 1_000_000

class AutoPartsCatalog:
    def __init__(self):
        self.setup_directories()
        self.conn = duckdb.connect(str(DB_PATH))
        self.setup_database()

    def setup_directories(self):
        DATA_DIR.mkdir(exist_ok=True)

    def setup_database(self):
        # Создаём таблицы с автоинкрементом для id
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS data_files (
                id INTEGER PRIMARY KEY,
                filename VARCHAR,
                file_type VARCHAR,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version INTEGER DEFAULT 1
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe_data (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts_data (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                artikul VARCHAR,
                brand VARCHAR,
                multiplicity INTEGER,
                barcode VARCHAR,
                length DOUBLE, 
                width DOUBLE,
                height DOUBLE, 
                weight DOUBLE,
                image_url VARCHAR,
                dimensions_str VARCHAR,
                description VARCHAR,
                price DECIMAL(10, 2),
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR PRIMARY KEY,
                price DECIMAL(10, 2),
                markup DECIMAL(5, 2)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fixed_prices (
                artikul_norm VARCHAR PRIMARY KEY,
                fixed_price DECIMAL(10, 2)
            )
        """)

    # --------- Методы для хранения информации о файлах ---------
    def update_data_file_record(self, filename, file_type):
        cursor = self.conn.execute(
            "SELECT id, version FROM data_files WHERE filename = ? AND file_type = ?",
            (filename, file_type)
        )
        row = cursor.fetchone()
        if row:
            new_version = row[1] + 1
            self.conn.execute(
                "UPDATE data_files SET uploaded_at = CURRENT_TIMESTAMP, version = ? WHERE id = ?",
                (new_version, row[0])
            )
        else:
            # Вставляем новую запись без id, он автоматически присвоится
            self.conn.execute(
                "INSERT INTO data_files (filename, file_type) VALUES (?, ?)", (filename, file_type)
            )

    # --------- Методы для загрузки и обновления данных ---------
    def load_price_list(self, file_path, markup=0.0):
        filename = Path(file_path).name
        self.update_data_file_record(filename, 'price_list')
        df = pl.read_excel(file_path)
        df = df.rename({
            'Артикул': 'artikul',
            'Кол-во': 'quantity',
            'Бренд': 'brand',
            'Цена': 'price'
        })
        df = df.with_columns(
            artikul_norm=self.normalize_key(pl.col('artikul')),
            brand_norm=self.normalize_key(pl.col('brand'))
        )
        df = df.select(['artikul_norm', 'price'])
        df = df.unique(subset=['artikul_norm'])
        self._upsert_prices(df)
        if markup != 0.0:
            self.apply_markup(markup)

    def load_fixed_prices(self, file_path):
        filename = Path(file_path).name
        self.update_data_file_record(filename, 'fixed_prices')
        df = pl.read_excel(file_path)
        df = df.rename({'Артикул': 'artikul', 'Цена РРЦ': 'fixed_price'})
        df = df.with_columns(
            artikul_norm=self.normalize_key(pl.col('artikul'))
        )
        df = df.select(['artikul_norm', 'fixed_price'])
        df = df.unique(subset=['artikul_norm'])
        self._upsert_fixed_prices(df)

    def exclude_rows(self, exclusion_strings):
        """
        Исключает строки по совпадениям из description.
        """
        patterns = exclusion_strings.split('|')
        for pattern in patterns:
            self.conn.execute(f"""
                DELETE FROM parts_data
                WHERE LOWER(description) LIKE '%{pattern.lower()}%'
            """)

    def apply_markup(self, markup_value):
        """
        Применяет наценку ко всему прайсу.
        """
        self.conn.execute(f"""
            UPDATE prices
            SET price = price * (1 + {markup_value})
        """)

    def _upsert_prices(self, df):
        """
        Обновляет или вставляет цены.
        """
        self.conn.register("temp_prices", df.to_arrow())
        # Используем UPSERT через ON CONFLICT
        self.conn.execute("""
            INSERT INTO prices (artikul_norm, price)
            SELECT artikul_norm, price FROM temp_prices
            ON CONFLICT (artikul_norm) DO UPDATE SET price=excluded.price
        """)
        self.conn.unregister("temp_prices")
    
    def _upsert_fixed_prices(self, df):
        """
        Обновляет или вставляет фиксированные цены.
        """
        self.conn.register("temp_fixed", df.to_arrow())
        self.conn.execute("""
            INSERT INTO fixed_prices (artikul_norm, fixed_price)
            SELECT artikul_norm, fixed_price FROM temp_fixed
            ON CONFLICT (artikul_norm) DO UPDATE SET fixed_price=excluded.fixed_price
        """)
        self.conn.unregister("temp_fixed")

    # --------- Методы для обработки и чтения файлов ---------
    def detect_columns(self, actual_columns, expected_columns):
        """
        Детектит соответствие колонок.
        """
        mapping = {}
        for exp in expected_columns:
            for act in actual_columns:
                if act.lower().find(exp.lower()) != -1:
                    mapping[act] = exp
        return mapping

    def clean_values(self, series):
        return series.fill_null("").cast(pl.Utf8).str.replace_all("'", "").str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "").str.replace_all(r"\s+", " ").str.strip_chars()

    def normalize_key(self, series):
        return series.fill_null("").cast(pl.Utf8).str.replace_all("'", "").str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "").str.replace_all(r"\s+", " ").str.strip_chars().str.to_lowercase()

    def read_and_prepare_file(self, file_path, file_type):
        try:
            df = pl.read_excel(file_path)
        except Exception as e:
            print(f"Ошибка чтения файла {file_path}: {e}")
            return None
        schema_map = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dim_str'],
            'images': ['artikul', 'brand', 'image_url']
        }
        expected_cols = schema_map.get(file_type, [])
        col_mapping = self.detect_columns(df.columns, expected_cols)
        df = df.rename(col_mapping)
        # Очистка и нормализация
        if 'artikul' in df.columns:
            df = df.with_columns(artikul=self.clean_values(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand=self.clean_values(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number=self.clean_values(pl.col('oe_number')))
        key_cols = []
        if 'oe_number' in df.columns:
            key_cols.append('oe_number')
        if 'artikul' in df.columns:
            key_cols.append('artikul')
        if 'brand' in df.columns:
            key_cols.append('brand')
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        # Нормализация ключей
        if 'artikul' in df.columns:
            df = df.with_columns(artikul_norm=self.normalize_key(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand_norm=self.normalize_key(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number_norm=self.normalize_key(pl.col('oe_number')))
        return df

    def merge_all_data_parallel(self, file_paths):
        start_time = time.time()
        dataframes = {}
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor() as executor:
            futures = {}
            for ftype, path in file_paths.items():
                futures[executor.submit(self.read_and_prepare_file, path, ftype)] = ftype
            for future in as_completed(futures):
                ftype = futures[future]
                df = future.result()
                if df is not None:
                    dataframes[ftype] = df
        self.process_dataframes(dataframes)
        processing_time = time.time() - start_time
        total_records = self.get_total_records()
        return {'processing_time': processing_time, 'total_records': total_records}

    def process_dataframes(self, dataframes):
        # Обработка OE
        if 'oe' in dataframes:
            df_oe = dataframes['oe']
            df_oe = df_oe.filter(pl.col('oe_number_norm') != "")
            df_oe = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            if 'name' in df_oe.columns:
                df_oe = df_oe.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                df_oe = df_oe.with_columns(category=pl.lit('Разное'))
            self.upsert_oe(df_oe)

            # Cross-ссылки
            if 'artikul' in df_oe.columns:
                cross_df = df_oe.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).distinct()
                self.upsert_cross(cross_df)

        # Обработка cross
        if 'cross' in dataframes:
            df_cross = dataframes['cross']
            df_cross = df_cross.filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            self.upsert_cross(df_cross)

        # Обработка Parts
        parts_df = None
        file_types_priority = ['oe', 'barcode', 'images', 'dimensions']
        for ftype in file_types_priority:
            if ftype in dataframes:
                df = dataframes[ftype]
                if df is None or df.is_empty():
                    continue
                if 'artikul' in df.columns:
                    df = df.with_columns(artikul=self.clean_values(pl.col('artikul')))
                if 'brand' in df.columns:
                    df = df.with_columns(brand=self.clean_values(pl.col('brand')))
                if 'artikul' in df.columns:
                    df = df.with_columns(artikul_norm=self.normalize_key(pl.col('artikul')))
                if 'brand' in df.columns:
                    df = df.with_columns(brand_norm=self.normalize_key(pl.col('brand')))
                if parts_df is None:
                    parts_df = df
                else:
                    parts_df = parts_df.join(df, on=['artikul_norm', 'brand_norm'], how='outer')
        if parts_df is not None:
            parts_df = self.process_dimensions(parts_df)
            parts_df = self.create_description(parts_df)
            final_cols = ['artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode', 'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description']
            for col in final_cols:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(pl.lit(None).alias(col))
            parts_df = parts_df.select(final_cols)
            self.upsert_parts(parts_df)

    def process_dimensions(self, df):
        for dim in ['length', 'width', 'height']:
            if dim not in df.columns:
                df = df.with_columns(pl.lit(None).alias(dim))
        df = df.with_columns(
            (pl.col('length').cast(pl.Utf8).fill_null('') + 'x' +
             pl.col('width').cast(pl.Utf8).fill_null('') + 'x' +
             pl.col('height').cast(pl.Utf8).fill_null('')).alias('dimensions_str')
        )
        return df

    def create_description(self, df):
        df = df.with_columns(
            pl.concat_str([
                'Артикул: ', pl.col('artikul'), ', Бренд: ', pl.col('brand'), ', Кратность: ', pl.col('multiplicity').cast(pl.Utf8), ' шт.'
            ], separator='').alias('description')
        )
        return df

    def upsert_oe(self, df):
        self.conn.register("temp_oe", df.to_arrow())
        self.conn.execute("""
            INSERT INTO oe_data (oe_number_norm, oe_number, name, applicability, category)
            SELECT oe_number_norm, oe_number, name, applicability, category FROM temp_oe
            ON CONFLICT (oe_number_norm) DO UPDATE SET
                oe_number=excluded.oe_number,
                name=excluded.name,
                applicability=excluded.applicability,
                category=excluded.category
        """)
        self.conn.unregister("temp_oe")

    def upsert_cross(self, df):
        self.conn.register("temp_cross", df.to_arrow())
        self.conn.execute("""
            INSERT INTO cross_references (oe_number_norm, artikul_norm, brand_norm)
            SELECT oe_number_norm, artikul_norm, brand_norm FROM temp_cross
            ON CONFLICT (oe_number_norm, artikul_norm, brand_norm) DO NOTHING
        """)
        self.conn.unregister("temp_cross")

    def upsert_parts(self, df):
        self.conn.register("temp_parts", df.to_arrow())
        self.conn.execute("""
            INSERT INTO parts_data (
                artikul_norm, brand_norm, artikul, brand, multiplicity, barcode,
                length, width, height, weight, image_url, dimensions_str, description
            )
            SELECT artikul_norm, brand_norm, artikul, brand, multiplicity, barcode,
                   length, width, height, weight, image_url, dimensions_str, description
            FROM temp_parts
            ON CONFLICT (artikul_norm, brand_norm) DO UPDATE SET
                artikul=excluded.artikul,
                brand=excluded.brand,
                multiplicity=excluded.multiplicity,
                barcode=excluded.barcode,
                length=excluded.length,
                width=excluded.width,
                height=excluded.height,
                weight=excluded.weight,
                image_url=excluded.image_url,
                dimensions_str=excluded.dimensions_str,
                description=excluded.description
        """)
        self.conn.unregister("temp_parts")

    def determine_category_vectorized(self, series):
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        result_series = pl.Series(dtype=pl.Utf8)
        for cat, pattern in categories_map.items():
            result_series = result_series.when(series.str.contains(pattern)).then(pl.lit(cat)).otherwise(result_series)
        return result_series.fill_null('Разное')

    def get_total_records(self):
        try:
            return self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()[0]
        except:
            return 0

    def get_statistics(self):
        stats = {}
        total_parts = self.get_total_records()
        stats['total_parts'] = total_parts
        try:
            total_oe = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()[0]
        except:
            total_oe = 0
        stats['total_oe'] = total_oe
        try:
            total_brands = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data WHERE brand IS NOT NULL").fetchone()[0]
        except:
            total_brands = 0
        stats['total_brands'] = total_brands
        # топ брендов
        try:
            top_brands = self.conn.execute("SELECT brand, COUNT(*) as cnt FROM parts_data WHERE brand IS NOT NULL GROUP BY brand ORDER BY cnt DESC LIMIT 10").fetchall()
            stats['top_brands'] = pl.DataFrame(top_brands, schema=["brand", "count"])
        except:
            stats['top_brands'] = pl.DataFrame()
        # категории
        try:
            categories = self.conn.execute("SELECT category, COUNT(*) as cnt FROM oe_data WHERE category IS NOT NULL GROUP BY category ORDER BY cnt DESC").fetchall()
            stats['categories'] = pl.DataFrame(categories, schema=["category", "count"])
        except:
            stats['categories'] = pl.DataFrame()
        return stats

    def show_export_interface(self):
        # Ваша новая функция
        st.subheader("Экспорт данных")
        export_type = st.selectbox("Выберите формат экспорта", ["CSV", "Excel", "Parquet"])
        if st.button("Экспортировать"):
            try:
                filename = f"auto_parts_export_{int(time.time())}"
                df = self.conn.execute("SELECT * FROM parts_data").fetchdf()
                path = DATA_DIR / (filename + "." + export_type.lower())
                if export_type == "CSV":
                    df.write_csv(str(path))
                elif export_type == "Excel":
                    df.write_excel(str(path))
                elif export_type == "Parquet":
                    df.write_parquet(str(path))
                st.success(f"Данные экспортированы: {path}")
            except Exception as e:
                st.error(f"❌ Ошибка при экспорте: {e}")

    def delete_by_brand(self, brand_norm):
        try:
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE brand_norm = ?", [brand_norm])
            return count
        except:
            return 0

    def delete_by_artikul(self, artikul_norm):
        try:
            count = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            return count
        except:
            return 0

# --------- Визуальный интерфейс с Streamlit ---------

def main():
    st.set_page_config(
        page_title="AutoParts 10M+",
        layout="wide",
        page_icon="🚗"
    )
    st.title("🚗 AutoParts Catalog — Управление 10+ миллионов позиций")
    st.markdown("""
    **Мощная платформа для обработки и управления большими данными автозапчастей.**
    - Инкрементальные обновления
    - Объединение данных из разных источников
    - Быстрый доступ и экспорт
    - Надежная база данных
    """)

    catalog = AutoPartsCatalog()

    # Меню
    menu = st.sidebar.radio("Выберите раздел", ["Загрузка данных", "Статистика", "Экспорт", "Управление"])

    if menu == "Загрузка данных":
        st.header("📥 Загрузка и подготовка данных")
        is_db_empty = catalog.get_total_records() == 0

        col1, col2 = st.columns(2)
        with col1:
            file_oe = st.file_uploader("1. Основные данные (OE)", type=["xlsx", "xls"])
            file_cross = st.file_uploader("2. Кроссы (OE -> Артикул)", type=["xlsx", "xls"])
            file_barcode = st.file_uploader("3. Штрих-коды и кратность", type=["xlsx", "xls"])
        with col2:
            file_dimensions = st.file_uploader("4. Весогабаритные данные", type=["xlsx", "xls"])
            file_images = st.file_uploader("5. Ссылки на изображения", type=["xlsx", "xls"])

        files_map = {
            "oe": file_oe,
            "cross": file_cross,
            "barcode": file_barcode,
            "dimensions": file_dimensions,
            "images": file_images
        }

        if st.button("🚀 Начать обработку"):
            uploaded_paths = {}
            for key, ufile in files_map.items():
                if ufile:
                    save_path = DATA_DIR / f"{key}_{int(time.time())}_{ufile.name}"
                    with open(save_path, "wb") as f:
                        f.write(ufile.read())
                    uploaded_paths[key] = str(save_path)
            if is_db_empty:
                required = ["oe", "cross", "barcode", "dimensions", "images"]
                missing = [r for r in required if r not in uploaded_paths]
                if missing:
                    st.error(f"❌ Для начальной загрузки нужны все 5 файлов. Отсутсвуют: {', '.join(missing)}")
                elif len(uploaded_paths) == 5:
                    stats = catalog.merge_all_data_parallel(uploaded_paths)
                    st.success(f"Обработка завершена за {stats['processing_time']:.2f} секунд.")
                    st.metric("Всего артикулов", f"{stats['total_records']:,}")
                else:
                    st.error("Загрузите все 5 файлов.")
            else:
                if len(uploaded_paths) > 0:
                    stats = catalog.merge_all_data_parallel(uploaded_paths)
                    st.success(f"Обработка завершена за {stats['processing_time']:.2f} секунд.")
                    st.metric("Всего артикулов", f"{stats['total_records']:,}")

        # Дополнительные операции с ценами и исключениями
        st.subheader("Дополнительные операции с ценами и исключениями")
        uploaded_price_file = st.file_uploader("Загрузить прайс-лист (Excel)", type=["xlsx", "xls"], key="pricefile")
        markup_value = st.number_input("Наценка (например, 0.1 для 10%)", min_value=0.0, max_value=1.0, step=0.01)
        if uploaded_price_file:
            save_path = DATA_DIR / f"price_list_{int(time.time())}_{uploaded_price_file.name}"
            with open(save_path, "wb") as f:
                f.write(uploaded_price_file.read())
            catalog.load_price_list(str(save_path), markup=markup_value)
            st.success("Прайс-лист загружен и применена наценка.")

        uploaded_fixed_price_file = st.file_uploader("Загрузить фиксированные цены (Excel)", type=["xlsx", "xls"], key="fixedprice")
        if uploaded_fixed_price_file:
            save_path_fp = DATA_DIR / f"fixed_prices_{int(time.time())}_{uploaded_fixed_price_file.name}"
            with open(save_path_fp, "wb") as f:
                f.write(uploaded_fixed_price_file.read())
            catalog.load_fixed_prices(str(save_path_fp))
            st.success("Фиксированные цены загружены.")

        exclude_name = st.text_input("Исключить позиции по названию/описанию (через | )")
        if st.button("❌ Исключить позиции по названию"):
            if exclude_name:
                catalog.exclude_rows(exclude_name)
                st.success("Позиции исключены.")

    elif menu == "Статистика":
        st.header("📊 Статистика")
        with st.spinner("Собираю статистику..."):
            stats = catalog.get_statistics()
        st.metric("Общее число запчастей", f"{stats['total_parts']:,}")
        st.metric("Общее число OE", f"{stats['total_oe']:,}")
        st.metric("Общее число брендов", f"{stats['total_brands']:,}")
        st.subheader("Топ брендов")
        if not stats['top_brands'].is_empty():
            st.dataframe(stats['top_brands'].to_pandas())
        else:
            st.write("Нет данных.")
        st.subheader("Распределение по категориям")
        if not stats['categories'].is_empty():
            st.bar_chart(stats['categories'].to_pandas().set_index('category'))
        else:
            st.write("Нет данных.")

    elif menu == "Экспорт":
        catalog.show_export_interface()

    elif menu == "Управление":
        st.header("🗑️ Управление данными")
        st.warning("⚠️ Осторожно! Операции необратимы.")
        option = st.radio("Выберите действие", ["Удалить по бренду", "Удалить по артикулу"])

        if option == "Удалить по бренду":
            try:
                brands = [row[0] for row in catalog.conn.execute("SELECT DISTINCT brand FROM parts_data WHERE brand IS NOT NULL").fetchall()]
            except:
                brands = []
            if brands:
                selected_brand = st.selectbox("Выберите бренд", brands)
                result = catalog.conn.execute("SELECT brand_norm FROM parts_data WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()
                brand_norm = result[0] if result else ''
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
                st.info(f"Удаление {count} позиций по бренду {selected_brand}")
                if st.checkbox("Подтверждаю удаление"):
                    deleted = catalog.delete_by_brand(brand_norm)
                    st.success(f"Удалено {deleted} позиций.")
                    st.experimental_rerun()
            else:
                st.write("Нет доступных брендов.")

        else:
            # Удаление по артикулу
            artic_input = st.text_input("Введите артикул")
            if artic_input:
                result = catalog.conn.execute("SELECT artikul_norm FROM parts_data WHERE artikul = ? LIMIT 1", [artic_input]).fetchone()
                artikul_norm = result[0] if result else ''
                count = catalog.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
                st.info(f"Удаление {count} позиций по артикулу {articul_input}")
                if st.checkbox("Подтверждаю удаление"):
                    deleted = catalog.delete_by_artikul(artikul_norm)
                    st.success(f"Удалено {deleted} позиций.")
                    st.experimental_rerun()

if __name__ == "__main__":
    main()
