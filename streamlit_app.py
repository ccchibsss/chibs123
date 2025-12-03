import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()
        
    def setup_database(self):
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
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                quantity INTEGER,
                recommended_price DECIMAL(10, 2),
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS markup_settings (
                id SERIAL PRIMARY KEY,
                brand VARCHAR UNIQUE,
                markup_percentage DECIMAL(5, 2)
            )
        """)
        
    def create_indexes(self):
        st.info("Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_data_oe ON oe_data(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_data_keys ON parts_data(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)"
        ]
        for index_sql in indexes:
            self.conn.execute(index_sql)
        st.success("Индексы созданы.")

    @staticmethod
    def normalize_key(key_series: pl.Series) -> pl.Series:
        return (
            key_series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .str.to_lowercase()
        )

    @staticmethod
    def clean_values(value_series: pl.Series) -> pl.Series:
        return (
            value_series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )

    @staticmethod
    def determine_category_vectorized(name_series: pl.Series) -> pl.Series:
        categories_map = {
            'Фильтр': 'фильтр|filter', 
            'Тормозная система': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|Рычаг|Рычаги|Шаровая опора|Опора шаровая|Сайлентблок|Ступиц|подшипник ступицы|подшипники ступицы', 
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission', 
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering', 
            'Выхлопная система': 'глушитель|глушител|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling', 
            'Топливо': 'топливный|бензонасос|форсунк|fuel',
        }
        name_lower = name_series.str.to_lowercase()
        expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for category, pattern in categories_map.items():
            expr = expr.when(name_lower.str.contains(pattern)).then(pl.lit(category))
        return expr.otherwise(pl.lit('Разное')).alias('category')

    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        mapping = {}
        column_variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE'], 
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer'], 
            'name': ['наименование', 'название', 'name', 'описание', 'description'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability'], 
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity'], 
            'length': ['длина (см)', 'длина', 'length', 'длинна'],
            'width': ['ширина (см)', 'ширина', 'width'], 
            'height': ['высота (см)', 'высота', 'height'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight'], 
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size'], 
            'price': ['цена', 'стоимость', 'price'],
            'quantity': ['количество', 'amount', 'quantity']
        }
        actual_lower = {col.lower(): col for col in actual_columns}
        for expected in expected_columns:
            variants = [v.lower() for v in column_variants.get(expected, [expected])]
            for variant in variants:
                for actual_l, actual_orig in actual_lower.items():
                    if variant in actual_l:
                        mapping[actual_orig] = expected
                        break
                if expected in mapping.values():
                    break
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        logger.info(f"Начинаю обработку файла: {file_type} ({file_path})")
        try:
            df = pl.read_excel(file_path, engine='calamine')
        except Exception as e:
            logger.error(f"Не удалось прочитать файл {file_path}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'barcode': ['brand', 'artikul', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'prices': ['artikul', 'brand', 'quantity', 'recommended_price']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        df = df.rename(column_mapping)
        
        # Очистить оригинальные значения от апострофов и мусора на входе
        if 'artikul' in df.columns:
            df = df.with_columns(artikul=self.clean_values(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand=self.clean_values(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number=self.clean_values(pl.col('oe_number')))
        
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        # Создать нормализованные версии для ключей (нижний регистр)
        if 'artikul' in df.columns:
            df = df.with_columns(artikul_norm=self.normalize_key(pl.col('artikul')))
        if 'brand' in df.columns:
            df = df.with_columns(brand_norm=self.normalize_key(pl.col('brand')))
        if 'oe_number' in df.columns:
            df = df.with_columns(oe_number_norm=self.normalize_key(pl.col('oe_number')))
            
        return df

    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return
        
        df = df.unique(keep='first')
        
        cols = df.columns
        pk_str = ", ".join(f'"{c}"' for c in pk)
        
        temp_view_name = f"temp_{table_name}_{int(time.time())}"
        self.conn.register(temp_view_name, df.to_arrow())
        
        update_cols = [col for col in cols if col not in pk]
        
        if not update_cols:
            on_conflict_action = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = excluded."{col}"' for col in update_cols])
            on_conflict_action = f"DO UPDATE SET {update_clause}"

        sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM {temp_view_name}
        ON CONFLICT ({pk_str}) {on_conflict_action};
        """
        
        try:
            self.conn.execute(sql)
            logger.info(f"Успешно обновлено/вставлено {len(df)} записей в таблицу {table_name}.")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            self.conn.unregister(temp_view_name)

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        
        steps = [s for s in ['oe', 'cross', 'parts', 'prices'] if s in dataframes or s == 'parts']
        num_steps = len(steps)
        progress_bar = st.progress(0, text="Подготовка к обновлению базы данных...")
        step_counter = 0

        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'], keep='first')
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe_data', oe_df, ['oe_number_norm'])
            
            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])

        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")
        parts_df = None
        # Определяем порядок обработки файлов для правильного приоритета данных
        # Порядок важен: сначала базовые данные, потом специфичные (dimensions имеет приоритет)
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        key_files = {ftype: df for ftype, df in dataframes.items() if ftype in file_priority}
        
        if key_files:
            # Собираем все уникальные артикулы из всех файлов
            all_parts = pl.concat([
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm']) 
                for df in key_files.values() if 'artikul_norm' in df.columns and 'brand_norm' in df.columns
            ]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm', 'brand_norm'], keep='first')

            parts_df = all_parts

            # Обрабатываем файлы в определенном порядке для правильного приоритета данных
            for ftype in file_priority:
                if ftype not in key_files: continue
                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns: continue
                
                join_cols = [col for col in df.columns if col not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols: continue
                
                # Фильтруем колонки, которые уже есть в parts_df, чтобы избежать дублирования
                existing_cols = set(parts_df.columns)
                join_cols = [col for col in join_cols if col not in existing_cols]
                if not join_cols: continue
                
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
                # coalesce=True перезаписывает пустые значения существующих колонок
                # Суффиксы не создаются, так как мы уже отфильтровали существующие колонки
                parts_df = parts_df.join(df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

        if parts_df is not None and not parts_df.is_empty():
            # Безопасная обработка multiplicity
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
            else:
                parts_df = parts_df.with_columns(
                    pl.col('multiplicity').fill_null(1).cast(pl.Int32)
                )
            
            # Обработка размеров и описание
            for col in ['length', 'width', 'height']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
            # Создать строки размеров
            parts_df = parts_df.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null('').alias('_length_str'),
                pl.col('width').cast(pl.Utf8).fill_null('').alias('_width_str'),
                pl.col('height').cast(pl.Utf8).fill_null('').alias('_height_str'),
            ])
            # Создавать описание
            parts_df = parts_df.with_columns(
                description=pl.concat_str(
                    [
                        pl.lit('Артикул: '), pl.col('_length_str'),
                        pl.lit(', Бренд: '), pl.col('_width_str'),
                        pl.lit(', Кратность: '), pl.col('_height_str'), 
                        pl.lit(' шт.')
                    ],
                    separator=''
                )
            )
            # Удаляем временные колонки
            parts_df = parts_df.drop(['_length_str', '_width_str', '_height_str'])
            final_columns = [
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode', 
                'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]
            select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(None).alias(c) for c in final_columns]
            parts_df = parts_df.select(select_exprs)
            self.upsert_data('parts_data', parts_df, ['artikul_norm', 'brand_norm'])
        
        if 'prices' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1), text=f"({step_counter}/{num_steps}) Обработка ценовых данных...")
            df = dataframes['prices'].filter((pl.col('artikul_norm') != "") & (pl.col('brand_norm') != ""))
            self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])
        
        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()
        st.success("💾 Загрузка данных в базу завершена.")

    def merge_all_data_parallel(self, file_paths: Dict[str, str]) -> Dict[str, any]:
        start_time = time.time()
        stats = {}
        st.info("🚀 Начало параллельного чтения и подготовки файлов...")
        n_files = len(file_paths)
        file_progress_bar = st.progress(0, text="Ожидание...")
        dataframes = {}
        processed_files = 0
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.read_and_prepare_file, path, ftype): ftype for ftype, path in file_paths.items()}
            for future in as_completed(future_to_file):
                ftype = future_to_file[future]
                try:
                    df = future.result()
                    if not df.is_empty():
                        dataframes[ftype] = df
                        st.success(f"✅ Файл '{ftype}' прочитан: {len(df):,} строк.")
                    else:
                        st.warning(f"⚠️ Файл '{ftype}' пуст или не удалось обработать.")
                except Exception as e:
                    logger.exception(f"Ошибка обработки файла {ftype}")
                    st.error(f"❌ Ошибка в {ftype}: {e}")
                finally:
                    processed_files += 1
                    file_progress_bar.progress(processed_files / n_files, text=f"Обработка файла: {ftype} ({processed_files}/{n_files})")
        file_progress_bar.empty()

        if not dataframes:
            st.error("❌ Ни один файл не был загружен. Обработка остановлена.")
            return {}

        self.process_and_load_data(dataframes)
        processing_time = time.time() - start_time
        total_records = self.get_total_records()
        stats['processing_time'] = processing_time
        stats['total_records'] = total_records
        st.success(f"🎉 Обработка завершена за {processing_time:.2f} секунд")
        st.success(f"📊 Всего уникальных артикулов в базе: {total_records:,}")
        self.create_indexes()
        return stats
    
    def get_total_records(self) -> int:
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM parts_data").fetchone()
            return result[0] if result else 0
        except (duckdb.Error, TypeError):
            return 0

    def get_export_query(self, exclusions=None, columns=None) -> str:
        exclusion_conditions = ''
        if exclusions:
            exclusion_conditions = f"AND NOT REGEXP_MATCHES(CONCAT(representative_name, '|', representative_applicability), '{exclusions}')"
        
        column_selection = '*'
        if columns:
            column_selection = ', '.join(columns)
        
        return rf"""
        WITH PartDetails AS (
            SELECT
                cr.artikul_norm,
                cr.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(o.oe_number, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\-\s]', '', 'g'), ', ') AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            JOIN oe_data o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT
                cr1.artikul_norm,
                cr1.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(p2.artikul, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\-\s]', '', 'g'), ', ') as analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts_data p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        )
        SELECT {column_selection}
        FROM parts_data p
        LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
        LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
        WHERE pd.oe_list IS NOT NULL
        {exclusion_conditions}
        ORDER BY p.brand, p.artikul
        """

    def export_to_csv_optimized(self, output_path: str, exclusions=None, columns=None) -> bool:
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data) AS t").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        
        st.info(f"📤 Экспорт {total_records:,} записей в CSV...")
        try:
            query = self.get_export_query(exclusions, columns)
            df = self.conn.execute(query).pl()

            # Преобразуем числовые столбцы в строки для консистентности
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
            for col_name in dimension_cols:
                if col_name in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col_name).is_not_null())
                        .then(pl.col(col_name).cast(pl.Utf8))
                        .otherwise(pl.lit(""))
                        .alias(col_name)
                    )

            buf = io.StringIO()
            df.write_csv(buf, separator=';')
            csv_text = buf.getvalue()

            with open(output_path, 'wb') as f:
                f.write(b'\xef\xbb\xbf')
                f.write(csv_text.encode('utf-8'))

            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в CSV: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в CSV")
            st.error(f"❌ Ошибка экспорта в CSV: {e}")
            return False
    
    def export_to_excel(self, output_path: Path, exclusions=None, columns=None) -> Tuple[bool, Path]:
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data) AS t").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False, None

        st.info(f"📤 Экспорт {total_records:,} записей в Excel...")
        try:
            num_files = (total_records + EXCEL_ROW_LIMIT - 1) // EXCEL_ROW_LIMIT
            base_query = self.get_export_query(exclusions, columns)
            exported_files = []

            progress_bar = st.progress(0, text=f"Подготовка к экспорту {num_files} файла(ов)...")

            for i in range(num_files):
                progress_bar.progress((i + 1) / num_files, text=f"Экспорт части {i+1} из {num_files}...")
                offset = i * EXCEL_ROW_LIMIT
                query = f"{base_query} LIMIT {EXCEL_ROW_LIMIT} OFFSET {offset}"
                df = self.conn.execute(query).pl()

                # Преобразуем числовые столбцы в строки
                dimension_cols = ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "Кратность"]
                for col_name in dimension_cols:
                    if col_name in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col(col_name).is_not_null())
                            .then(pl.col(col_name).cast(pl.Utf8))
                            .otherwise(pl.lit(""))
                            .alias(col_name)
                        )

                file_part_path = output_path.with_name(f"{output_path.stem}_part_{i+1}.xlsx")
                df.write_excel(str(file_part_path))
                exported_files.append(file_part_path)
            progress_bar.empty()

            # Упаковка в ZIP, если больше одного файла
            if num_files > 1:
                st.info("Архивация файлов в ZIP...")
                zip_path = output_path.with_suffix('.zip')
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in exported_files:
                        zipf.write(file, arcname=file.name)
                        os.remove(file)
                final_path = zip_path
            else:
                final_path = exported_files[0]
                # Переименовать, если нужно
                if final_path != output_path:
                    os.rename(final_path, output_path)
                final_path = output_path

            file_size = os.path.getsize(final_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы: {final_path.name} ({file_size:.1f} МБ)")
            return True, final_path

        except Exception as e:
            logger.exception("Ошибка экспорта в Excel")
            st.error(f"❌ Ошибка экспорта в Excel: {e}")
            return False, None
            
    def export_to_parquet(self, output_path: str, exclusions=None, columns=None) -> bool:
        total_records = self.conn.execute("SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data) AS t").fetchone()[0]
        if total_records == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total_records:,} записей в Parquet...")
        try:
            query = self.get_export_query(exclusions, columns)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы в Parquet: {output_path} ({file_size:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта в Parquet")
            st.error(f"❌ Ошибка экспорта в Parquet: {e}")
            return False

    def show_export_interface(self):
        st.header("📤 Умный экспорт данных")
        total_records = self.conn.execute("SELECT count(DISTINCT (artikul_norm, brand_norm)) FROM parts_data").fetchone()[0]
        st.info(f"Всего записей для экспорта (строк): {total_records:,}")
        if total_records == 0:
            st.warning("База данных пуста или нет связей для экспорта. Сначала загрузите данные.")
            return
        exclusions = st.text_area("Исключить строки по наименованию (разделяйте символами '|'):", placeholder="Пример: фильтр масляный|лампы накаливания")
        available_columns = [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота",
            "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение"
        ]
        selected_columns = st.multiselect("Выберите столбцы для экспорта (пусто = все)", options=available_columns, default=available_columns)
        export_format = st.radio("Выберите формат экспорта:", ["CSV", "Excel (.xlsx)", "Parquet (для разработчиков)"], index=0)

        if export_format == "CSV":
            if st.button("🚀 Экспорт в CSV"):
                output_path = self.data_dir / "auto_parts_report.csv"
                with st.spinner("Идет экспорт в CSV..."):
                    success = self.export_to_csv_optimized(str(output_path), exclusions, selected_columns if selected_columns else None)
                if success:
                    with open(output_path, "rb") as f:
                        st.download_button("📥 Скачать CSV файл", f, "auto_parts_report.csv", "text/csv")

        elif export_format == "Excel (.xlsx)":
            st.info("ℹ️ Если записей больше 1 млн, результат будет разделен на несколько файлов и упакован в ZIP-архив.")
            if st.button("📊 Экспорт в Excel"):
                output_path = self.data_dir / "auto_parts_report.xlsx"
                with st.spinner("Идет экспорт в Excel..."):
                    success, final_path = self.export_to_excel(output_path, exclusions, selected_columns if selected_columns else None)
                if success and final_path and final_path.exists():
                    with open(final_path, "rb") as f:
                        mime = "application/zip" if final_path.suffix == ".zip" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        st.download_button(f"📥 Скачать {final_path.name}", f, final_path.name, mime)

        elif export_format == "Parquet (для разработчиков)":
            if st.button("⚡️ Экспорт в Parquet"):
                output_path = self.data_dir / "auto_parts_report.parquet"
                with st.spinner("Идет экспорт в Parquet..."):
                    success = self.export_to_parquet(str(output_path), exclusions, selected_columns if selected_columns else None)
                if success:
                    with open(output_path, "rb") as f:
                        st.download_button("📥 Скачать Parquet файл", f, "auto_parts_report.parquet", "application/octet-stream")

    def delete_by_brand(self, brand_norm: str) -> int:
        try:
            count_result = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                logger.info(f"No records found for brand: {brand_norm}")
                return 0
            self.conn.execute("DELETE FROM parts_data WHERE brand_norm = ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)")
            logger.info(f"Deleted {deleted_count} records for brand: {brand_norm}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by brand {brand_norm}: {e}")
            raise

    def delete_by_artikul(self, artikul_norm: str) -> int:
        try:
            count_result = self.conn.execute("SELECT COUNT(*) FROM parts_data WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                logger.info(f"No records found for artikul: {artikul_norm}")
                return 0
            self.conn.execute("DELETE FROM parts_data WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts_data)")
            logger.info(f"Deleted {deleted_count} records for artikul: {artikul_norm}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting by artikul {artikul_norm}: {e}")
            raise

    def get_statistics(self) -> Dict:
        stats = {}
        try:
            stats['total_parts'] = self.get_total_records()
            if stats['total_parts'] == 0:
                return {
                    'total_parts': 0, 'total_oe': 0, 'total_brands': 0,
                    'top_brands': pl.DataFrame(), 'categories': pl.DataFrame()
                }
            total_oe_res = self.conn.execute("SELECT COUNT(*) FROM oe_data").fetchone()
            stats['total_oe'] = total_oe_res[0] if total_oe_res else 0
            total_brands_res = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts_data WHERE brand IS NOT NULL").fetchone()
            stats['total_brands'] = total_brands_res[0] if total_brands_res else 0
            brand_stats = self.conn.execute("SELECT brand, COUNT(*) as count FROM parts_data WHERE brand IS NOT NULL GROUP BY brand ORDER BY count DESC LIMIT 10").pl()
            stats['top_brands'] = brand_stats
            category_stats = self.conn.execute("SELECT category, COUNT(*) as count FROM oe_data WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC").pl()
            stats['categories'] = category_stats
        except Exception as e:
            logger.error(f"Ошибка при сборе статистики: {e}")
            return {
                'total_parts': 0, 'total_oe': 0, 'total_brands': 0,
                'top_brands': pl.DataFrame(), 'categories': pl.DataFrame()
            }
        return stats

    def apply_markup(self, brand: str, percentage: float) -> bool:
        try:
            exists_result = self.conn.execute("SELECT COUNT(*) FROM markup_settings WHERE brand = ?", [brand]).fetchone()
            if exists_result[0] > 0:
                self.conn.execute("UPDATE markup_settings SET markup_percentage = ? WHERE brand = ?", [percentage, brand])
            else:
                self.conn.execute("INSERT INTO markup_settings (brand, markup_percentage) VALUES (?, ?)", [brand, percentage])
            prices_result = self.conn.execute("SELECT artikul_norm, brand_norm, recommended_price FROM prices WHERE brand_norm = ?", [brand]).fetchall()
            updated_prices = []
            for row in prices_result:
                artikul_norm, brand_norm, old_price = row
                new_price = round(old_price * (1 + percentage / 100), 2)
                updated_prices.append((new_price, artikul_norm, brand_norm))
            if updated_prices:
                self.conn.executemany("UPDATE prices SET recommended_price = ? WHERE artikul_norm = ? AND brand_norm = ?", updated_prices)
            return True
        except Exception as e:
            logger.error(f"Ошибка при применении наценки: {e}")
            return False

    def show_management_interface(self):
        st.header("🔧 Настройки и управление базой данных")
        st.subheader("Настройка общей наценки")
        global_markup = st.number_input("Общая наценка (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.1)
        if st.button("Применить общую наценку ко всем брендам"):
            self.conn.execute("DELETE FROM markup_settings")
            affected_rows = self.conn.execute("UPDATE prices SET recommended_price = recommended_price * (1 + ? / 100)", [global_markup]).rowcount
            st.success(f"Применена общая наценка {global_markup}% к {affected_rows} записям.")
        st.subheader("Настройка индивидуальной наценки по брендам")
        brand = st.text_input("Название бренда (без нормировки):")
        if brand:
            norm_brand = self.normalize_key(pl.Series([brand]))[0]
            current_markup = self.conn.execute("SELECT markup_percentage FROM markup_settings WHERE brand = ?", [norm_brand]).fetchone()
            current_markup_value = current_markup[0] if current_markup else 0.0
            brand_markup = st.number_input(f"Наценка для бренда '{brand}' (%)", min_value=0.0, max_value=100.0, value=float(current_markup_value), step=0.1)
            if st.button("Установить индивидуальную наценку"):
                success = self.apply_markup(norm_brand, brand_markup)
                if success:
                    st.success(f"Установлена индивидуальная наценка {brand_markup}% для бренда '{brand}'.")
                else:
                    st.error("Ошибка при установке индивидуальной наценки.")


def main():
    st.set_page_config(page_title="AutoParts Catalog 10M+", layout="wide", page_icon="🚗")
    st.title("🚗 AutoParts Catalog - Профессиональная система для 10+ млн записей")
    st.markdown("""
    ### 💪 Мощная платформа для управления большими объемами данных автозапчастей
    - **Инкрементальные обновления**: Безопасно добавляйте новые файлы для дополнения и обновления каталога.
    - **Надежное объединение**: Данные из 5-ти типов файлов корректно сливаются в единую базу.
    - **Оптимизированное хранение**: Использование DuckDB для мгновенного доступа и анализа.
    - **Умный экспорт**: Быстрый и надежный экспорт в CSV, Excel или Parquet с гарантией отсутствия дубликатов.
    """)
    
    catalog = HighVolumeAutoPartsCatalog()
    st.sidebar.title("🧭 Навигация")
    menu_option = st.sidebar.radio("Выберите действие:", ["Загрузка данных", "Экспорт", "Статистика", "Управление данными"])
    
    if menu_option == "Загрузка данных":
        st.header("📥 Загрузка и обработка данных")
        st.info("""
        **Порядок работы:**
        1. Загрузите один или несколько файлов Excel (`.xlsx`, `.xls`). Не обязательно загружать все сразу.
        2. Нажмите кнопку "Начать обработку".
        3. Система автоматически прочитает, объединит данные и обновит/дополнит существующую базу.
        ...
        """)
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("1. Основные данные (OE)", type=['xlsx', 'xls'])
            cross_file = st.file_uploader("2. Кроссы (OE -> Артикул)", type=['xlsx', 'xls'])
            barcode_file = st.file_uploader("3. Штрих-коды и кратность", type=['xlsx', 'xls'])
        with col2:
            dimensions_file = st.file_uploader("4. Весогабаритные данные", type=['xlsx', 'xls'])
            images_file = st.file_uploader("5. Ссылки на изображения", type=['xlsx', 'xls'])
            prices_file = st.file_uploader("6. Цены (рекомендованные)", type=['xlsx', 'xls'])
        file_map = {
            'oe': oe_file, 'cross': cross_file, 'barcode': barcode_file,
            'dimensions': dimensions_file, 'images': images_file, 'prices': prices_file
        }
        if st.button("🚀 Начать обработку данных"):
            paths_to_process = {}
            any_file_uploaded = False
            for ftype, uploaded_file in file_map.items():
                if uploaded_file:
                    any_file_uploaded = True
                    path = catalog.data_dir / f"{ftype}_data_{int(time.time())}_{uploaded_file.name}"
                    with open(path, "wb") as f:
                        f.write(uploaded_file.getvalue())
                    paths_to_process[ftype] = str(path)
            if any_file_uploaded:
                stats = catalog.merge_all_data_parallel(paths_to_process)
                if stats:
                    st.subheader("📊 Статистика обработки")
                    st.metric("Общее время", f"{stats.get('processing_time', 0):.2f} сек")
                    st.metric("Всего артикулов в базе", f"{stats.get('total_records', 0):,}")
                    st.metric("Обработано файлов", f"{len(paths_to_process)}")
            else:
                st.warning("⚠️ Пожалуйста, загрузите хотя бы один файл для начала обработки.")
    elif menu_option == "Экспорт":
        catalog.show_export_interface()
    elif menu_option == "Статистика":
        st.header("📈 Статистика по каталогу")
        with st.spinner("Сбор статистики..."):
            stats = catalog.get_statistics()
        if stats.get('total_parts', 0) > 0:
            st.metric("Уникальных артикулов", f"{stats.get('total_parts', 0):,}")
            st.metric("Уникальных OE", f"{stats.get('total_oe', 0):,}")
            st.metric("Уникальных брендов", f"{stats.get('total_brands', 0):,}")
            st.subheader("🏆 Топ-10 брендов по количеству артикулов")
            if 'top_brands' in stats and not stats['top_brands'].is_empty():
                st.dataframe(stats['top_brands'].to_pandas(), width='stretch')
            else:
                st.write("Нет данных по брендам.")
            st.subheader("📊 Распределение по категориям")
            if 'categories' in stats and not stats['categories'].is_empty():
                st.bar_chart(stats['categories'].to_pandas().set_index('category'))
            else:
                st.write("Нет данных по категориям.")
        else:
            st.info("Данные отсутствуют. Перейдите в раздел 'Загрузка данных', чтобы начать.")
    elif menu_option == "Управление данными":
        catalog.show_management_interface()

if __name__ == "__main__":
    main()
