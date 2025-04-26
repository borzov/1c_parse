#!filepath: src/__main__.py
# src/__main__.py
# -*- coding: utf-8 -*-
import os
import sys
import logging
import argparse

from src.config import (
    DATA_DIR, OUTPUT_DIR, TEMPLATES_DIR, REPORT_1_FILENAME, REPORT_2_FILENAME,
    DEBUG_MODE, DEBUG_NAMES_FILENAME, DEBUG_TRANSACTIONS_FILENAME
)
from src.parser_1c import parse_1c_file
from src.processing import (
    detect_organizations, process_documents,
    debug_save_names, debug_save_processed_transactions
)
from src.reporting import generate_counterparty_annual_report_v5, generate_org_comparison_report_v5

# --- Парсинг аргументов ---
parser = argparse.ArgumentParser(description="Анализатор выписок 1С и генератор отчетов.")
parser.add_argument(
    '--filter-name',
    type=str,
    default=None,
    help='Часть имени контрагента (регистронезависимо) для фильтрации отладочного файла транзакций и лог-файла (только в DEBUG_MODE).'
)
args = parser.parse_args()
# --- Конец парсинга аргументов ---

# --- Фильтр для логов (используется только в DEBUG_MODE) ---
class NameFilter(logging.Filter):
    def __init__(self, name_substring=None):
        super().__init__()
        self.name_substring = name_substring.lower() if name_substring else None

    def filter(self, record):
        if self.name_substring is None:
            return True
        try:
             message = record.getMessage()
             has_match = self.name_substring in message.lower()
             if not has_match and record.args and isinstance(record.args, (tuple, list)):
                 has_match = any(self.name_substring in str(arg).lower() for arg in record.args)
             return has_match
        except Exception:
             return True

# --- Настройка логирования ---
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
log_format = '%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s'
log_handlers = []
log_to_file_success = False

if DEBUG_MODE:
    # Настройка логирования в файл только в DEBUG_MODE
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_filename = 'analysis.txt'
    log_filepath = os.path.join(OUTPUT_DIR, log_filename)
    try:
        file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format))
        if args.filter_name:
            try:
                name_filter_instance = NameFilter(args.filter_name)
                file_handler.addFilter(name_filter_instance)
                print(f"[DEBUG] Лог-фильтр по имени '{args.filter_name}' активирован.")
            except Exception as e:
                print(f"[ERROR] Не удалось применить NameFilter к FileHandler: {e}")
        log_handlers.append(file_handler)
        log_to_file_success = True
    except Exception as e:
        print(f"CRITICAL: Не удалось настроить логирование в файл '{log_filepath}': {e}", file=sys.stderr)
else:
    # В обычном режиме - только консоль уровня INFO
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s')) # Упрощенный формат для консоли
    console_handler.setLevel(logging.INFO)
    log_handlers.append(console_handler)

# Применяем настроенные обработчики
try:
    # Убираем стандартные обработчики, если они есть
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=log_level, format=log_format, handlers=log_handlers)
    logger = logging.getLogger(__name__) # Получаем логгер после basicConfig
    logger.debug("Настройка логирования завершена.")
except Exception as e:
    # Fallback на простейший вывод в stderr в случае ошибки basicConfig
    print(f"CRITICAL: Не удалось настроить logging через basicConfig: {e}", file=sys.stderr)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)
    logger.error("Произошла ошибка при настройке логирования.")

# --- Конец Настройки логирования ---


def run_analysis():
    """Основная функция запуска анализа."""
    logger.info("="*50)
    logger.info("Запуск анализатора выписок 1С...")
    if DEBUG_MODE and log_to_file_success:
        if args.filter_name:
             logger.info(f"Логи DEBUG (с фильтром '{args.filter_name}') сохраняются в файл: {log_filepath}")
        else:
             logger.info(f"Логи DEBUG сохраняются в файл: {log_filepath}")

    if DEBUG_MODE:
        logger.warning("*** РЕЖИМ ОТЛАДКИ АКТИВЕН ***")
        if args.filter_name:
            logger.warning(f"*** Фильтр имени для отладки: '{args.filter_name}' ***")

    # Проверка папок
    if not os.path.isdir(DATA_DIR): logger.error(f"Папка данных '{DATA_DIR}' не найдена."); sys.exit(1)
    if not os.path.isdir(TEMPLATES_DIR): logger.error(f"Папка шаблонов '{TEMPLATES_DIR}' не найдена."); sys.exit(1)

    # Чтение и парсинг файлов
    parsed_files_data = []
    files_processed_count, files_error_count = 0, 0
    try:
        all_files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith('.txt')]
        total_files = len(all_files)
        logger.info(f"Найдено {total_files} .txt файлов в '{DATA_DIR}'. Начинаем парсинг...")
    except Exception as e:
        logger.error(f"Ошибка при чтении содержимого папки '{DATA_DIR}': {e}", exc_info=DEBUG_MODE); sys.exit(1)

    for i, filename in enumerate(all_files):
        filepath = os.path.join(DATA_DIR, filename)
        logger.debug(f"Парсинг файла [{i+1}/{total_files}]: {filename}")
        try:
            header, docs = parse_1c_file(filepath)
            if header is not None and docs is not None:
                if header: header['_filepath'] = filepath # Добавляем путь для логов
                parsed_files_data.append((header, docs))
                files_processed_count += 1
                logger.debug(f"Файл {filename} успешно разобран.")
            else:
                files_error_count += 1 # parse_1c_file вернул None, вероятно была ошибка, залогированная внутри
        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге файла {filename}: {e}", exc_info=DEBUG_MODE)
            files_error_count += 1

    logger.info(f"Парсинг завершен: Успешно {files_processed_count}, Ошибки {files_error_count}")

    if not parsed_files_data:
        logger.warning("Нет успешно разобранных файлов. Завершение работы.")
        sys.exit(0)

    # Определение организаций
    try:
        detected_organizations_map = detect_organizations(parsed_files_data)
        if not detected_organizations_map:
            logger.error("Не удалось определить организации. Завершение работы.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Критическая ошибка при определении организаций: {e}", exc_info=DEBUG_MODE)
        sys.exit(1)

    # Сбор всех документов для обработки
    all_documents = []
    for _, docs in parsed_files_data:
        if docs: all_documents.extend(docs)

    if not all_documents:
        logger.warning("Нет секций документов в файлах для обработки. Завершение работы.")
        sys.exit(0)

    # Обработка документов
    try:
        processed_transactions = process_documents(all_documents, detected_organizations_map)
    except Exception as e:
        logger.error(f"Критическая ошибка при обработке документов: {e}", exc_info=DEBUG_MODE)
        sys.exit(1)

    # Сохранение отладочных файлов (только в DEBUG_MODE)
    if DEBUG_MODE:
        logger.info("Сохранение отладочных файлов...")
        try:
            debug_save_names(OUTPUT_DIR)
            debug_save_processed_transactions(OUTPUT_DIR, name_filter=args.filter_name)
        except Exception as e:
            logger.error(f"Ошибка при сохранении отладочных файлов: {e}", exc_info=True) # В DEBUG_MODE показываем traceback

    # Генерация отчетов
    if not processed_transactions:
        logger.warning("Нет значащих транзакций для анализа. Генерируются пустые отчеты.")
        try:
            report1_path = os.path.join(OUTPUT_DIR, REPORT_1_FILENAME)
            generate_counterparty_annual_report_v5([], detected_organizations_map, report1_path)
            report2_path = os.path.join(OUTPUT_DIR, REPORT_2_FILENAME)
            generate_org_comparison_report_v5([], detected_organizations_map, report2_path)
            logger.info("Пустые отчеты созданы.")
        except Exception as e:
            logger.error(f"Ошибка при генерации пустых отчетов: {e}", exc_info=DEBUG_MODE)
        sys.exit(0)

    logger.info("Генерация отчетов...")
    try:
        report1_path = os.path.join(OUTPUT_DIR, REPORT_1_FILENAME)
        logger.info(f" -> {REPORT_1_FILENAME}")
        generate_counterparty_annual_report_v5(processed_transactions, detected_organizations_map, report1_path)

        report2_path = os.path.join(OUTPUT_DIR, REPORT_2_FILENAME)
        logger.info(f" -> {REPORT_2_FILENAME}")
        generate_org_comparison_report_v5(processed_transactions, detected_organizations_map, report2_path)
        logger.info(f"Отчеты сохранены в папку: {OUTPUT_DIR}")
    except Exception as e:
        logger.error(f"Критическая ошибка при генерации отчетов: {e}", exc_info=DEBUG_MODE)
        sys.exit(1)

    logger.info("Анализ успешно завершен.")
    logger.info("="*50)

if __name__ == "__main__":
    try:
        run_analysis()
    except Exception as e:
        try:
            logging.critical(f"Критическая ошибка выполнения: {e}", exc_info=DEBUG_MODE)
        except Exception:
            pass
        print(f"CRITICAL ERROR: {e}", file=sys.stderr)
        sys.exit(1)