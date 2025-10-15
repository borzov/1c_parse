#!filepath: src/processing.py
# src/processing.py
# -*- coding: utf-8 -*-
import os
import csv
import logging
import sys
from collections import defaultdict, Counter
from typing import Optional, List, Dict, Set, Tuple, Any, Union
from .utils import get_doc_party_name, parse_date, safe_float, get_best_name
from .normalization import normalize_and_classify, format_fio_display
from .config import (
    DEFAULT_ORG_NAME_PREFIX, DEBUG_NAMES_FILENAME, DEBUG_TRANSACTIONS_FILENAME,
    DEBUG_MODE
)

logger = logging.getLogger(__name__)

# Списки для отладки (заполняются только в DEBUG_MODE)
DEBUG_NAMES_LIST: List[Dict[str, Any]] = []
DEBUG_TRANSACTIONS_LIST: List[Dict[str, Any]] = []

def detect_organizations(parsed_files_data: List[Tuple[Optional[Dict], Optional[List[Dict]]]]) -> Dict[str, Dict]:
    """
    Определяет наши организации по данным из файлов выписок.
    Оптимизированная версия с единым проходом для создания индекса.

    Args:
        parsed_files_data: Список кортежей (header_info, documents) для каждого файла.

    Returns:
        Словарь, где ключ - расчетный счет нашей организации,
        значение - словарь с информацией ('name', 'normalized', 'legal_form', 'inn').
    """
    logger.info("Автоматическое определение организаций (оптимизированный алгоритм)...")
    detected_orgs: Dict[str, Dict] = {}
    
    # Создаем единый индекс: счет -> (имена, ИННы, источники)
    account_index: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'names': Counter(), 'inns': set(), 'sources': set(), 'header_inn': None
    })
    
    # Собираем все документы и создаем индекс за один проход
    all_docs: List[Dict] = []
    for header_info, documents in parsed_files_data:
        if not header_info: continue
        
        file_account = header_info.get('ОсновнойСчетФайла')
        file_path_for_log = header_info.get('_filepath', 'Неизвестный')
        
        if not file_account:
            logger.warning(f"Файл {file_path_for_log} не содержит 'ОсновнойСчетФайла' в заголовке.")
            continue
            
        # Добавляем информацию из заголовка
        account_index[file_account]['header_inn'] = header_info.get("ИНН")
        account_index[file_account]['sources'].add(file_path_for_log)
        
        # Добавляем имена из заголовка
        header_name = header_info.get('Плательщик') or header_info.get('Получатель')
        if header_name:
            account_index[file_account]['names'].update([header_name])
        
        # Обрабатываем документы
        if documents:
            all_docs.extend(documents)
            for doc in documents:
                p_acc, r_acc = doc.get('ПлательщикСчет'), doc.get('ПолучательСчет')
                p_name, r_name = get_doc_party_name(doc, 'Плательщик'), get_doc_party_name(doc, 'Получатель')
                p_inn, r_inn = doc.get('ПлательщикИНН'), doc.get('ПолучательИНН')
                
                # Добавляем данные плательщика
                if p_acc in account_index:
                    if p_name: account_index[p_acc]['names'].update([p_name])
                    if p_inn: account_index[p_acc]['inns'].add(p_inn)
                
                # Добавляем данные получателя
                if r_acc in account_index:
                    if r_name: account_index[r_acc]['names'].update([r_name])
                    if r_inn: account_index[r_acc]['inns'].add(r_inn)

    logger.debug(f"Создан индекс для {len(account_index)} счетов")
    
    # Обрабатываем каждый счет из индекса
    for account, data in account_index.items():
        # Выбираем лучшее имя
        best_raw_name = get_best_name(data['names'], DEFAULT_ORG_NAME_PREFIX)
        
        # Определяем ИНН (приоритет: из документов, затем из заголовка)
        final_inn = None
        if data['inns']:
            # Если есть несколько ИНН, берем самый частый или первый
            inn_list = list(data['inns'])
            final_inn = inn_list[0] if len(inn_list) == 1 else inn_list[0]
        elif data['header_inn']:
            final_inn = data['header_inn']
        
        # Пытаемся нормализовать имя
        if best_raw_name and DEFAULT_ORG_NAME_PREFIX not in best_raw_name:
            name_norm, form, _ = normalize_and_classify(best_raw_name, final_inn)
            if name_norm and name_norm != '?':
                detected_orgs[account] = {
                    'name': best_raw_name, 
                    'normalized': name_norm, 
                    'legal_form': form, 
                    'inn': final_inn or ''
                }
                logger.info(f"  Определена организация: Счет {account} -> '{name_norm}' (Форма: {form}, ИНН: {final_inn or 'нет'})")
            else:
                # Fallback к дефолтному имени
                detected_orgs[account] = {
                    'name': f"{DEFAULT_ORG_NAME_PREFIX} {account}",
                    'normalized': f"{DEFAULT_ORG_NAME_PREFIX} {account}",
                    'legal_form': 'ДРУГОЕ',
                    'inn': final_inn or ''
                }
                logger.warning(f"  Счет {account}: Имя '{best_raw_name[:30]}...' не нормализовалось. Используется дефолтное.")
        else:
            # Нет подходящего имени
            detected_orgs[account] = {
                'name': f"{DEFAULT_ORG_NAME_PREFIX} {account}",
                'normalized': f"{DEFAULT_ORG_NAME_PREFIX} {account}",
                'legal_form': 'ДРУГОЕ',
                'inn': final_inn or ''
            }
            logger.warning(f"  Счет {account}: Имя не найдено. Используется дефолтное.")

    logger.info(f"Определение организаций завершено. Найдено {len(detected_orgs)} уникальных счетов организаций.")
    if logger.isEnabledFor(logging.DEBUG):
        for acc, data in detected_orgs.items():
             logger.debug(f"    Итог: {acc} -> '{data['normalized']}' (Raw: '{data['name'][:50]}...', Form: {data['legal_form']}, INN: {data.get('inn', 'N/A')})")
    return detected_orgs


def _determine_transaction_type_and_parties(doc: Dict, our_accounts: Set[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Определяет тип транзакции и участников.
    
    Returns:
        Tuple[type, our_account, cp_name, cp_inn, cp_account, our_details]
        type: 'income' или 'expense' или None
    """
    p_acc, r_acc = doc.get('ПлательщикСчет'), doc.get('ПолучательСчет')
    f_acc = doc.get('СчетФайла')
    
    p_name_raw = get_doc_party_name(doc, 'Плательщик')
    r_name_raw = get_doc_party_name(doc, 'Получатель')
    
    is_p_ours = p_acc in our_accounts
    is_r_ours = r_acc in our_accounts
    
    # Определяем тип операции и участников
    if p_acc == f_acc and is_p_ours:
        return 'expense', p_acc, r_name_raw, doc.get('ПолучательИНН', ''), r_acc, None
    elif r_acc == f_acc and is_r_ours:
        return 'income', r_acc, p_name_raw, doc.get('ПлательщикИНН', ''), p_acc, None
    else:
        # Случай несовпадения счета файла
        if is_p_ours and not is_r_ours:
            return 'expense', p_acc, r_name_raw, doc.get('ПолучательИНН', ''), r_acc, None
        elif is_r_ours and not is_p_ours:
            return 'income', r_acc, p_name_raw, doc.get('ПлательщикИНН', ''), p_acc, None
        else:
            return None, None, None, None, None, None

def _create_transaction_data(doc: Dict, our_details: Dict, our_acc: str, type: str, 
                           cp_raw: str, cp_inn: str, cp_acc: str) -> Dict:
    """Создает структуру данных транзакции."""
    date_field = 'ДатаСписано' if type == 'expense' else 'ДатаПоступило'
    date_str = doc.get(date_field) or doc.get('Дата')
    date_oper = parse_date(date_str)
    amount = safe_float(doc.get('Сумма'))
    
    cp_inn_clean = cp_inn.strip() if cp_inn else ''
    cp_acc_clean = cp_acc.strip() if cp_acc else ''
    cp_norm, cp_legal_form, cp_raw_original = normalize_and_classify(cp_raw, cp_inn_clean)
    
    # Генерируем ID контрагента
    if cp_inn_clean and cp_inn_clean != '0':
        cp_id = f"INN:{cp_inn_clean}"
    else:
        name_part_for_id = "?"
        if cp_norm and cp_norm != '?': 
            name_part_for_id = cp_norm.upper()
        elif cp_raw_original and cp_raw_original != '?': 
            name_part_for_id = cp_raw_original.upper()
        else: 
            name_part_for_id = "БЕЗ_ИМЕНИ"
        acc_part = cp_acc_clean if cp_acc_clean else "БЕЗ_СЧЕТА"
        cp_id = f"NAME_ACC:{name_part_for_id}|{acc_part}"
    
    # Форматируем отображаемое имя для ФИО
    display_name_hint = None
    if cp_norm and cp_norm != '?' and cp_legal_form in ['ИП', 'ФЛ']:
        display_name_hint = format_fio_display(cp_norm)
    
    return {
        'our_org_normalized': our_details.get('normalized', '?'),
        'our_org_original': our_details.get('name', '?'),
        'our_account': our_acc,
        'type': type,
        'cp_id': cp_id,
        'cp_name_raw': cp_raw_original or "?",
        'cp_name_normalized': cp_norm or "?",
        'cp_display_name_hint': display_name_hint,
        'cp_legal_form': cp_legal_form,
        'cp_inn': cp_inn_clean,
        'cp_account': cp_acc_clean,
        'date': date_oper.strftime('%Y-%m-%d'),
        'year': date_oper.year,
        'amount': amount,
        'doc_number': doc.get('Номер', ''),
        'purpose': doc.get('НазначениеПлатежа', '')
    }

def process_documents(all_documents: List[Dict], our_orgs_map: Dict[str, Dict]) -> List[Dict]:
    """
    Обрабатывает список всех документов, нормализует контрагентов,
    фильтрует ненужные транзакции и готовит данные для отчетов.
    """
    processed: List[Dict] = []
    our_accounts: Set[str] = set(our_orgs_map.keys())
    processed_count = 0
    skipped_internal = 0
    skipped_no_our_org = 0
    skipped_mismatch = 0
    skipped_details_missing = 0
    skipped_invalid_data = 0
    skipped_no_cp_name = 0

    global DEBUG_NAMES_LIST, DEBUG_TRANSACTIONS_LIST
    if DEBUG_MODE:
        DEBUG_NAMES_LIST = []
        DEBUG_TRANSACTIONS_LIST = []

    total_docs = len(all_documents)
    logger.info(f"Начало обработки {total_docs} документов...")

    for doc_index, doc in enumerate(all_documents):
        doc_num_str = doc.get('Номер', 'б/н')
        doc_date_str = doc.get('Дата', '?')
        file_basename = os.path.basename(doc.get('_filepath','?'))
        log_prefix = f"Doc {doc_index+1} (№{doc_num_str} от {doc_date_str}, Файл: {file_basename}):"

        p_acc, r_acc = doc.get('ПлательщикСчет'), doc.get('ПолучательСчет')
        is_p_ours = p_acc in our_accounts
        is_r_ours = r_acc in our_accounts

        # Фильтрация: пропускаем транзакции без наших организаций
        if not (is_p_ours or is_r_ours):
            logger.debug(f"{log_prefix} SKIP (Не наша транзакция)")
            skipped_no_our_org += 1
            continue

        # Фильтрация: пропускаем внутренние переводы
        if is_p_ours and is_r_ours:
            logger.debug(f"{log_prefix} SKIP (Внутренний перевод): {p_acc} -> {r_acc}")
            skipped_internal += 1
            continue

        # Определяем тип операции и участников
        type, our_acc, cp_raw, cp_inn, cp_acc, _ = _determine_transaction_type_and_parties(doc, our_accounts)
        
        if not type:
            logger.warning(f"{log_prefix} SKIP (Несоответствие счета файла): Не удалось определить тип. "
                          f"Файл={doc.get('СчетФайла')}, Плат={p_acc}({is_p_ours}), Пол={r_acc}({is_r_ours}).")
            skipped_mismatch += 1
            continue

        # Получаем детали нашей организации
        our_details = our_orgs_map.get(our_acc)
        if not our_details:
            logger.error(f"{log_prefix} SKIP (Ошибка: детали нашей организации): Нет данных для счета {our_acc}. Контрагент: '{cp_raw[:50]}...'")
            skipped_details_missing += 1
            continue

        # Проверяем наличие имени контрагента
        if not cp_raw or cp_raw == '?':
            logger.warning(f"{log_prefix} SKIP (Нет имени контрагента): Тип={type}, ИНН КА={cp_inn}, Счет КА={cp_acc}.")
            skipped_no_cp_name += 1
            continue

        # Валидация даты и суммы
        date_field = 'ДатаСписано' if type == 'expense' else 'ДатаПоступило'
        date_str = doc.get(date_field) or doc.get('Дата')
        date_oper = parse_date(date_str)
        amount = safe_float(doc.get('Сумма'))

        if not date_oper or amount <= 0:
            logger.debug(f"{log_prefix} SKIP (Невалидные данные): Дата='{date_str}', Сумма='{doc.get('Сумма')}'. Контрагент: '{cp_raw[:50]}...'")
            skipped_invalid_data += 1
            continue

        processed_count += 1

        # Создаем структуру данных транзакции
        transaction_data = _create_transaction_data(doc, our_details, our_acc, type, cp_raw, cp_inn, cp_acc)
        processed.append(transaction_data)

        # Отладочная информация
        if DEBUG_MODE:
            DEBUG_NAMES_LIST.append({
                'original': transaction_data['cp_name_raw'],
                'normalized': transaction_data['cp_name_normalized'],
                'form': transaction_data['cp_legal_form'],
                'inn': transaction_data['cp_inn']
            })
            DEBUG_TRANSACTIONS_LIST.append({
                **transaction_data,
                '_doc_index': doc_index + 1,
                '_file': file_basename,
                '_skipped': False
            })

    # Перевод строки после прогресс-бара, если он был
    # print()
    logger.info(f"Обработка документов завершена. Найдено {processed_count} значащих транзакций.")
    logger.info("Статистика пропущенных документов:")
    logger.info(f"  Внутренние переводы: {skipped_internal}")
    logger.info(f"  Не наши транзакции: {skipped_no_our_org}")
    logger.info(f"  Несоответствие счета файла/неясный тип: {skipped_mismatch}")
    logger.info(f"  Ошибка: детали нашей организации: {skipped_details_missing}")
    logger.info(f"  Невалидные дата/сумма: {skipped_invalid_data}")
    logger.info(f"  Нет имени контрагента: {skipped_no_cp_name}")
    total_skipped = (skipped_internal + skipped_no_our_org + skipped_mismatch +
                     skipped_details_missing + skipped_invalid_data + skipped_no_cp_name)
    logger.info(f"  Всего пропущено: {total_skipped} / {total_docs}")

    return processed

# --- Функции debug_save_names и debug_save_processed_transactions ---
# Вызываются только из __main__ при DEBUG_MODE=True

def debug_save_names(output_dir: str) -> None:
    """
    Сохраняет отладочную информацию по нормализации имен в CSV (только в DEBUG_MODE).
    
    Args:
        output_dir: Путь к папке для сохранения файла
    """
    if not DEBUG_MODE: return
    filepath = os.path.join(output_dir, DEBUG_NAMES_FILENAME)
    if not DEBUG_NAMES_LIST:
        logger.warning("Отладочный список имен пуст, файл не создан.")
        return

    unique_names_data = {}
    for item in DEBUG_NAMES_LIST:
        key = (item['original'], item['inn'])
        if key not in unique_names_data:
            unique_names_data[key] = {'normalized_set': set(), 'form_set': set()}
        unique_names_data[key]['normalized_set'].add(item['normalized'])
        unique_names_data[key]['form_set'].add(item['form'])

    output_rows = [
        {
            'original': k[0],
            'inn': k[1],
            'normalized': " | ".join(sorted(list(n for n in v['normalized_set'] if n is not None and n != '?'))) or "?",
            'form': " | ".join(sorted(list(v['form_set'])))
        }
        for k, v in unique_names_data.items()
    ]
    sorted_names_data = sorted(output_rows, key=lambda x: (x.get('form', ''), x.get('original', '').lower()))

    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['original', 'normalized', 'form', 'inn']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(sorted_names_data)
        logger.info(f"Отладочный файл имен сохранен: {filepath} ({len(sorted_names_data)} строк)")
    except Exception as e:
        logger.error(f"Ошибка сохранения отладочного файла имен '{filepath}': {e}", exc_info=True)

def debug_save_processed_transactions(output_dir: str, name_filter: Optional[str] = None) -> None:
    """
    Сохраняет обработанные транзакции в CSV (только в DEBUG_MODE).
    Может фильтровать по имени контрагента.
    
    Args:
        output_dir: Путь к папке для сохранения файла
        name_filter: Опциональный фильтр по имени контрагента
    """
    if not DEBUG_MODE: return
    filepath = os.path.join(output_dir, DEBUG_TRANSACTIONS_FILENAME)
    if not DEBUG_TRANSACTIONS_LIST:
        logger.warning("Список транзакций для отладки пуст, файл не создан.")
        return

    transactions_to_save = DEBUG_TRANSACTIONS_LIST
    if name_filter:
        name_filter_lower = name_filter.lower()
        logger.info(f"Фильтрация отладочных транзакций по имени: '{name_filter}'...")
        filtered_transactions = [
            t for t in DEBUG_TRANSACTIONS_LIST
            if name_filter_lower in t.get('cp_name_raw', '').lower()
        ]
        logger.info(f"Результат фильтрации: {len(filtered_transactions)} из {len(transactions_to_save)} транзакций.")
        transactions_to_save = filtered_transactions

    if not transactions_to_save:
        logger.warning(f"Транзакции, соответствующие фильтру '{name_filter}' (или все), не найдены. Файл '{DEBUG_TRANSACTIONS_FILENAME}' не будет создан/будет очищен.")
        if os.path.exists(filepath):
            try:
                 with open(filepath, 'w', newline='', encoding='utf-8') as csvfile: csvfile.write("")
                 logger.info(f"Отладочный файл очищен: {filepath}")
            except OSError as e: logger.error(f"Не удалось очистить отладочный файл {filepath}: {e}")
        return

    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
             if transactions_to_save:
                 fieldnames_set = set()
                 all_keys = []
                 for t in transactions_to_save:
                    for k in t.keys():
                        if k not in fieldnames_set:
                            fieldnames_set.add(k)
                            all_keys.append(k)
                 fieldnames = all_keys

                 writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
                 writer.writeheader()
                 writer.writerows(transactions_to_save)
                 logger.info(f"Отладочный файл транзакций сохранен: {filepath} ({len(transactions_to_save)} строк)")
    except Exception as e:
        logger.error(f"Ошибка сохранения отладочного файла транзакций '{filepath}': {e}", exc_info=True)