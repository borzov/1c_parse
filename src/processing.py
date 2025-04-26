#!filepath: src/processing.py
# src/processing.py
# -*- coding: utf-8 -*-
import os
import csv
import logging
import sys
from collections import defaultdict, Counter
from typing import Optional, List, Dict, Set, Tuple, Any
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

    Args:
        parsed_files_data: Список кортежей (header_info, documents) для каждого файла.

    Returns:
        Словарь, где ключ - расчетный счет нашей организации,
        значение - словарь с информацией ('name', 'normalized', 'legal_form', 'inn').
    """
    logger.info("Автоматическое определение организаций (2 прохода)...")
    detected_orgs: Dict[str, Dict] = {}
    needing_names: Dict[str, Dict] = {} # Счета, для которых имя не нашлось сразу
    all_docs: List[Dict] = [doc for _, docs in parsed_files_data if docs for doc in docs]
    processed_file_accounts: Set[str] = set()

    logger.debug("  Проход 1: Поиск имен в 'родных' файлах...")
    for header_info, documents in parsed_files_data:
        if not header_info: continue
        file_account = header_info.get('ОсновнойСчетФайла')
        file_path_for_log = header_info.get('_filepath', 'Неизвестный')
        if not file_account:
            logger.warning(f"Файл {file_path_for_log} не содержит 'ОсновнойСчетФайла' в заголовке.")
            continue
        if file_account in processed_file_accounts:
            logger.debug(f"Счет {file_account} уже обработан в Проходе 1 (из файла {file_path_for_log}), пропуск.")
            continue
        processed_file_accounts.add(file_account)

        org_inn = header_info.get("ИНН")
        name_raw = None

        payer_doc = next((d for d in documents if d.get('ПлательщикСчет') == file_account and get_doc_party_name(d, 'Плательщик')), None)
        if payer_doc:
            name_raw = get_doc_party_name(payer_doc, 'Плательщик')
            if not org_inn: org_inn = payer_doc.get('ПлательщикИНН')
            logger.debug(f"Найдено имя для счета {file_account} как Плательщик: '{name_raw[:50]}...' (в файле {file_path_for_log})")

        if not name_raw:
            receiver_doc = next((d for d in documents if d.get('ПолучательСчет') == file_account and get_doc_party_name(d, 'Получатель')), None)
            if receiver_doc:
                name_raw = get_doc_party_name(receiver_doc, 'Получатель')
                if not org_inn: org_inn = receiver_doc.get('ПолучательИНН')
                logger.debug(f"Найдено имя для счета {file_account} как Получатель: '{name_raw[:50]}...' (в файле {file_path_for_log})")

        if not name_raw:
            name_raw = header_info.get('Плательщик') or header_info.get('Получатель')
            if name_raw: logger.debug(f"Найдено имя для счета {file_account} в заголовке файла {file_path_for_log}: '{name_raw[:50]}...'")

        if name_raw:
            name_norm, form, _ = normalize_and_classify(name_raw, org_inn)
            if name_norm and name_norm != '?':
                detected_orgs[file_account] = {'name': name_raw, 'normalized': name_norm, 'legal_form': form, 'inn': org_inn or ''}
                logger.info(f"  Определена организация: Счет {file_account} -> '{name_norm}' (Форма: {form}, ИНН: {org_inn or 'нет'})")
            else:
                needing_names[file_account] = {'name': f"{DEFAULT_ORG_NAME_PREFIX} {file_account}", 'normalized': f"{DEFAULT_ORG_NAME_PREFIX} {file_account}", 'legal_form': 'ДРУГОЕ', 'inn': org_inn or '', 'sources': [file_path_for_log]}
                logger.warning(f"  Счет {file_account}: Имя '{name_raw[:30]}...' найдено (П1), но не нормализовалось. Требуется Проход 2.")
        else:
            needing_names[file_account] = {'name': f"{DEFAULT_ORG_NAME_PREFIX} {file_account}", 'normalized': f"{DEFAULT_ORG_NAME_PREFIX} {file_account}", 'legal_form': 'ДРУГОЕ', 'inn': org_inn or '', 'sources': [file_path_for_log]}
            logger.warning(f"  Счет {file_account}: Имя не найдено в 'родном' файле (П1) {file_path_for_log}. Требуется Проход 2.")

    logger.debug("  Проход 2: Поиск имен для оставшихся счетов...")
    if needing_names:
        names_found_global: Dict[str, Counter[str]] = defaultdict(Counter)
        inns_found_global: Dict[str, Set[str]] = defaultdict(set)

        for doc in all_docs:
            p_acc, r_acc = doc.get('ПлательщикСчет'), doc.get('ПолучательСчет')
            p_name, r_name = get_doc_party_name(doc, 'Плательщик'), get_doc_party_name(doc, 'Получатель')
            p_inn, r_inn = doc.get('ПлательщикИНН'), doc.get('ПолучательИНН')

            if p_acc in needing_names:
                if p_name: names_found_global[p_acc].update([p_name])
                if p_inn: inns_found_global[p_acc].add(p_inn)
            if r_acc in needing_names:
                if r_name: names_found_global[r_acc].update([r_name])
                if r_inn: inns_found_global[r_acc].add(r_inn)

        for acc, default_data in needing_names.items():
            if acc in detected_orgs: continue

            name_counts = names_found_global.get(acc)
            best_raw = get_best_name(name_counts, DEFAULT_ORG_NAME_PREFIX) if name_counts else None
            acc_inns = inns_found_global.get(acc)
            inn_for_detection = list(acc_inns)[0] if acc_inns and len(acc_inns) == 1 else default_data.get('inn')

            if best_raw and DEFAULT_ORG_NAME_PREFIX not in best_raw:
                name_norm, form, _ = normalize_and_classify(best_raw, inn_for_detection)
                if name_norm and name_norm != '?':
                    detected_orgs[acc] = {'name': best_raw, 'normalized': name_norm, 'legal_form': form, 'inn': inn_for_detection or ''}
                    logger.info(f"  Определена организация (П2): Счет {acc} -> '{name_norm}' (Форма: {form}, ИНН: {inn_for_detection or 'нет'})")
                else:
                    detected_orgs[acc] = default_data
                    logger.warning(f"  Счет {acc}: Лучшее имя '{best_raw[:30]}...' найдено (П2), но не нормализовалось. Используется дефолтное.")
            else:
                detected_orgs[acc] = default_data
                logger.warning(f"  Счет {acc}: Имя не найдено (П2). Используется дефолтное.")
    else:
        logger.debug("  Счета, требующие Прохода 2, отсутствуют.")

    logger.info(f"Определение организаций завершено. Найдено {len(detected_orgs)} уникальных счетов организаций.")
    if logger.isEnabledFor(logging.DEBUG):
        for acc, data in detected_orgs.items():
             logger.debug(f"    Итог: {acc} -> '{data['normalized']}' (Raw: '{data['name'][:50]}...', Form: {data['legal_form']}, INN: {data.get('inn', 'N/A')})")
    return detected_orgs


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
        # Индикатор прогресса убран из production кода
        # if (doc_index + 1) % 500 == 0 or (doc_index + 1) == total_docs:
        #     progress = (doc_index + 1) / total_docs
        #     progress_bar = '#' * int(progress * 20) + '-' * (20 - int(progress * 20))
        #     print(f"\rОбработка документов: [{progress_bar}] {doc_index+1}/{total_docs}", end='')
        #     sys.stdout.flush()

        doc_num_str = doc.get('Номер', 'б/н')
        doc_date_str = doc.get('Дата', '?')
        file_basename = os.path.basename(doc.get('_filepath','?'))
        log_prefix = f"Doc {doc_index+1} (№{doc_num_str} от {doc_date_str}, Файл: {file_basename}):"

        p_acc, r_acc = doc.get('ПлательщикСчет'), doc.get('ПолучательСчет')
        f_acc = doc.get('СчетФайла')

        p_name_raw = get_doc_party_name(doc, 'Плательщик')
        r_name_raw = get_doc_party_name(doc, 'Получатель')

        is_p_ours = p_acc in our_accounts
        is_r_ours = r_acc in our_accounts

        if not (is_p_ours or is_r_ours):
            logger.debug(f"{log_prefix} SKIP (Не наша транзакция)")
            skipped_no_our_org += 1
            continue

        if is_p_ours and is_r_ours:
            logger.debug(f"{log_prefix} SKIP (Внутренний перевод): {p_acc} -> {r_acc}")
            skipped_internal += 1
            continue

        type, our_details, our_acc = None, None, None
        cp_raw, cp_inn, cp_acc = None, None, None

        if p_acc == f_acc and is_p_ours:
            type, our_acc = 'expense', p_acc
            our_details = our_orgs_map.get(our_acc)
            cp_raw, cp_inn, cp_acc = r_name_raw, doc.get('ПолучательИНН',''), r_acc
        elif r_acc == f_acc and is_r_ours:
            type, our_acc = 'income', r_acc
            our_details = our_orgs_map.get(our_acc)
            cp_raw, cp_inn, cp_acc = p_name_raw, doc.get('ПлательщикИНН',''), p_acc
        else:
            if is_p_ours and not is_r_ours:
                 type, our_acc = 'expense', p_acc
                 our_details = our_orgs_map.get(our_acc)
                 cp_raw, cp_inn, cp_acc = r_name_raw, doc.get('ПолучательИНН',''), r_acc
                 logger.debug(f"{log_prefix} INFO (Счет файла не совпал): Мы плательщик ({p_acc}), тип 'expense'.")
            elif is_r_ours and not is_p_ours:
                 type, our_acc = 'income', r_acc
                 our_details = our_orgs_map.get(our_acc)
                 cp_raw, cp_inn, cp_acc = p_name_raw, doc.get('ПлательщикИНН',''), p_acc
                 logger.debug(f"{log_prefix} INFO (Счет файла не совпал): Мы получатель ({r_acc}), тип 'income'.")
            else:
                 logger.warning(f"{log_prefix} SKIP (Несоответствие счета файла): Не удалось определить тип. "
                                f"Файл={f_acc}, Плат={p_acc}({is_p_ours}), Пол={r_acc}({is_r_ours}).")
                 skipped_mismatch += 1
                 continue

        if not our_details:
            logger.error(f"{log_prefix} SKIP (Ошибка: детали нашей организации): Нет данных для счета {our_acc}. Контрагент: '{cp_raw[:50]}...'")
            skipped_details_missing += 1
            continue

        if not cp_raw or cp_raw == '?':
            logger.warning(f"{log_prefix} SKIP (Нет имени контрагента): Тип={type}, ИНН КА={cp_inn}, Счет КА={cp_acc}.")
            skipped_no_cp_name += 1
            continue

        date_field = 'ДатаСписано' if type == 'expense' else 'ДатаПоступило'
        date_str = doc.get(date_field) or doc.get('Дата')
        date_oper = parse_date(date_str)
        amount = safe_float(doc.get('Сумма'))

        if not date_oper or amount <= 0:
            logger.debug(f"{log_prefix} SKIP (Невалидные данные): Дата='{date_str}', Сумма='{doc.get('Сумма')}'. Контрагент: '{cp_raw[:50]}...'")
            skipped_invalid_data += 1
            continue

        processed_count += 1

        cp_inn_clean = cp_inn.strip() if cp_inn else ''
        cp_acc_clean = cp_acc.strip() if cp_acc else ''
        cp_norm, cp_legal_form, cp_raw_original = normalize_and_classify(cp_raw, cp_inn_clean)

        if DEBUG_MODE:
            DEBUG_NAMES_LIST.append({
                'original': cp_raw_original or "?",
                'normalized': cp_norm or '?',
                'form': cp_legal_form,
                'inn': cp_inn_clean})

        if cp_inn_clean and cp_inn_clean != '0':
            cp_id = f"INN:{cp_inn_clean}"
        else:
            name_part_for_id = "?"
            if cp_norm and cp_norm != '?': name_part_for_id = cp_norm.upper()
            elif cp_raw_original and cp_raw_original != '?': name_part_for_id = cp_raw_original.upper()
            else: name_part_for_id = "БЕЗ_ИМЕНИ"
            acc_part = cp_acc_clean if cp_acc_clean else "БЕЗ_СЧЕТА"
            cp_id = f"NAME_ACC:{name_part_for_id}|{acc_part}"

        display_name_hint = None
        if cp_norm and cp_norm != '?' and cp_legal_form in ['ИП', 'ФЛ']:
            display_name_hint = format_fio_display(cp_norm)

        transaction_data = {
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
        processed.append(transaction_data)

        if DEBUG_MODE:
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

def debug_save_names(output_dir: str):
    """Сохраняет отладочную информацию по нормализации имен в CSV (только в DEBUG_MODE)."""
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

def debug_save_processed_transactions(output_dir: str, name_filter: Optional[str] = None):
    """
    Сохраняет обработанные транзакции в CSV (только в DEBUG_MODE).
    Может фильтровать по имени контрагента.
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