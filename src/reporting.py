#!filepath: src/reporting.py
# src/reporting.py
# -*- coding: utf-8 -*-
import os
import json
import logging
from datetime import datetime
from collections import defaultdict, Counter
from typing import Optional, List, Dict, Set, Tuple, Any
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import (
    TEMPLATES_DIR, TEMPLATE_ANNUAL, TEMPLATE_COMPARISON,
    DEFAULT_ORG_NAME_PREFIX, DEBUG_MODE
)
from .utils import get_best_name, format_currency
from .normalization import normalize_and_classify, format_fio_display

logger = logging.getLogger(__name__)

# --- Инициализация Jinja2 ---
try:
    jinja_env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        trim_blocks=True,
        lstrip_blocks=True,
        autoescape=select_autoescape(['html', 'xml'])
    )
    jinja_env.globals['format_currency'] = format_currency
    jinja_env.globals['format_fio_display'] = format_fio_display
    logger.debug("Jinja2 Environment инициализирован успешно.")
except Exception as e:
    logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации Jinja2. Путь к шаблонам: '{TEMPLATES_DIR}'. Ошибка: {e}", exc_info=DEBUG_MODE)
    jinja_env = None

# --- Вспомогательная функция для подготовки деталей контрагента ---

def _prepare_final_cp_details(cp_data: Dict, org_data_cache: Dict, report_name: str) -> Dict:
    """
    Финализирует детали контрагента перед рендерингом отчета.
    Оптимизированная версия с улучшенным кэшированием.
    """
    details = cp_data.get('details')
    if not details:
        logger.error(f"[{report_name}] Ошибка: нет ключа 'details' в cp_data ID {cp_data.get('id')}")
        return {'error': 'Missing details'}

    agg_key = cp_data.get('id', details.get('cp_id'))
    if not agg_key:
         logger.error(f"[{report_name}] Ошибка: не определен agg_key для данных: {cp_data}")
         return {'error': 'Missing key'}

    # Проверяем, есть ли уже готовые нормализованные данные из транзакций
    if 'name_normalized' in details and 'legal_form' in details:
        # Данные уже нормализованы, используем их
        logger.debug(f"[{report_name}] Использованы готовые нормализованные данные для {agg_key}")
        final_name_norm = details['name_normalized']
        final_legal_form = details['legal_form']
    else:
        # Нужна нормализация
        raw_names_set = details.get('raw_names') or set(details.get('names_counter', {}).keys())
        current_inn = details.get('inn','')
        cache_key = (current_inn, tuple(sorted(raw_names_set)))

        if cache_key in org_data_cache:
            # Используем кэшированные данные
            final_name_norm, final_legal_form, best_raw_name = org_data_cache[cache_key]
            logger.debug(f"[{report_name}] Использованы кэшированные данные для {agg_key}: '{final_name_norm}', '{final_legal_form}'")
        else:
            # Выбираем лучшее сырое имя
            names_cnt = details.get('names_counter') or Counter(raw_names_set)
            best_raw_name = get_best_name(names_cnt, DEFAULT_ORG_NAME_PREFIX)
            logger.debug(f"[{report_name}] Нормализация для {agg_key}: лучшее сырое имя='{best_raw_name}', ИНН='{current_inn}'")

            # Повторно нормализуем лучшее сырое имя с ИНН
            final_name_norm, final_legal_form, _ = normalize_and_classify(best_raw_name, current_inn)

            # Обработка случая, если нормализация не удалась (fallback)
            if final_name_norm is None or final_name_norm == '?':
                logger.warning(f"[{report_name}] Повторная нормализация для {agg_key} ('{best_raw_name}') не дала имени. Используем fallback.")
                # Пытаемся найти другое валидное сырое имя
                fallback_raw = next((name for name, count in names_cnt.most_common() if name and name != '?' and DEFAULT_ORG_NAME_PREFIX not in name), None)
                if fallback_raw and fallback_raw != best_raw_name:
                     logger.debug(f"[{report_name}] Fallback для {agg_key}: используем другое сырое имя '{fallback_raw}'")
                     fb_norm, fb_form, _ = normalize_and_classify(fallback_raw, current_inn)
                     if fb_norm and fb_norm != '?':
                         final_name_norm = fb_norm
                         final_legal_form = fb_form
                     else:
                         final_name_norm = best_raw_name if best_raw_name else "?"
                         final_legal_form = 'ДРУГОЕ'
                else:
                     final_name_norm = best_raw_name if best_raw_name else "?"
                     final_legal_form = 'ДРУГОЕ'

                logger.warning(f"[{report_name}] Fallback результат для {agg_key}: Норм.имя='{final_name_norm}', Форма='{final_legal_form}'")

            # Кэшируем результат
            org_data_cache[cache_key] = (final_name_norm, final_legal_form, best_raw_name)
            logger.debug(f"[{report_name}] Данные для {agg_key} добавлены в кэш.")

        # Присваиваем финальные значения
        details['name_normalized'] = final_name_norm
        details['legal_form'] = final_legal_form

    # Формируем отображаемое имя (display_name) для шаблона
    display_name = None
    name_to_format = details['name_normalized']
    if name_to_format and name_to_format != '?':
         if final_legal_form in ('ИП', 'ФЛ'):
             display_name = format_fio_display(name_to_format)
         else:
             display_name = name_to_format
    details['display_name'] = display_name

    # Строка с оригинальными именами для tooltip
    raw_names_set = details.get('raw_names') or set(details.get('names_counter', {}).keys())
    raw_names_list = sorted(list(raw_names_set))
    raw_names_str = " | ".join(raw_names_list)
    max_raw_names_display = 3
    if len(raw_names_list) > max_raw_names_display:
         raw_names_str = f"{' | '.join(raw_names_list[:max_raw_names_display])} | ...и еще {len(raw_names_list) - max_raw_names_display}"
    details['raw_names_str'] = raw_names_str

    # Строка со счетами
    accounts_list = sorted(list(details.get('accounts', set())))
    details['accounts_str'] = ", ".join(accounts_list) if accounts_list else ""

    # Проверка наличия обязательных ключей перед возвратом
    if 'name_normalized' not in details:
         logger.error(f"[{report_name}] Ошибка: отсутствует 'name_normalized' в details {agg_key}")
         details['name_normalized'] = 'ОШИБКА_ИМЕНИ'
    if 'legal_form' not in details:
         logger.error(f"[{report_name}] Ошибка: отсутствует 'legal_form' в details {agg_key}")
         details['legal_form'] = 'ОШИБКА_ФОРМЫ'

    return details

# --- Генерация годового отчета ---

def generate_counterparty_annual_report(transactions: List[Dict], detected_orgs_map: Dict[str, Dict], output_path: str) -> None:
    """
    Генерирует годовой отчет по контрагентам.
    
    Args:
        transactions: Список обработанных транзакций
        detected_orgs_map: Словарь с информацией об организациях
        output_path: Путь для сохранения HTML-отчета
    """
    report_id = "Annual"
    if not jinja_env:
        logger.error(f"[{report_id}] Jinja2 не инициализирован, отчет '{os.path.basename(output_path)}' не сгенерирован.")
        return
    start_time = datetime.now()
    logger.info(f"[{report_id}] Начало генерации: '{os.path.basename(output_path)}'. Транзакций: {len(transactions)}")

    cp_aggregated_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'id': None, 'details': { 'cp_id': None, 'inn': '', 'account': '', 'accounts': set(), 'names_counter': Counter(), 'raw_names': set()},
        'totals': {'income': 0.0, 'expense': 0.0},
        'years': defaultdict(lambda: {'totals': {'income': 0.0, 'expense': 0.0}, 'by_org': defaultdict(lambda: {'income': 0.0, 'expense': 0.0})})
    })
    all_org_names_display = sorted(list(set(d.get('normalized', '?') for d in detected_orgs_map.values())))

    # Группируем транзакции по cp_id, year, org
    tx_by_cp_year_org = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for t in transactions:
        tx_by_cp_year_org[t['cp_id']][t['year']][t['our_org_normalized']].append(t)

    # Агрегация данных
    for i, t in enumerate(transactions):
        agg_key = t['cp_id']
        if cp_aggregated_data[agg_key]['id'] is None:
             cp_aggregated_data[agg_key]['id'] = agg_key
             cp_aggregated_data[agg_key]['details']['cp_id'] = agg_key
        agg = cp_aggregated_data[agg_key]; details = agg['details']
        year, org_norm = t['year'], t['our_org_normalized']
        details['names_counter'].update([t['cp_name_raw']]); details['raw_names'].add(t['cp_name_raw'])
        if not details['inn'] and t['cp_inn']: details['inn'] = t['cp_inn']
        if t['cp_account']: details['accounts'].add(t['cp_account']);
        if not details['account']: details['account'] = t['cp_account'] # Запоминаем первый встреченный счет
        y_data = agg['years'][year]; org_y_data = y_data['by_org'][org_norm]
        y_totals, cp_totals = y_data['totals'], agg['totals']; amount = t['amount']
        if t['type'] == 'income': org_y_data['income'] += amount; y_totals['income'] += amount; cp_totals['income'] += amount
        else: org_y_data['expense'] += amount; y_totals['expense'] += amount; cp_totals['expense'] += amount

    logger.info(f"[{report_id}] Агрегация завершена. Уникальных контрагентов: {len(cp_aggregated_data)}")

    report_data_json_list = []
    normalization_cache = {} # Кэш для _prepare_final_cp_details
    # Финальная подготовка данных и нормализация
    for agg_key, data in cp_aggregated_data.items():
        final_details = _prepare_final_cp_details(data, normalization_cache, report_id)
        if final_details.get('error'):
             logger.error(f"[{report_id}] Пропуск контрагента {agg_key} из-за ошибки: {final_details.get('error')}"); continue

        cp_entry = {
            'id': agg_key,
            'name_normalized': final_details.get('name_normalized', '?'),
            'display_name': final_details.get('display_name'), # Может быть None
            'legal_form': final_details.get('legal_form', 'ДРУГОЕ'),
            'inn': final_details.get('inn', ''),
            'account': final_details.get('account', ''),
            'accounts': final_details.get('accounts_str', ''),
            'raw_names': final_details.get('raw_names_str', ''),
            'total_income': data['totals']['income'],
            'total_expense': data['totals']['expense'],
            'years_details': []
        }
        for year, y_data in sorted(data['years'].items()):
            if y_data['totals']['income'] == 0 and y_data['totals']['expense'] == 0: continue
            y_entry = {
                'year': year,
                'year_income': y_data['totals']['income'],
                'year_expense': y_data['totals']['expense'],
                'orgs': [],
                'operations': []  # Новый ключ: все операции за год по этому контрагенту
            }
            for org, o_data in sorted(y_data['by_org'].items()):
                if o_data['income'] == 0 and o_data['expense'] == 0: continue
                y_entry['orgs'].append({'name': org, 'income': o_data['income'], 'expense': o_data['expense']})
                # Добавляем операции по этому org+year+cp
                ops = tx_by_cp_year_org[agg_key][year][org]
                for op in ops:
                    y_entry['operations'].append({
                        'date': op['date'],
                        'type': op['type'],
                        'amount': op['amount'],
                        'purpose': op.get('purpose', ''),
                        'doc_number': op.get('doc_number', ''),
                        'account': op.get('our_account', ''),
                        'org': op.get('our_org_normalized', ''),
                    })
            if y_entry['orgs'] or y_entry['year_income'] != 0 or y_entry['year_expense'] != 0:
                cp_entry['years_details'].append(y_entry)
        report_data_json_list.append(cp_entry)

    logger.info(f"[{report_id}] Подготовка данных завершена. Записей для отчета: {len(report_data_json_list)}")

    # Сортировка по имени (с учетом "?") и ИНН
    try:
        report_data_json_list.sort(key=lambda x: (
            str(x.get('name_normalized', 'zzz')).lower() if x.get('name_normalized', '?') != '?' else 'zzz',
            str(x.get('inn', '')) or ''
        ))
        logger.debug(f"[{report_id}] Сортировка данных для отчета завершена.")
    except Exception as e:
        logger.error(f"[{report_id}] Ошибка при сортировке данных: {e}", exc_info=DEBUG_MODE)

    # Рендеринг шаблона
    try:
        template = jinja_env.get_template(TEMPLATE_ANNUAL)
        report_data_json_string = json.dumps(report_data_json_list, ensure_ascii=False, separators=(',', ':'))
        # Собираем уникальные типы документов для фильтра
        doc_types = set()
        for t in transactions:
            if t.get('doc_number'):
                doc_types.add(t.get('doc_number', '').split()[0] if t.get('doc_number') else '')
        doc_types.discard('')  # Убираем пустые
        
        html_content = template.render(
            generation_time=datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
            analyzed_org_names=all_org_names_display,
            report_data_json=report_data_json_string,
            document_types=sorted(list(doc_types))
        )
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{report_id}] Отчет '{os.path.basename(output_path)}' успешно сгенерирован ({duration:.2f} сек).")
    except Exception as e:
        logger.error(f"[{report_id}] Ошибка генерации/записи отчета '{os.path.basename(output_path)}': {e}", exc_info=DEBUG_MODE)

# --- Генерация отчета о взаимодействии ---

def generate_org_comparison_report(transactions: List[Dict], detected_orgs_map: Dict[str, Dict], output_path: str) -> None:
    """
    Генерирует отчет о взаимодействии контрагентов с организациями.
    
    Args:
        transactions: Список обработанных транзакций
        detected_orgs_map: Словарь с информацией об организациях
        output_path: Путь для сохранения HTML-отчета
    """
    report_id = "Comparison"
    if not jinja_env:
        logger.error(f"[{report_id}] Jinja2 не инициализирован, отчет '{os.path.basename(output_path)}' не будет сгенерирован.")
        return
    start_time = datetime.now()
    logger.info(f"[{report_id}] Начало генерации: '{os.path.basename(output_path)}'. Транзакций: {len(transactions)}")

    comparison_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'id': None, 'details': { 'cp_id': None, 'inn':'', 'account':'', 'accounts': set(), 'names_counter': Counter(), 'raw_names': set(), 'has_income': False, 'has_expense': False, 'interacted_orgs': set() },
        'interactions': defaultdict(lambda: { 'types': set(), 'income_sum': 0.0, 'income_count': 0, 'expense_sum': 0.0, 'expense_count': 0 })
    })

    # Агрегация данных
    for i, t in enumerate(transactions):
        agg_key = t['cp_id']
        if comparison_data[agg_key]['id'] is None:
             comparison_data[agg_key]['id'] = agg_key
             comparison_data[agg_key]['details']['cp_id'] = agg_key
        org_norm = t['our_org_normalized']
        agg = comparison_data[agg_key]; details = agg['details']; interaction = agg['interactions'][org_norm]
        details['names_counter'].update([t['cp_name_raw']]); details['raw_names'].add(t['cp_name_raw'])
        if not details['inn'] and t['cp_inn']: details['inn'] = t['cp_inn']
        if t['cp_account']: details['accounts'].add(t['cp_account']);
        if not details['account']: details['account'] = t['cp_account'] # Первый встреченный счет
        interaction['types'].add(t['type'])
        if t['type'] == 'income':
            interaction['income_sum'] += t['amount']; interaction['income_count'] += 1; details['has_income'] = True
        else:
            interaction['expense_sum'] += t['amount']; interaction['expense_count'] += 1; details['has_expense'] = True

    logger.info(f"[{report_id}] Агрегация завершена. Уникальных контрагентов: {len(comparison_data)}")

    processed_comparison_data: Dict[str, Dict[str, Any]] = {}
    final_detected_legal_forms = set()
    normalization_cache = {} # Кэш для _prepare_final_cp_details
    # Финальная подготовка данных и нормализация
    for agg_key, data in comparison_data.items():
        final_details = _prepare_final_cp_details(data, normalization_cache, report_id)
        if final_details.get('error'):
            logger.error(f"[{report_id}] Пропуск контрагента {agg_key} из-за ошибки: {final_details.get('error')}"); continue
        # Добавляем список организаций, с которыми было взаимодействие
        final_details['interacted_orgs'] = sorted(list(data['interactions'].keys()))
        final_detected_legal_forms.add(final_details.get('legal_form', 'ДРУГОЕ'))
        data['details'] = final_details # Обновляем детали в исходной структуре
        processed_comparison_data[agg_key] = data

    logger.info(f"[{report_id}] Подготовка данных завершена. Записей для отчета: {len(processed_comparison_data)}")

    # Сортировка данных для шаблона по имени и ИНН
    sorted_comparison_data_for_template = {}
    try:
        sorted_agg_keys = sorted( processed_comparison_data.keys(), key=lambda k: (
            str(processed_comparison_data[k].get('details',{}).get('name_normalized', 'zzz')).lower()
            if processed_comparison_data[k].get('details',{}).get('name_normalized', '?') != '?' else 'zzz',
            str(processed_comparison_data[k].get('details',{}).get('inn', '')) or ''
            )
        )
        sorted_comparison_data_for_template = { key: processed_comparison_data[key] for key in sorted_agg_keys }
        logger.debug(f"[{report_id}] Сортировка данных для отчета завершена.")
    except Exception as e:
        logger.error(f"[{report_id}] Ошибка при сортировке данных: {e}", exc_info=DEBUG_MODE)
        sorted_comparison_data_for_template = processed_comparison_data # Используем несортированные в случае ошибки

    # Подготовка списка ОПФ для фильтра
    sorted_our_orgs_normalized = sorted(list(set(d.get('normalized', '?') for d in detected_orgs_map.values())))
    main_forms_for_filter = ['ИП', 'ООО', 'АО', 'ГОС', 'ФЛ'];
    relevant_legal_forms = sorted([f for f in final_detected_legal_forms if f in main_forms_for_filter])
    has_other_forms = any(f not in main_forms_for_filter and f not in ['ЮЛ', 'ДРУГОЕ'] for f in final_detected_legal_forms)
    if has_other_forms or 'ДРУГОЕ' in final_detected_legal_forms:
        if 'ДРУГОЕ' not in relevant_legal_forms: relevant_legal_forms.append('ДРУГОЕ')

    # Рендеринг шаблона
    try:
        template = jinja_env.get_template(TEMPLATE_COMPARISON)
        html_content = template.render(
            generation_time=datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
            comparison_data=sorted_comparison_data_for_template,
            our_org_names=sorted_our_orgs_normalized,
            legal_forms=relevant_legal_forms
        )
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{report_id}] Отчет '{os.path.basename(output_path)}' успешно сгенерирован ({duration:.2f} сек).")
    except Exception as e:
        logger.error(f"[{report_id}] Ошибка генерации/записи отчета '{os.path.basename(output_path)}': {e}", exc_info=DEBUG_MODE)