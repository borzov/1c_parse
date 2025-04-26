#!filepath: src/normalization.py
# src/normalization.py
# -*- coding: utf-8 -*-
import re
import logging
from typing import Optional, Tuple, Dict, List

logger = logging.getLogger(__name__)

# --- Паттерны и константы для нормализации v7.1.3 ---

# Определения ОПФ и компиляция паттернов (без изменений)
LEGAL_FORMS_KEYWORDS: Dict[str, List[str]] = {
    'ИП': [r'ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ', r'ИП'],
    'ООО': [r'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ', r'ООО'],
    'АО': [r'ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', r'НЕПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', r'АКЦИОНЕРНОЕ ОБЩЕСТВО', r'ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', r'ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', r'ПАО', r'НАО', r'ЗАО', r'ОАО', r'АО'],
    'ГОС': [r'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ', r'ФОНД СОЦИАЛЬНОГО СТРАХОВАНИЯ', r'ИФНС', r'УФК', r'ГУ', r'ФСС', r'ФГБУ', r'ФКУ', r'ФАУ', r'ФГУП', r'МИНИСТЕРСТВО', r'ДЕПАРТАМЕНТ', r'АДМИНИСТРАЦИЯ', r'КАЗНАЧЕЙСТВО', r'РОСПОТРЕБНАДЗОР', r'РОСМОЛОДЕЖЬ', r'УПРАВЛЕНИЕ МВД', r'ОТДЕЛЕНИЕ ФОНДА', r'ТУРИСТИЧЕСКИЙ ОТДЕЛ', r'ПОСОЛЬСТВО', r'УГИБДД', r'ОСП', r'АДМИНИСТРАТОР МОСКОВСКОГО ПАРКОВОЧНОГО'],
    'ФОНД': [r'БЛАГОТВОРИТЕЛЬНЫЙ ФОНД', r'ФОНД'],
    'АНО': [r'АВТОНОМНАЯ НЕКОММЕРЧЕСКАЯ ОРГАНИЗАЦИЯ', r'АНО'],
    'НКО': [r'НЕКОММЕРЧЕСКАЯ ОРГАНИЗАЦИЯ', r'НКО'],
    'АССОЦ': [r'АССОЦИАЦИЯ'], 'КООП': [r'КООПЕРАТИВ', r'КФХ'], 'ПАРТНЕРСТВО': [r'ПАРТНЕРСТВО', r'НП'],
    'АДВ_БЮРО': [r'АДВОКАТСКОЕ БЮРО'], 'КОЛЛ_АДВ': [r'КОЛЛЕГИЯ АДВОКАТОВ'],
    'ФИЛИАЛ': [r'ФИЛИАЛ', r'ПРЕДСТАВИТЕЛЬСТВО'],
    'LTD': [r'LIMITED', r'LTD'], 'LLC': [r'LLC'], 'GMBH': [r'GMBH'],
    'SIA': [r'SIA'], 'AS': [r'AS'], 'UAB': [r'UAB'], 'TOO': [r'TOO'],
    'CORP': [r'CORPORATION', r'CORP'], 'INC': [r'INC'], 'PLC': [r'PLC']
}
COMPILED_LEGAL_FORMS_FIND: Dict[str, List[re.Pattern]] = {}
for form_key, patterns in LEGAL_FORMS_KEYWORDS.items():
    compiled_patterns = []
    for p_str in patterns:
        escaped_p_str = re.escape(p_str)
        if form_key == 'ИП':
            regex = r'(?i)(?:(?:^' + escaped_p_str + r'\s+)|(?:\(\s*' + escaped_p_str + r'\s*\)))'
        else:
            regex = r'(?i)(?:\b' + escaped_p_str + r'\b|\(\s*' + escaped_p_str + r'\s*\))'
        try:
            compiled_patterns.append(re.compile(regex))
        except re.error as e:
            logger.error(f"Ошибка компиляции Regex FIND для ОПФ '{form_key}', паттерн '{p_str}': {e}")
    COMPILED_LEGAL_FORMS_FIND[form_key] = compiled_patterns

# Паттерны для удаления мусора
TRASH_PATTERNS_SPECIFIC = [
    re.compile(r'(?:^|\s)(?:Р/СЧ?|Л/СЧ?|К/СЧ?|БИК)\s*\d+.*', re.IGNORECASE),
    re.compile(r'\s+\bИНН\s*\d{10}(\d{2})?\b', re.IGNORECASE),
    re.compile(r'\s+\bКПП\s*\d{9}\b', re.IGNORECASE),
    re.compile(r'\s+\b(УНП|БИН)\s*\d+\b', re.IGNORECASE),
    re.compile(r'\s+ID[/\s:]*\d+'),
    re.compile(r'\s*//.*?//\s*', re.DOTALL),
    # Удалено слишком широкое правило для адресов
    re.compile(r'\s+\b(РОССИЯ|РФ|РЕСПУБЛИКА|КАЗАХСТАН|ЛИТВА|ЛАТВИЯ|РБ|KZ|LT|LV|DE|ГОРОД|ОБЛАСТЬ|КРАЙ|АО|ГО)\b.*?(?=(//|$|\s+В\s+\w+\s+БАНК))', re.IGNORECASE | re.DOTALL),
    re.compile(r'\b\d{6}\b'), # 6-значный индекс
    re.compile(r'\s+В\s+.*?\s+(БАНК\b|ФИЛИАЛ\b|ОАО\b|ПАО\b|АО\b|УФК\b|ОТДЕЛЕНИЕ\b)', re.IGNORECASE | re.DOTALL),
    re.compile(r'\s+Р/С\s+NULL\b', re.IGNORECASE),
    re.compile(r'\s+\(.*\)$', re.DOTALL)
]

# Прочие паттерны
QUOTES_PATTERN = re.compile(r'[\"„""«»\'`<>]+')
SPACES_PATTERN = re.compile(r'\s+')
IS_FIO_STRUCTURE = re.compile(r'^([А-ЯЁа-яё\-]+\s+)+[А-ЯЁа-яё\-.]+$')
INITIALS_PATTERN = re.compile(r'\b([А-ЯЁ])\s*\.\s*([А-ЯЁ])\s*\.?$')

# --- Функции ---

def detect_final_legal_form(raw_name: Optional[str], inn: Optional[str] = None) -> str:
    """Определяет ОПФ контрагента."""
    if not raw_name: return "ФЛ"
    upper_name = raw_name.upper().strip()
    if not upper_name: return "ФЛ"

    best_match_form = None
    max_len = 0
    for form_key, compiled_patterns in COMPILED_LEGAL_FORMS_FIND.items():
        for pattern in compiled_patterns:
            for match in pattern.finditer(upper_name):
                match_text = match.group(0).strip('() ')
                current_len = len(match_text)
                if current_len > max_len:
                    max_len = current_len
                    best_match_form = form_key
                elif current_len == max_len and form_key == 'ИП' and best_match_form != 'ИП':
                    best_match_form = form_key

    if best_match_form:
        logger.debug(f"Форма для {repr(raw_name)} -> '{best_match_form}' (по ключу)")
        return best_match_form

    if inn:
        inn = inn.strip()
        if len(inn) == 12:
            logger.debug(f"Форма для {repr(raw_name)} -> 'ФЛ' (по ИНН 12)")
            return "ФЛ"
        if len(inn) == 10:
            logger.debug(f"Форма для {repr(raw_name)} -> 'ЮЛ' (по ИНН 10)")
            return "ЮЛ"

    name_for_fio_check = re.sub(r'[\d.,\"\']', '', raw_name).strip()
    fio_words = re.findall(r'\b[А-ЯЁ][а-яё\-]+\b', name_for_fio_check)
    if len(fio_words) >= 2 and IS_FIO_STRUCTURE.match(name_for_fio_check):
        logger.debug(f"Форма для {repr(raw_name)} -> 'ФЛ' (по структуре ФИО)")
        return "ФЛ"

    logger.debug(f"Форма для {repr(raw_name)} -> 'ДРУГОЕ' (не определена)")
    return "ДРУГОЕ"


def format_fio_display(fio_str: Optional[str]) -> str:
    """Форматирует ФИО в вид 'Фамилия И.О.'."""
    if not fio_str: return "?"
    name = re.sub(r'^ИП\s+', '', fio_str.strip(), flags=re.IGNORECASE)
    name = QUOTES_PATTERN.sub('', name)
    name = SPACES_PATTERN.sub(' ', name).strip()
    words = [w.strip('.,') for w in name.split() if w.strip('.,')]
    if not words: return "?"

    initials_match = INITIALS_PATTERN.search(name)
    if initials_match:
        surname_part = name[:initials_match.start()].strip()
        if surname_part:
            surname = surname_part.split()[-1]
            return f"{surname.capitalize()} {initials_match.group(1).upper()}.{initials_match.group(2).upper()}."

    meaningful_words = [w for w in words if len(w) > 1 or w.isalpha()]
    is_likely_fio = all(re.match(r'^[А-ЯЁ]', w) for w in meaningful_words[:3])

    if len(meaningful_words) >= 3 and is_likely_fio:
        return f"{meaningful_words[0].capitalize()} {meaningful_words[1][0].upper()}.{meaningful_words[2][0].upper()}."
    if len(meaningful_words) == 2 and is_likely_fio:
        return f"{meaningful_words[0].capitalize()} {meaningful_words[1].capitalize()}"
    if len(meaningful_words) == 1:
        return meaningful_words[0].capitalize()

    return words[0].capitalize() + (' ' + ' '.join(words[1:]) if len(words)>1 else '')


def normalize_name_core(raw_name: Optional[str], detected_form: Optional[str] = None) -> Optional[str]:
    """Очищает имя от мусора и ОПФ."""
    if not raw_name or not isinstance(raw_name, str): return None
    name = raw_name.strip()
    if not name: return None
    logger.debug(f"Нормализация ядра для {repr(name)}, форма: {detected_form}")

    # 1. Удаление мусора
    cleaned = name
    for i, pattern in enumerate(TRASH_PATTERNS_SPECIFIC):
        cleaned_before = cleaned
        try:
            cleaned = pattern.sub(' ', cleaned)
            if cleaned != cleaned_before:
                 logger.debug(f"  Применено правило мусора #{i} ({pattern.pattern}): {repr(cleaned_before)} -> {repr(cleaned.strip())}")
        except Exception as e:
            logger.error(f"Ошибка Regex мусора #{i} ({pattern.pattern}) для {repr(cleaned_before)}: {e}")
            cleaned = cleaned_before
    cleaned = SPACES_PATTERN.sub(' ', cleaned).strip()
    if not cleaned:
        logger.debug(f"Имя стало пустым после чистки мусора.")
        return None
    logger.debug(f"Имя после чистки мусора: {repr(cleaned)}")

    # 2. Удаление ОПФ
    core_name = cleaned
    processed_opf = False
    if detected_form and detected_form in LEGAL_FORMS_KEYWORDS:
        logger.debug(f"Попытка удалить ОПФ '{detected_form}' из {repr(core_name)}")
        if detected_form == 'ИП':
            ip_prefix_str = 'ИП '
            if core_name.upper().startswith(ip_prefix_str):
                prefix_len = len(ip_prefix_str)
                new_name = core_name[prefix_len:].strip()
                logger.debug(f"  Удален префикс ИП -> {repr(new_name)}")
                core_name = new_name
                processed_opf = True
            # Дополнительная проверка для "ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ" (как отдельное слово)
            temp_core_name = core_name
            for opf_pattern_str in LEGAL_FORMS_KEYWORDS['ИП']:
                 if opf_pattern_str == 'ИП' and processed_opf: continue # Уже обработали префикс
                 try:
                     escaped_opf = re.escape(opf_pattern_str)
                     pattern_for_sub = re.compile(r'(?i)(?:\b' + escaped_opf + r'\b\s*|\s*\(\s*' + escaped_opf + r'\s*\)\s*)')
                     new_name, num_subs = pattern_for_sub.subn(' ', temp_core_name)
                     if num_subs > 0:
                         logger.debug(f"  Дополнительно удалена ОПФ ИП '{opf_pattern_str}' -> {repr(new_name.strip())}")
                         temp_core_name = new_name
                         processed_opf = True
                 except re.error as e: logger.error(f"Ошибка Regex при доп. удалении ОПФ ИП '{opf_pattern_str}': {e}")
            core_name = SPACES_PATTERN.sub(' ', temp_core_name).strip()
        else: # Для других ОПФ
             temp_core_name = core_name
             patterns_to_try = sorted(LEGAL_FORMS_KEYWORDS[detected_form], key=len, reverse=True)
             for opf_pattern_str in patterns_to_try:
                  try:
                      escaped_opf = re.escape(opf_pattern_str)
                      pattern_for_sub = re.compile(r'(?i)(?:\b' + escaped_opf + r'\b\s*|\s*\b' + escaped_opf + r'\b|\s*\(\s*' + escaped_opf + r'\s*\)\s*)')
                      new_name, num_subs = pattern_for_sub.subn(' ', temp_core_name)
                      if num_subs > 0:
                           logger.debug(f"  Удалена ОПФ '{opf_pattern_str}' -> {repr(new_name.strip())}")
                           temp_core_name = new_name
                           processed_opf = True
                  except re.error as e: logger.error(f"Ошибка Regex при удалении ОПФ '{opf_pattern_str}': {e}")
             core_name = SPACES_PATTERN.sub(' ', temp_core_name).strip()
        if processed_opf: logger.debug(f"Имя после удаления ОПФ: {repr(core_name)}")

    # 3. Удаление кавычек
    core_name_no_quotes = QUOTES_PATTERN.sub('', core_name).strip()
    if core_name != core_name_no_quotes:
        logger.debug(f"Имя после удаления кавычек: {repr(core_name_no_quotes)}")

    # 4. Финальная чистка пробелов
    final_core_name = SPACES_PATTERN.sub(' ', core_name_no_quotes).strip()
    logger.debug(f"Финальное ядро имени: {repr(final_core_name)}")

    # 5. Проверка результата
    if final_core_name and len(final_core_name) > 1 and not final_core_name.isdigit() and not re.fullmatch(r'[-.,]+', final_core_name):
        result = final_core_name.upper() if detected_form not in ['ИП', 'ФЛ', 'ИП/ФЛ'] else final_core_name
        logger.debug(f"Нормализация {repr(raw_name)} -> Возвращено: {repr(result)}")
        return result
    else:
        if not final_core_name: reason = "пустое"
        elif len(final_core_name) <= 1: reason = "слишком короткое"
        elif final_core_name.isdigit(): reason = "только цифры"
        elif re.fullmatch(r'[-.,]+', final_core_name): reason = "только пунктуация"
        else: reason = "неизвестно"
        logger.warning(f"Нормализация {repr(raw_name)}: ядро {repr(final_core_name)} невалидно ({reason}). Возвращаем None.")
        return None


def normalize_and_classify(raw_name: Optional[str], inn: Optional[str] = None) -> Tuple[Optional[str], str, str]:
    """Определяет ОПФ и нормализует имя."""
    original = raw_name.strip() if raw_name else ""
    if not original: return "?", "ФЛ", ""

    form = detect_final_legal_form(original, inn)
    core_name = normalize_name_core(original, form)

    if core_name is None or core_name == '?':
        if form not in ('ФЛ', 'ДРУГОЕ', 'ЮЛ', 'ИП', 'ИП/ФЛ'):
             core_name = form
             logger.debug(f"Ядро имени None/?, используем форму '{form}' как имя для {repr(original)}")
        else:
            if core_name is None:
                 logger.warning(f"Не удалось нормализовать имя для {repr(original)} (Форма: {form}), результат '?'")
            core_name = "?"

    logger.debug(f"Итог normalize_and_classify для {repr(original)}: Ядро={repr(core_name)}, Форма='{form}'")
    return core_name, form, original