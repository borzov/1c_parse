# -*- coding: utf-8 -*-
import re
from datetime import datetime
from collections import Counter
from typing import Optional, Counter as TypingCounter, Union, Dict

# --- Вспомогательные функции общего назначения ---

def safe_float(value_str: Union[str, int, float, None]) -> float:
    """Безопасное преобразование строки в float."""
    if not value_str: return 0.0
    try:
        cleaned_str = re.sub(r'\s+', '', str(value_str)).replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return 0.0

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Парсинг даты из разных форматов ДД.ММ.ГГГГ."""
    if not date_str or not isinstance(date_str, str): return None
    for fmt in ('%d.%m.%Y', '%d-%m-%Y', '%Y.%m.%d', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None

def format_currency(amount: Union[float, int, None]) -> str:
    """Форматирует число как валюту с неразрывным пробелом и запятой."""
    if amount is None: return "0,00"
    try:
        return f"{amount:,.2f}".replace(",", "\xa0").replace(".", ",")
    except (ValueError, TypeError):
        return "0,00"

def get_best_name(names_counter: Optional[TypingCounter[str]], default_prefix: str = "Организация счета") -> Optional[str]:
    """
    Выбирает наиболее вероятное 'правильное' имя из счетчика имен.
    Возвращает лучшее сырое имя или None.
    """
    if not names_counter: return None
    valid_names = {name: count for name, count in names_counter.items()
                   if name and isinstance(name, str) and default_prefix not in name and name != "?"}
    if not valid_names: return None
    sorted_names = sorted(valid_names.items(), key=lambda item: (-item[1], -len(item[0])))
    return sorted_names[0][0]

def get_doc_party_name(doc: Dict[str, str], party_type: str) -> str:
    """
    Извлекает имя Плательщика или Получателя из документа 1С,
    проверяя поля N и N1. Возвращает пустую строку, если не найдено.
    """
    base_field = party_type
    alt_field = base_field + "1"
    name = doc.get(base_field, '') or doc.get(alt_field, '')
    return name.strip() if name else ''