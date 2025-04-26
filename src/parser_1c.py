# -*- coding: utf-8 -*-
import io
import re
import os
from typing import Union, Optional, List, Dict, Tuple # Добавлен импорт typing
from .config import FILE_ENCODING

# Используем старый синтаксис Union и Optional для Python 3.9
def parse_1c_file(filepath: str) -> Tuple[Optional[Dict], Optional[List[Dict]]]:
    """
    Парсит один файл формата 1CClientBankExchange.
    Возвращает кортеж: (header_info, documents) или (None, None) при ошибке.
    """
    try:
        with io.open(filepath, 'r', encoding=FILE_ENCODING, errors='replace') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Ошибка: Файл не найден {filepath}")
        return None, None
    except Exception as e:
        print(f"Ошибка чтения файла {filepath}: {e}")
        return None, None

    header_info, documents = {}, []
    current_doc, file_account = None, None
    in_doc = False
    file_encoding_declared = None

    if not lines or not lines[0].strip().startswith('1CClientBankExchange'):
        print(f"Предупреждение: Файл {filepath} не начинается с '1CClientBankExchange' или пуст.")

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line: continue

        if line.startswith('СекцияДокумент='):
            in_doc=True
            doc_type = line.split('=',1)[1].strip() if '=' in line else '?'
            current_doc={'ТипДокумента': doc_type, '_line': i+1} # Используем ТипДокумента
            continue
        elif line.startswith('КонецДокумента'):
            if current_doc: documents.append(current_doc)
            in_doc=False; current_doc=None; continue
        elif line.startswith('КонецФайла'): break

        m = re.match(r'([^=]+)=(.*)', line)
        if m:
            key, value = m.groups()[0].strip(), m.groups()[1].strip()
            if in_doc and current_doc is not None:
                current_doc[key] = value
            else:
                if key == 'Кодировка' and 'Кодировка' not in header_info:
                     header_info['Кодировка'] = value
                     file_encoding_declared = value
                     if value.lower() not in ['windows', 'cp1251'] and FILE_ENCODING.lower() == 'cp1251':
                          print(f"ПРЕДУПРЕЖДЕНИЕ ({os.path.basename(filepath)}): Кодировка файла ({value}) != используемой ({FILE_ENCODING}).")
                elif key == 'РасчСчет' and value:
                    file_account = value
                    header_info.setdefault('ОсновнойСчетФайла', file_account)
                elif key not in header_info:
                    header_info[key] = value

    header_info.setdefault('ОсновнойСчетФайла', file_account)

    if not header_info.get('ОсновнойСчетФайла'):
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось определить основной р/с для файла {filepath}. Файл пропущен.")
        return None, None

    main_acc = header_info['ОсновнойСчетФайла']
    for doc in documents:
        doc['СчетФайла'] = main_acc
        doc['_filepath'] = filepath

    return header_info, documents
