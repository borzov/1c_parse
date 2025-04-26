#!filepath: src/config.py
# src/config.py
# -*- coding: utf-8 -*-
import os

# --- Режим отладки ---
# Если True, будут генерироваться отладочные файлы и выводиться подробные логи.
DEBUG_MODE = False

# --- Основные пути ---
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SRC_DIR)

DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'reports') # Отчеты и отладочные файлы сюда
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')

# --- Настройки файлов ---
FILE_ENCODING = 'cp1251'
DEFAULT_ORG_NAME_PREFIX = "Организация счета"

# Имена файлов отчетов
REPORT_1_FILENAME = 'counterparty_annual_payments.html'
REPORT_2_FILENAME = 'counterparty_organization_comparison.html'

# Имена отладочных файлов (в папке OUTPUT_DIR)
DEBUG_NAMES_FILENAME = 'debug_normalized_names.csv'
DEBUG_TRANSACTIONS_FILENAME = 'debug_processed_transactions.csv'

# Имена шаблонов
TEMPLATE_ANNUAL = 'report_annual_template.html'
TEMPLATE_COMPARISON = 'report_comparison_template.html'