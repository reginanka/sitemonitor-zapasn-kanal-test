#!/usr/bin/env python3
"""Тиха звірка черг: оновлює базу так само, як ztoe_queue_monitor, без повідомлень людям.

Підсумок іде лише в лог-групу, гілка 155 «Перевірка черг» — той самий чат, що й звичайний крон.
Не в публічний канал і не користувачам.
Запуск: python ztoe_queue_sync_silent.py
"""
from __future__ import annotations

import os
import sys

os.environ["ZTOE_QUEUE_SILENT"] = "1"

import ztoe_queue_monitor


if __name__ == "__main__":
    sys.exit(ztoe_queue_monitor.main())
