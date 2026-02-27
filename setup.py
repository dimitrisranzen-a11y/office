#!/usr/bin/env python3
"""
ICEBERG OFFICE — Установка и запуск
=====================================
Запусти этот файл ОДИН РАЗ для установки всего нужного:
    python setup.py

После этого запускай сервер:
    python server.py
"""
import subprocess
import sys
import os

PACKAGES = [
    "flask",
    "flask-cors",
    "playwright",
    "requests",
    "beautifulsoup4",
    "openpyxl",
]

def run(cmd, check=True):
    print(f"\n> {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check)
    return result.returncode == 0

def main():
    print("\n" + "="*50)
    print("  ICEBERG OFFICE — Setup")
    print("="*50)

    # 1. Install Python packages
    print("\n📦 Установка Python пакетов...")
    for pkg in PACKAGES:
        run([sys.executable, "-m", "pip", "install", pkg, "--quiet"])

    # 2. Install Playwright browsers
    print("\n🎭 Установка Playwright Chromium...")
    run([sys.executable, "-m", "playwright", "install", "chromium"])

    print("\n" + "="*50)
    print("  ✅ Установка завершена!")
    print("="*50)
    print("\nТеперь запускай сервер командой:")
    print("    python server.py")
    print("\nЗатем открой dashboard.html в браузере.")
    print("\nЛогин: admin | Пароль: prague2024")
    input("\nНажми Enter для выхода...")

if __name__ == "__main__":
    main()
