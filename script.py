#!/usr/bin/env python3
import mysql.connector
from mysql.connector import Error
import requests
import json
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

load_dotenv()

# Конфигурация
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
RECORDS_LIMIT = 2  # если нужно обновить все записи то поставить: None
API_DELAY = 1

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"Ошибка MySQL: {e}")
        return None

def get_content_records(connection, limit):
    try:
        cursor = connection.cursor(dictionary=True)
        query = """
            SELECT `id`, `title`, `fulltext`, `introtext`, `metakey`, `metadesc`, `state`
            FROM `cokw3_content`
            ORDER BY `id`
        """
        if limit:
            query += f" LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print(f"Ошибка чтения cokw3_content: {e}")
        return None

def clean_html_text(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text().strip()

def get_clean_content(record):
    content = record.get('fulltext', '')
    if not content or str(content).strip() == '':
        content = record.get('introtext', '')
    return clean_html_text(content)

def generate_meta_with_deepseek(title, content):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }

    prompt = (
        "Сгенерируй SEO-оптимизированные meta_keywords и meta_description для статьи, соблюдая правила:\n"
        "1. **meta_keywords**: 5-10 ключевых слов/фраз, релевантных содержанию, через запятую. "
        "Используй только значимые термины, избегая стоп-слов.\n"
        "2. **meta_description**: краткое описание до 160 символов с интригой или пользой для читателя. "
        "Должен включать главный ключевой запрос и призыв к действию.\n"
        "Исключай повторы призыва (например: Узнайте и Читайте в одном описании избыточно).\n"
        "3. **Тон**: формальный, но понятный.\n\n"
        "Исходные данные:\n"
        f"- Заголовок: {title}\n"
        f"- Контент: {content}\n\n"
        "Ответ строго в JSON-формате без пояснений, например:\n"
        '{"meta_keywords": "ключевые, слова", "meta_description": "описание"}\n'
        "Не добавляй другие поля или текст."
    )

    data = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 200,
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=data, timeout=15)
        response.raise_for_status()
        result = response.json()

        if "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            content = content.replace('```json', '').replace('```', '').strip()
            return json.loads(content)

    except Exception as e:
        print(f"Ошибка API: {str(e)}")
    return None

def update_content_meta(connection, record_id, meta_keywords, meta_description):
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE `cokw3_content` SET `metakey` = %s, `metadesc` = %s WHERE `id` = %s",
            (meta_keywords, meta_description, record_id)
        )
        connection.commit()
        return True
    except Error as e:
        print(f"Ошибка UPDATE для записи {record_id}: {e}")
        return False

def process_records(db_conn, records):
    success_count = 0
    skipped_empty = 0
    skipped_api_errors = 0
    skipped_db_errors = 0
    skipped_state = 0

    for i, record in enumerate(records, 1):
        print(f"\n--- Запись {i}/{len(records)} [ID: {record['id']}] ---")
        print(f"Заголовок: {record['title'][:50]}{'...' if len(record['title']) > 50 else ''}")

        if record.get('state', 1) == 0:
            print("Пропуск: запись не опубликована (state = 0)")
            skipped_state += 1
            continue

        clean_content = get_clean_content(record)

        if not clean_content or clean_content.strip() == '':
            print("Пропуск: нет текста для обработки (ни в fulltext, ни в introtext)")
            skipped_empty += 1
            continue

        print("Генерация meta-данных...")
        meta_data = generate_meta_with_deepseek(record["title"], clean_content)

        if not meta_data:
            print("Пропуск: ошибка генерации meta-данных")
            skipped_api_errors += 1
            continue

        print("Сгенерированные мета-данные:")
        print(f"Keywords: {meta_data['meta_keywords']}")
        print(f"Description: {meta_data['meta_description']}")

        if update_content_meta(
            db_conn,
            record["id"],
            meta_data['meta_keywords'],
            meta_data['meta_description']
        ):
            print("Успешно обновлено в БД")
            success_count += 1
        else:
            skipped_db_errors += 1

        if i < len(records):
            time.sleep(API_DELAY)

    return {
        'success': success_count,
        'skipped_empty': skipped_empty,
        'skipped_api_errors': skipped_api_errors,
        'skipped_db_errors': skipped_db_errors,
        'skipped_state': skipped_state
    }

def main():
    print(f"\n=== Генерация meta-данных (лимит: {RECORDS_LIMIT or 'все'}) ===")

    db_conn = get_db_connection()
    if not db_conn:
        return

    try:
        records = get_content_records(db_conn, RECORDS_LIMIT)
        if not records:
            print("Нет записей для обработки!")
            return

        total = len(records)
        print(f"Найдено записей: {total}\n")

        result = process_records(db_conn, records)

        print("\n=== Результаты обработки ===")
        print(f"Успешно обработано: {result['success']}")
        print(f"Пропущено (пустой текст): {result['skipped_empty']}")
        print(f"Пропущено (state = 0): {result['skipped_state']}")
        print(f"Пропущено (ошибки API): {result['skipped_api_errors']}")
        print(f"Пропущено (ошибки БД): {result['skipped_db_errors']}")
        print(f"Всего записей: {total}")
        
    finally:
        db_conn.close()
        print("\nРабота завершена")

if __name__ == "__main__":
    main()