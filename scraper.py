"""
ICEBERG OFFICE — Firmy.cz Scraper
===================================
Собирает ВСЕ фирмы с Firmy.cz по выбранной категории и городу.
Проходит через все страницы пагинации до конца.
Для каждой фирмы собирает: название, адрес, телефон, сайт, email, описание.

Запуск напрямую (тест):
    python scraper.py

Используется из server.py автоматически.
"""

import re
import time
import random
import sqlite3
import datetime
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, quote

DB_FILE = os.path.join(os.path.dirname(__file__), "iceberg.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Список городов Чехии для Firmy.cz
CITIES = {
    "Praha":        "Praha",
    "Brno":         "Brno",
    "Ostrava":      "Ostrava",
    "Plzeň":        "Plzen",
    "Liberec":      "Liberec",
    "Olomouc":      "Olomouc",
    "České Budějovice": "Ceske-Budejovice",
    "Hradec Králové": "Hradec-Kralove",
    "Pardubice":    "Pardubice",
    "Zlín":         "Zlin",
    "Kladno":       "Kladno",
    "Most":         "Most",
    "Opava":        "Opava",
    "Frýdek-Místek": "Frydek-Mistek",
    "Karviná":      "Karvina",
    "Jihlava":      "Jihlava",
    "Teplice":      "Teplice",
    "Ústí nad Labem": "Usti-nad-Labem",
    "Chomutov":     "Chomutov",
    "Děčín":        "Decin",
}

# Категории Firmy.cz (slug → отображаемое имя)
CATEGORIES = {
    "restaurace-bary-a-kavárny": "Рестораны, бары, кафе",
    "it-a-telekomunikace":       "IT и телекоммуникации",
    "stavebnictvi":              "Строительство",
    "reality":                   "Недвижимость",
    "finance-a-pojistovnictvi":  "Финансы и страхование",
    "pravo-a-notari":            "Юридические услуги",
    "zdravotnictvi-a-farmace":   "Медицина и фармация",
    "doprava-a-logistika":       "Транспорт и логистика",
    "ubytovani-a-cestovni-ruch": "Гостиницы и туризм",
    "sport-a-volny-cas":         "Спорт и досуг",
    "krasa-a-zdravi":            "Красота и здоровье",
    "vzdělávání":                "Образование",
    "marketing-a-reklama":       "Маркетинг и реклама",
    "ucetnictvi-a-ekonomika":    "Бухгалтерия",
    "prumysl-a-vyroba":          "Промышленность",
    "auto-moto":                 "Авто и мото",
    "e-commerce-a-obchod":       "E-commerce и торговля",
    "energetika":                "Энергетика",
    "zemedelstvi":               "Сельское хозяйство",
    "media-a-komunikace":        "СМИ и коммуникации",
}


# ══════════════════════════════════════════
# БАЗА ДАННЫХ SQLite
# ══════════════════════════════════════════

def init_db():
    """Создаёт базу данных и таблицы если их нет."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            category    TEXT,
            city        TEXT,
            address     TEXT,
            phone       TEXT,
            email       TEXT,
            website     TEXT,
            description TEXT,
            source_url  TEXT,
            status      TEXT DEFAULT 'new',
            quality     INTEGER DEFAULT 0,
            date_added  TEXT,
            note        TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            dt          TEXT,
            category    TEXT,
            city        TEXT,
            total_found INTEGER,
            total_saved INTEGER,
            emails_found INTEGER,
            phones_found INTEGER
        )
    """)
    # Индексы для быстрого поиска
    c.execute("CREATE INDEX IF NOT EXISTS idx_name ON contacts(name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cat  ON contacts(category)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_city ON contacts(city)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON contacts(status)")
    conn.commit()
    conn.close()

def get_existing_names(city=None, category=None):
    """Возвращает set имён уже сохранённых фирм."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    if city and category:
        c.execute("SELECT LOWER(name) FROM contacts WHERE city=? AND category=?", (city, category))
    elif city:
        c.execute("SELECT LOWER(name) FROM contacts WHERE city=?", (city,))
    else:
        c.execute("SELECT LOWER(name) FROM contacts")
    names = {row[0] for row in c.fetchall()}
    conn.close()
    return names

def save_contacts(contacts, city, category):
    """Сохраняет список контактов в SQLite. Возвращает количество новых записей."""
    if not contacts:
        return 0
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    today = datetime.date.today().strftime("%d.%m.%Y")
    saved = 0
    for co in contacts:
        # Проверяем дубликат по имени + город
        c.execute("SELECT id FROM contacts WHERE LOWER(name)=? AND city=?",
                  (co['name'].lower().strip(), city))
        if c.fetchone():
            continue
        quality = calc_quality(co)
        c.execute("""
            INSERT INTO contacts (name, category, city, address, phone, email,
                                  website, description, source_url, status, quality, date_added)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            co.get('name',''), category, city,
            co.get('address',''), co.get('phone',''),
            co.get('email',''), co.get('website',''),
            co.get('description',''), co.get('source_url',''),
            'new', quality, today
        ))
        saved += 1
    conn.commit()
    conn.close()
    return saved

def save_session(dt, category, city, total_found, total_saved, emails, phones):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO sessions (dt, category, city, total_found, total_saved, emails_found, phones_found)
        VALUES (?,?,?,?,?,?,?)
    """, (dt, category, city, total_found, total_saved, emails, phones))
    conn.commit()
    conn.close()

def get_all_contacts(city=None, category=None, status=None, search=None,
                     sort='date_added', limit=2000, offset=0):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    where, params = [], []
    if city:       where.append("city=?");     params.append(city)
    if category:   where.append("category=?"); params.append(category)
    if status:     where.append("status=?");   params.append(status)
    if search:
        where.append("(LOWER(name) LIKE ? OR LOWER(email) LIKE ? OR phone LIKE ?)")
        s = f"%{search.lower()}%"
        params.extend([s, s, s])
    sql = "SELECT * FROM contacts"
    if where: sql += " WHERE " + " AND ".join(where)
    order_map = {'date': 'id DESC', 'name': 'name ASC',
                 'quality': 'quality DESC', 'status': 'status ASC'}
    sql += f" ORDER BY {order_map.get(sort,'id DESC')}"
    sql += f" LIMIT {limit} OFFSET {offset}"
    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def get_stats():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    stats = {}
    c.execute("SELECT COUNT(*) FROM contacts"); stats['total'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email!=''"); stats['with_email'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL AND phone!=''"); stats['with_phone'] = c.fetchone()[0]
    c.execute("SELECT AVG(quality) FROM contacts"); avg = c.fetchone()[0]; stats['avg_quality'] = round(avg or 0)
    c.execute("SELECT COUNT(*) FROM sessions"); stats['sessions'] = c.fetchone()[0]
    c.execute("SELECT status, COUNT(*) FROM contacts GROUP BY status")
    stats['by_status'] = {row[0]: row[1] for row in c.fetchall()}
    c.execute("SELECT city, COUNT(*) FROM contacts GROUP BY city ORDER BY COUNT(*) DESC LIMIT 20")
    stats['by_city'] = {row[0]: row[1] for row in c.fetchall()}
    c.execute("SELECT category, COUNT(*) FROM contacts GROUP BY category ORDER BY COUNT(*) DESC LIMIT 20")
    stats['by_category'] = {row[0]: row[1] for row in c.fetchall()}
    c.execute("SELECT * FROM sessions ORDER BY id DESC LIMIT 20")
    conn.row_factory = sqlite3.Row
    c2 = conn.cursor()
    c2.execute("SELECT * FROM sessions ORDER BY id DESC LIMIT 20")
    stats['sessions_list'] = [dict(r) for r in c2.fetchall()]
    conn.close()
    return stats

def update_contact(contact_id, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    fields = ['name','category','city','address','phone','email','website','description','status','note']
    sets = [f"{f}=?" for f in fields if f in data]
    vals = [data[f] for f in fields if f in data]
    if sets:
        quality = calc_quality(data)
        sets.append("quality=?"); vals.append(quality)
        vals.append(contact_id)
        c.execute(f"UPDATE contacts SET {', '.join(sets)} WHERE id=?", vals)
    conn.commit(); conn.close()

def delete_contact(contact_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
    conn.commit(); conn.close()

def clear_all():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM contacts")
    c.execute("DELETE FROM sessions")
    conn.commit(); conn.close()

def calc_quality(c):
    s = 0
    if c.get('name'):        s += 20
    if c.get('email'):       s += 35
    if c.get('phone'):       s += 20
    if c.get('website'):     s += 15
    if c.get('address'):     s += 5
    if c.get('description'): s += 5
    return s


# ══════════════════════════════════════════
# SCRAPER
# ══════════════════════════════════════════

def extract_email(text):
    if not text: return None
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    skip = ["example", "domain", "test", "noreply", "no-reply", "sentry",
            "wix", "wordpress", "google", "schema", "email", "info@firmy",
            "placeholder", "user@", "@example"]
    for e in emails:
        if not any(x in e.lower() for x in skip):
            return e
    return None

def get_email_from_website(url, timeout=8):
    """Заходит на сайт компании и ищет email адрес."""
    if not url or not url.startswith("http"):
        return None
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        email = extract_email(r.text)
        if email:
            return email
        # Ищем страницу контактов
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = str(a.get("href", ""))
            text = a.get_text(strip=True).lower()
            if any(w in text or w in href.lower()
                   for w in ["kontakt", "contact", "o-nas", "o-firme"]):
                contact_url = urljoin(url, href)
                if contact_url != url:
                    try:
                        cr = SESSION.get(contact_url, timeout=6)
                        e = extract_email(cr.text)
                        if e:
                            return e
                    except Exception:
                        pass
    except Exception:
        pass
    return None

def scrape_listing_page(url, log_fn=None):
    """Парсит одну страницу листинга Firmy.cz. Возвращает список фирм."""
    results = []
    try:
        time.sleep(random.uniform(1.2, 2.8))
        r = SESSION.get(url, timeout=15)
        if r.status_code == 429:
            if log_fn: log_fn("⚠ Rate limit — пауза 15 сек...", "err")
            time.sleep(15)
            r = SESSION.get(url, timeout=15)
        if r.status_code != 200:
            if log_fn: log_fn(f"HTTP {r.status_code} на {url}", "err")
            return results, None

        soup = BeautifulSoup(r.text, "html.parser")

        # Найти все карточки фирм
        cards = soup.select([
            "div.companyTitle",
            "div[class*='company-title']",
            "article.companyBox",
            "li.item-firm",
            "div.item",
            "li[class*='item']",
        ][0]) if soup.select("div.companyTitle") else None

        # Попробуем разные селекторы
        selectors = [
            "div.companyTitle",
            "article[class*='company']",
            "li[class*='firmItem']",
            "div[class*='firm-item']",
            "li.item",
        ]
        cards = []
        for sel in selectors:
            found = soup.select(sel)
            if found:
                cards = found
                break

        # Fallback — ищем по ссылкам на компании
        if not cards:
            cards = soup.select("h2.company-name, h3.company-name, a[href*='/firma/']")

        if log_fn: log_fn(f"  Карточек на странице: {len(cards)}", "info")

        for card in cards:
            try:
                # Название
                name_el = card.select_one("h2 a, h3 a, a.name, a[class*='name'], a[href*='/firma/']")
                if not name_el:
                    name_el = card if card.name == 'a' else None
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 2:
                    continue

                # Ссылка на карточку фирмы
                source_url = ""
                href = name_el.get("href", "")
                if href:
                    source_url = urljoin("https://www.firmy.cz", href)

                # Адрес
                addr_el = card.select_one("span.address, p.address, div[class*='address'], span[class*='address']")
                address = addr_el.get_text(strip=True) if addr_el else ""

                # Телефон
                phone_text = card.get_text()
                phone = re.search(r"(\+420[\s\-]?)?[0-9]{3}[\s\-]?[0-9]{3}[\s\-]?[0-9]{3}", phone_text)
                phone = phone.group(0).strip() if phone else ""

                # Сайт (ссылка на внешний сайт, не firmy.cz)
                website = ""
                for a in card.select("a[href]"):
                    h = a.get("href", "")
                    if h.startswith("http") and "firmy.cz" not in h and "seznam.cz" not in h:
                        website = h
                        break

                # Описание
                desc_el = card.select_one("p.description, div[class*='description'], span[class*='description'], p[class*='perex']")
                description = desc_el.get_text(strip=True)[:300] if desc_el else ""

                # Email — сначала из карточки
                email = extract_email(card.get_text())

                results.append({
                    "name": name,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "email": email,
                    "description": description,
                    "source_url": source_url,
                })
            except Exception:
                continue

        # Найти ссылку на следующую страницу
        next_page = None
        next_el = soup.select_one("a[rel='next'], a.next, a[class*='next'], li.next a, a[aria-label='Další']")
        if next_el:
            next_href = next_el.get("href", "")
            if next_href:
                next_page = urljoin("https://www.firmy.cz", next_href)

        # Также ищем по тексту ссылки
        if not next_page:
            for a in soup.select("a[href]"):
                text = a.get_text(strip=True).lower()
                href = a.get("href", "")
                if text in ["další", "next", "»", "›", "dalsi"] and href:
                    next_page = urljoin("https://www.firmy.cz", href)
                    break

        return results, next_page

    except Exception as e:
        if log_fn: log_fn(f"  Ошибка страницы: {e}", "err")
        return results, None


def scrape_firmy_cz(category_slug, city_slug, log_fn=None, stop_flag=None, max_pages=999):
    """
    Главная функция — собирает ВСЕ фирмы по категории и городу.
    Проходит все страницы пагинации.
    
    category_slug: например 'restaurace-bary-a-kavarny'
    city_slug: например 'Praha' или 'Brno'
    log_fn: callback для логирования (type, message)
    stop_flag: callable() -> bool, если True — остановить
    """
    def log(msg, t="info"):
        if log_fn: log_fn(msg, t)
        else: print(msg)

    all_results = []

    # Строим начальный URL
    # Firmy.cz URL формат: /category/location=City
    base_url = f"https://www.firmy.cz/{category_slug}?location={city_slug}"
    log(f"🔍 Начало сбора: {base_url}", "info")

    current_url = base_url
    page_num = 0
    total_cards = 0

    while current_url and page_num < max_pages:
        if stop_flag and stop_flag():
            log("⛔ Сбор остановлен", "err")
            break

        page_num += 1
        log(f"📄 Страница {page_num}: {current_url}", "info")

        page_results, next_url = scrape_listing_page(current_url, log_fn=log)

        if not page_results:
            log(f"  Страница пуста — конец пагинации", "info")
            break

        # Для каждой фирмы — дополнительный сбор email с сайта
        for i, firm in enumerate(page_results):
            if stop_flag and stop_flag():
                break
            if not firm.get("email") and firm.get("website"):
                log(f"  [{i+1}/{len(page_results)}] {firm['name'][:40]} → email поиск...", "info")
                email = get_email_from_website(firm["website"])
                if email:
                    firm["email"] = email
                    log(f"    ✉ Найден: {email}", "ok")
                time.sleep(random.uniform(0.3, 1.0))
            else:
                log(f"  [{i+1}/{len(page_results)}] ✓ {firm['name'][:50]}", "ok")

        all_results.extend(page_results)
        total_cards += len(page_results)
        log(f"  ✓ Собрано на этой странице: {len(page_results)} | Всего: {total_cards}", "ok")

        current_url = next_url
        if not next_url:
            log("  Следующая страница не найдена — сбор завершён", "info")
            break

        # Пауза между страницами
        time.sleep(random.uniform(2.0, 4.5))

    log(f"\n📊 Итого собрано: {total_cards} фирм", "ok")
    return all_results


# ══════════════════════════════════════════
# ТЕСТ ЗАПУСКА
# ══════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    print("База данных инициализирована:", DB_FILE)
    print("\nДоступные категории:")
    for slug, name in CATEGORIES.items():
        print(f"  {slug}: {name}")
    print("\nДоступные города:")
    for city in CITIES.keys():
        print(f"  {city}")
    print("\nЗапусти server.py и открой dashboard.html для работы.")
