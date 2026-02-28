"""
ICEBERG OFFICE — Firmy.cz Scraper
===================================
Правильные URL:
  Praha рестораны:  /Restauracni-a-pohostinske-sluzby/kraj-praha
  Praha кофейни:    /Restauracni-a-pohostinske-sluzby/Kavarny/kraj-praha
  Brno  кофейни:    /Restauracni-a-pohostinske-sluzby/Kavarny/kraj-jihomoravsky/brno-mesto/5740-brno
  Пагинация:        ?page=2
"""

import re, time, random, sqlite3, datetime, os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iceberg.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
    "Referer": "https://www.firmy.cz/",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ══════════════════════════════════════════
# КАТЕГОРИИ — точные Czech URL пути с Firmy.cz
# ══════════════════════════════════════════
# Ключ = URL path (регистр важен!), значение = отображаемое название

CATEGORIES = {
    "Restauracni-a-pohostinske-sluzby":                 "Рестораны (все)",
    "Restauracni-a-pohostinske-sluzby/Kavarny":         "Кофейни",
    "Restauracni-a-pohostinske-sluzby/Bary-a-puby":     "Бары и пабы",
    "Restauracni-a-pohostinske-sluzby/Fastfoody":       "Фастфуд",
    "Restauracni-a-pohostinske-sluzby/Pizzerie":        "Пиццерии",
    "Informacni-technologie":                           "IT и технологии (все)",
    "Informacni-technologie/Vyvoj-software":            "Разработка ПО",
    "Informacni-technologie/Webdesign-a-tvorba-webu":   "Веб-дизайн",
    "Informacni-technologie/Pocitacove-site":           "Компьютерные сети",
    "Stavebnictvi-a-reality":                           "Строительство и недвижимость",
    "Stavebnictvi-a-reality/Stavebni-prace":            "Строительные работы",
    "Stavebnictvi-a-reality/Reality":                   "Недвижимость / Риелторы",
    "Stavebnictvi-a-reality/Architekti-a-projektanti":  "Архитекторы",
    "Finance-a-pojistovnictvi":                         "Финансы и страхование",
    "Finance-a-pojistovnictvi/Ucetnictvi-a-dane":       "Бухгалтерия и налоги",
    "Finance-a-pojistovnictvi/Financni-poradenstvi":    "Финансовые консультанты",
    "Pravni-sluzby":                                    "Юридические услуги",
    "Zdravotnictvi-a-farmacie":                         "Медицина (все)",
    "Zdravotnictvi-a-farmacie/Prakticky-lekar":         "Терапевты",
    "Zdravotnictvi-a-farmacie/Stomatologie":            "Стоматологи",
    "Zdravotnictvi-a-farmacie/Lekarny":                 "Аптеки",
    "Doprava-a-logistika":                              "Транспорт и логистика",
    "Doprava-a-logistika/Stehovani":                    "Грузоперевозки / Переезды",
    "Ubytovani-a-cestovani":                            "Гостиницы и туризм (все)",
    "Ubytovani-a-cestovani/Hotely":                     "Отели",
    "Ubytovani-a-cestovani/Penziony-a-apartmany":       "Пансионаты и апартаменты",
    "Sport-a-volny-cas":                                "Спорт и досуг (все)",
    "Sport-a-volny-cas/Fitness-a-posilovny":            "Фитнес-центры",
    "Sport-a-volny-cas/Sportovni-kluby":                "Спортивные клубы",
    "Krasa-a-zdravi":                                   "Красота и здоровье (все)",
    "Krasa-a-zdravi/Kadernictvi":                       "Парикмахерские",
    "Krasa-a-zdravi/Kosmetika-a-vizaz":                 "Косметика и визаж",
    "Krasa-a-zdravi/Masaze":                            "Массаж",
    "Vzdelavani-a-vyuka":                               "Образование",
    "Vzdelavani-a-vyuka/Jazykove-skoly":                "Языковые школы",
    "Marketing-reklama-a-PR":                           "Маркетинг и реклама",
    "Auto-moto":                                        "Авто и мото (все)",
    "Auto-moto/Autoservisy":                            "Автосервисы",
    "Auto-moto/Prodej-aut":                             "Продажа авто",
    "Prumysl-a-vyroba":                                 "Промышленность и производство",
    "Obchod-a-sluzby":                                  "Торговля и услуги",
    "Media-a-komunikace":                               "СМИ и коммуникации",
    "Energetika-a-zivotni-prostredi":                   "Энергетика и экология",
}

# ══════════════════════════════════════════
# ГОРОДА — правильные kraj и city path
# ══════════════════════════════════════════
# Структура URL: /Category/kraj-{kraj}[/{city_path}]
# Praha особый случай — kraj-praha без city_path

CITIES = {
    "Praha": {
        "kraj": "kraj-praha",
        "city_path": "",          # Прага: только kraj, без города
    },
    "Brno": {
        "kraj": "kraj-jihomoravsky",
        "city_path": "brno-mesto/5740-brno",
    },
    "Ostrava": {
        "kraj": "kraj-moravskoslezsky",
        "city_path": "ostrava-mesto/554821-ostrava",
    },
    "Plzeň": {
        "kraj": "kraj-plzensky",
        "city_path": "plzen-mesto/554791-plzen",
    },
    "Liberec": {
        "kraj": "kraj-liberecky",
        "city_path": "liberec/563510-liberec",
    },
    "Olomouc": {
        "kraj": "kraj-olomoucky",
        "city_path": "olomouc/500496-olomouc",
    },
    "České Budějovice": {
        "kraj": "kraj-jihocesky",
        "city_path": "ceske-budejovice/544256-ceske-budejovice",
    },
    "Hradec Králové": {
        "kraj": "kraj-kralovehradecky",
        "city_path": "hradec-kralove/569810-hradec-kralove",
    },
    "Pardubice": {
        "kraj": "kraj-pardubicky",
        "city_path": "pardubice/555134-pardubice",
    },
    "Zlín": {
        "kraj": "kraj-zlinsky",
        "city_path": "zlin/585068-zlin",
    },
    "Ústí nad Labem": {
        "kraj": "kraj-ustecky",
        "city_path": "usti-nad-labem/554804-usti-nad-labem",
    },
    "Kladno": {
        "kraj": "kraj-stredocesky",
        "city_path": "kladno/532053-kladno",
    },
    "Opava": {
        "kraj": "kraj-moravskoslezsky",
        "city_path": "opava/507440-opava",
    },
    "Teplice": {
        "kraj": "kraj-ustecky",
        "city_path": "teplice/567442-teplice",
    },
    "Most": {
        "kraj": "kraj-ustecky",
        "city_path": "most/566683-most",
    },
}


def build_url(category_slug, city_key, page=1):
    """
    Строит правильный URL Firmy.cz.

    Praha, рестораны:   /Restauracni-a-pohostinske-sluzby/kraj-praha
    Praha, кофейни:     /Restauracni-a-pohostinske-sluzby/Kavarny/kraj-praha
    Brno,  кофейни, p2: /Restauracni-a-pohostinske-sluzby/Kavarny/kraj-jihomoravsky/brno-mesto/5740-brno?page=2
    """
    city = CITIES.get(city_key, CITIES["Praha"])
    kraj = city["kraj"]
    city_path = city.get("city_path", "")

    if city_path:
        base = f"https://www.firmy.cz/{category_slug}/{kraj}/{city_path}"
    else:
        base = f"https://www.firmy.cz/{category_slug}/{kraj}"

    if page > 1:
        return f"{base}?page={page}"
    return base


# ══════════════════════════════════════════
# БАЗА ДАННЫХ SQLite
# ══════════════════════════════════════════

def init_db():
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
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            dt           TEXT,
            category     TEXT,
            city         TEXT,
            total_found  INTEGER,
            total_saved  INTEGER,
            emails_found INTEGER,
            phones_found INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_name   ON contacts(name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cat    ON contacts(category)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_city   ON contacts(city)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON contacts(status)")
    conn.commit()
    conn.close()


def save_contacts(contacts, city, category):
    if not contacts:
        return 0
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    today = datetime.date.today().strftime("%d.%m.%Y")
    saved = 0
    for co in contacts:
        c.execute("SELECT id FROM contacts WHERE LOWER(name)=? AND city=?",
                  (co["name"].lower().strip(), city))
        if c.fetchone():
            continue
        q = calc_quality(co)
        c.execute("""
            INSERT INTO contacts
              (name,category,city,address,phone,email,website,description,source_url,status,quality,date_added)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (co.get("name",""), category, city,
              co.get("address",""), co.get("phone",""),
              co.get("email",""), co.get("website",""),
              co.get("description",""), co.get("source_url",""),
              "new", q, today))
        saved += 1
    conn.commit()
    conn.close()
    return saved


def save_session(dt, category, city, total_found, total_saved, emails, phones):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO sessions (dt,category,city,total_found,total_saved,emails_found,phones_found)
        VALUES (?,?,?,?,?,?,?)
    """, (dt, category, city, total_found, total_saved, emails, phones))
    conn.commit()
    conn.close()


def get_all_contacts(city=None, category=None, status=None, search=None,
                     sort="date", limit=5000, offset=0):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    where, params = [], []
    if city:     where.append("city=?");     params.append(city)
    if category: where.append("category=?"); params.append(category)
    if status:   where.append("status=?");   params.append(status)
    if search:
        where.append("(LOWER(name) LIKE ? OR LOWER(email) LIKE ? OR phone LIKE ? OR website LIKE ?)")
        s = f"%{search.lower()}%"
        params.extend([s, s, s, s])
    sql = "SELECT * FROM contacts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    order_map = {"date": "id DESC", "name": "name ASC",
                 "quality": "quality DESC", "status": "status ASC"}
    sql += f" ORDER BY {order_map.get(sort,'id DESC')}"
    sql += f" LIMIT {limit} OFFSET {offset}"
    c.execute(sql, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_stats():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    stats = {}
    c.execute("SELECT COUNT(*) FROM contacts");                                         stats["total"]        = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contacts WHERE email   IS NOT NULL AND email!=''"); stats["with_email"]   = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contacts WHERE phone   IS NOT NULL AND phone!=''"); stats["with_phone"]   = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM contacts WHERE website IS NOT NULL AND website!=''");stats["with_website"] = c.fetchone()[0]
    c.execute("SELECT AVG(quality) FROM contacts"); avg = c.fetchone()[0];              stats["avg_quality"]  = round(avg or 0)
    c.execute("SELECT COUNT(*) FROM sessions");                                         stats["sessions"]     = c.fetchone()[0]
    c.execute("SELECT status, COUNT(*) FROM contacts GROUP BY status")
    stats["by_status"] = {r[0]: r[1] for r in c.fetchall()}
    c.execute("SELECT city, COUNT(*) FROM contacts GROUP BY city ORDER BY COUNT(*) DESC LIMIT 20")
    stats["by_city"] = {r[0]: r[1] for r in c.fetchall()}
    c.execute("SELECT category, COUNT(*) FROM contacts GROUP BY category ORDER BY COUNT(*) DESC LIMIT 20")
    stats["by_category"] = {r[0]: r[1] for r in c.fetchall()}
    c.execute("SELECT * FROM sessions ORDER BY id DESC LIMIT 30")
    stats["sessions_list"] = [dict(r) for r in c.fetchall()]
    conn.close()
    return stats


def update_contact(contact_id, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    fields = ["name","category","city","address","phone","email","website","description","status","note"]
    sets = [f"{f}=?" for f in fields if f in data]
    vals = [data[f] for f in fields if f in data]
    if sets:
        sets.append("quality=?"); vals.append(calc_quality(data))
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
    c.execute("DELETE FROM contacts"); c.execute("DELETE FROM sessions")
    conn.commit(); conn.close()


def calc_quality(c):
    s = 0
    if c.get("name"):        s += 20
    if c.get("email"):       s += 35
    if c.get("phone"):       s += 20
    if c.get("website"):     s += 15
    if c.get("address"):     s += 5
    if c.get("description"): s += 5
    return s


# ══════════════════════════════════════════
# ПАРСЕР HTML FIRMY.CZ
# ══════════════════════════════════════════

def extract_email(text):
    if not text: return None
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    skip = ["example","domain","test","noreply","no-reply","sentry","wix",
            "wordpress","google","schema","firmy.cz","seznam.cz","placeholder","email@"]
    for e in emails:
        if not any(x in e.lower() for x in skip):
            return e
    return None


def get_email_from_website(url, timeout=8):
    if not url or not url.startswith("http"):
        return None
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        email = extract_email(r.text)
        if email:
            return email
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = str(a.get("href", ""))
            text = a.get_text(strip=True).lower()
            if any(w in text or w in href.lower()
                   for w in ["kontakt", "contact", "o-nas", "o-firme"]):
                contact_url = urljoin(url, href)
                if contact_url == url: continue
                try:
                    cr = SESSION.get(contact_url, timeout=6)
                    e = extract_email(cr.text)
                    if e: return e
                except Exception:
                    pass
    except Exception:
        pass
    return None


def parse_firmy_page(html, page_url):
    """
    Парсит страницу листинга Firmy.cz.
    Возвращает (list_of_firms, next_page_url_or_None).
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # ── Найти карточки фирм ──────────────────────────────────────────
    # Firmy.cz рендерит фирмы в <div class="companyWrapper"> или похожих
    # Пробуем несколько вариантов от самого специфичного к общему

    cards = []
    selectors = [
        "div.companyWrapper",
        "li.companyItem",
        "div[class*='companyBox']",
        "article[class*='company']",
        "li[class*='companyItem']",
        "div[class*='company-item']",
    ]
    for sel in selectors:
        found = soup.select(sel)
        if len(found) > 2:
            cards = found
            break

    # Fallback: ищем по ссылкам /firma/ (каждая фирма имеет такую ссылку)
    if not cards:
        # Группируем по ближайшему контейнеру li или article
        seen_parents = set()
        for a in soup.select("a[href*='/firma/']"):
            parent = a.find_parent("li") or a.find_parent("article") or a.find_parent("div")
            if parent is None:
                continue
            pid = id(parent)
            if pid in seen_parents:
                continue
            seen_parents.add(pid)
            # Проверяем что это не ссылка в меню — у фирм есть заголовок h2/h3
            if parent.find(["h2","h3"]):
                cards.append(parent)

    # ── Извлечь данные из каждой карточки ───────────────────────────
    for card in cards:
        try:
            # Название + ссылка на карточку
            name_el = (
                card.select_one("h2 a[href*='/firma/']") or
                card.select_one("h3 a[href*='/firma/']") or
                card.select_one("a[class*='companyName']") or
                card.select_one("a[class*='title']") or
                card.select_one("h2 a") or
                card.select_one("h3 a")
            )
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue

            href = name_el.get("href","")
            source_url = urljoin("https://www.firmy.cz", href) if href else ""

            full_text = card.get_text(" ", strip=True)

            # Адрес
            address = ""
            addr_el = card.select_one(
                "span[class*='address'], p[class*='address'], "
                "div[class*='address'], span[itemprop='streetAddress'], "
                "address, span[class*='locality'], div[class*='locality']"
            )
            if addr_el:
                address = addr_el.get_text(strip=True)

            # Телефон — ищем в тексте и в data-атрибутах
            phone = ""
            phone_el = card.select_one("a[href^='tel:'], span[class*='phone'], div[class*='phone']")
            if phone_el:
                href_tel = phone_el.get("href","")
                if href_tel.startswith("tel:"):
                    phone = href_tel.replace("tel:","").strip()
                else:
                    phone = phone_el.get_text(strip=True)
            if not phone:
                m = re.search(r"(\+420[\s\-]?)?[2-9][0-9]{2}[\s\-]?[0-9]{3}[\s\-]?[0-9]{3}", full_text)
                if m:
                    phone = m.group(0).strip()

            # Сайт — внешняя ссылка (не firmy.cz, seznam.cz, mapy.cz)
            website = ""
            for a in card.select("a[href^='http']"):
                h = a.get("href","")
                if not any(x in h for x in ["firmy.cz","seznam.cz","mapy.cz","google.","facebook."]):
                    website = h
                    break

            # Email из карточки
            email = extract_email(full_text)

            # Описание
            description = ""
            desc_el = card.select_one(
                "p[class*='description'], div[class*='description'], "
                "p[class*='perex'], span[class*='annotation'], p[class*='about']"
            )
            if desc_el:
                description = desc_el.get_text(strip=True)[:300]

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

    # ── Следующая страница ───────────────────────────────────────────
    next_url = None

    # rel="next"
    next_el = soup.select_one("a[rel='next']")
    if next_el and next_el.get("href"):
        next_url = urljoin("https://www.firmy.cz", next_el["href"])

    # Ссылка с текстом "Další"
    if not next_url:
        for a in soup.select("a[href]"):
            if a.get_text(strip=True).lower().rstrip(" »›") in ["další","next","dalsi"]:
                href = a.get("href","")
                if href:
                    next_url = urljoin("https://www.firmy.cz", href)
                    break

    # По номеру страницы в пагинаторе
    if not next_url:
        page_match = re.search(r"[?&]page=(\d+)", page_url)
        cur_page = int(page_match.group(1)) if page_match else 1
        for a in soup.select("a[href]"):
            if f"page={cur_page+1}" in a.get("href",""):
                next_url = urljoin("https://www.firmy.cz", a["href"])
                break

    return results, next_url


# ══════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════

def scrape_firmy_cz(category_slug, city_key, log_fn=None, stop_flag=None):
    """
    Собирает ВСЕ фирмы по категории и городу, проходя все страницы.
    """
    def log(msg, t="info"):
        if log_fn: log_fn(msg, t)
        else: print(msg)

    all_results = []
    page = 1

    start_url = build_url(category_slug, city_key, page=1)
    log(f"🔗 URL: {start_url}", "info")

    current_url = start_url

    while current_url:
        if stop_flag and stop_flag():
            log("⛔ Остановлено", "err")
            break

        log(f"📄 Стр. {page}: {current_url}", "info")

        try:
            time.sleep(random.uniform(1.5, 3.2))
            r = SESSION.get(current_url, timeout=20)

            if r.status_code == 404:
                log(f"  ✗ 404 — URL не найден: {current_url}", "err")
                log(f"  Проверь slug категории или ID города", "err")
                break
            if r.status_code == 429:
                log(f"  ⚠ Rate limit — пауза 25 сек", "err")
                time.sleep(25)
                r = SESSION.get(current_url, timeout=20)
            if r.status_code != 200:
                log(f"  ✗ HTTP {r.status_code}", "err")
                break

            page_results, next_url = parse_firmy_page(r.text, current_url)

            if not page_results:
                log(f"  Карточек не найдено — конец пагинации", "info")
                break

            log(f"  ✓ {len(page_results)} фирм на странице", "ok")

            # Для каждой фирмы — ищем email на сайте
            for i, firm in enumerate(page_results):
                if stop_flag and stop_flag():
                    break
                if not firm.get("email") and firm.get("website"):
                    log(f"  [{i+1}/{len(page_results)}] {firm['name'][:38]} → email...", "info")
                    email = get_email_from_website(firm["website"])
                    if email:
                        firm["email"] = email
                        log(f"    ✉ {email}", "ok")
                    time.sleep(random.uniform(0.4, 1.0))
                else:
                    em = firm.get("email","")
                    log(f"  [{i+1}/{len(page_results)}] ✓ {firm['name'][:38]}{' | ✉ '+em if em else ''}", "ok")

            all_results.extend(page_results)
            log(f"  Накоплено: {len(all_results)}", "info")

            if next_url and next_url != current_url:
                current_url = next_url
                page += 1
                time.sleep(random.uniform(2.0, 4.0))
            else:
                log("  Следующей страницы нет — сбор завершён", "info")
                break

        except Exception as e:
            log(f"  ✗ Ошибка: {e}", "err")
            break

    log(f"📊 Итого: {len(all_results)} фирм ({page} стр.)", "ok")
    return all_results


if __name__ == "__main__":
    init_db()
    print(f"DB: {DB_FILE}\n")
    print("Тест URL генерации:")
    tests = [
        ("Praha",            "Restauracni-a-pohostinske-sluzby"),
        ("Praha",            "Restauracni-a-pohostinske-sluzby/Kavarny"),
        ("Brno",             "Restauracni-a-pohostinske-sluzby/Kavarny"),
        ("Ostrava",          "Informacni-technologie"),
    ]
    for city, cat in tests:
        u1 = build_url(cat, city, page=1)
        u2 = build_url(cat, city, page=2)
        print(f"  {city} / {cat}")
        print(f"    стр.1: {u1}")
        print(f"    стр.2: {u2}")
