"""
ICEBERG OFFICE — Python Backend Server
Реальные источники: Google Maps, Firmy.cz, Zlaté stránky + email с сайтов
Установка: pip install flask flask-cors playwright requests beautifulsoup4 openpyxl && playwright install chromium
Запуск: python server.py
"""
import re, time, random, datetime, json, threading, os

# Playwright browsers path
if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/ms-playwright"
from flask import Flask, jsonify, request, Response
from flask_cors import CORS

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False

try:
    import requests as req_lib
    from bs4 import BeautifulSoup
    BS4_OK = True
except ImportError:
    BS4_OK = False

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    XLSX_OK = True
except ImportError:
    XLSX_OK = False

app = Flask(__name__)
CORS(app)

state = {"running": False, "progress": 0, "status_msg": "Ожидание", "log": [], "last_results": []}
DB_FILE = "iceberg_db.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "cs-CZ,cs;q=0.9",
}

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f: return json.load(f)
    return []

def save_db(data):
    with open(DB_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def log(msg, level="info"):
    entry = {"time": datetime.datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    state["log"].append(entry)
    print(f"[{entry['time']}] {msg}")

def rand_sleep(a=1.5, b=4.0): time.sleep(random.uniform(a, b))

def extract_email(text):
    if not text: return None
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    skip = ["example","domain","test","noreply","no-reply","sentry","wix","wordpress","google","schema"]
    for e in emails:
        if not any(x in e.lower() for x in skip): return e
    return None

def extract_phone(text):
    if not text: return None
    phones = re.findall(r"(?:\+420[\s\-]?)?[0-9]{3}[\s\-]?[0-9]{3}[\s\-]?[0-9]{3}", text)
    return phones[0].strip() if phones else None

def calc_quality(c):
    s = 0
    if c.get("name"):   s += 20
    if c.get("email"):  s += 35
    if c.get("phone"):  s += 20
    if c.get("site"):   s += 10
    if c.get("addr"):   s += 5
    if c.get("person"): s += 10
    return s

def get_email_from_site(url, timeout=8):
    if not BS4_OK or not url: return None
    try:
        if not url.startswith("http"): url = "https://" + url
        r = req_lib.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        email = extract_email(r.text)
        if email: return email
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a.get("href",""); text = a.get_text(strip=True).lower()
            if any(w in text or w in href.lower() for w in ["kontakt","contact","o-nas"]):
                curl = href if href.startswith("http") else url.rstrip("/")+"/"+href.lstrip("/")
                try:
                    cr = req_lib.get(curl, headers=HEADERS, timeout=6)
                    e = extract_email(cr.text)
                    if e: return e
                except: pass
    except: pass
    return None

def scrape_firmy_cz(slug, max_results=15):
    results = []
    if not BS4_OK: return results
    url = f"https://www.firmy.cz/{slug}?location=Praha"
    log(f"  Firmy.cz → {url}")
    try:
        r = req_lib.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("li[class*='item'], div[class*='companyBox'], article[class*='company'], div.item, li.item")
        log(f"  Найдено: {len(cards)} карточек")
        for card in cards[:max_results]:
            try:
                name_el = card.select_one("h2 a, h3 a, a[class*='companyName'], a[class*='name']")
                if not name_el: continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 2: continue
                phone = extract_phone(card.get_text())
                site = None
                for a in card.select("a[href]"):
                    href = a.get("href","")
                    if href.startswith("http") and "firmy.cz" not in href and "seznam.cz" not in href:
                        site = href; break
                addr_el = card.select_one("span[class*='address'], p[class*='address'], div[class*='address']")
                addr = addr_el.get_text(strip=True) if addr_el else "Praha"
                email = get_email_from_site(site) if site else None
                results.append({"name":name,"phone":phone,"email":email,"site":site,"addr":addr,"cat":slug,"source":"Firmy.cz"})
                log(f"  ✓ {name} | {email or 'нет email'}", "ok")
                rand_sleep(0.5, 1.5)
            except: pass
        rand_sleep(2, 4)
    except Exception as e:
        log(f"  Firmy.cz ошибка: {e}", "err")
    return results

def scrape_zlate_stranky(query, max_results=15):
    results = []
    if not BS4_OK: return results
    url = f"https://www.zlatestranky.cz/hledani/?co={query.replace(' ','+')}&kde=Praha"
    log(f"  Zlaté stránky → {url}")
    try:
        r = req_lib.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.subject, li.subject, article.subject, div[class*='SubjectBox']")
        log(f"  Najdeno: {len(cards)} karticek")
        for card in cards[:max_results]:
            try:
                name_el = card.select_one("h2, h3, a[class*='name']")
                if not name_el: continue
                name = name_el.get_text(strip=True)
                if not name or len(name) < 2: continue
                phone = extract_phone(card.get_text())
                site = None
                for a in card.select("a[href]"):
                    href = a.get("href","")
                    if href.startswith("http") and "zlatestranky" not in href:
                        site = href; break
                addr_el = card.select_one("span[class*='address'], div[class*='address']")
                addr = addr_el.get_text(strip=True) if addr_el else "Praha"
                email = get_email_from_site(site) if site else None
                results.append({"name":name,"phone":phone,"email":email,"site":site,"addr":addr,"cat":query,"source":"Zlaté stránky"})
                log(f"  ✓ {name} | {email or 'нет email'}", "ok")
                rand_sleep(0.5, 1.5)
            except: pass
        rand_sleep(2, 4)
    except Exception as e:
        log(f"  Zlaté stránky ошибка: {e}", "err")
    return results

def scrape_google_maps(query, max_results=20):
    results = []
    if not PLAYWRIGHT_OK:
        log("❌ Playwright не установлен", "err"); return results
    log(f"  Google Maps → '{query}'")
    try:
        with sync_playwright() as pw:
            launch_args = [
                    "--no-sandbox",
                    "--disable-dev-shm-usage", 
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--no-first-run",
                    "--no-zygote",
                    "--single-process",
                    "--disable-extensions",
                ]
                browser = pw.chromium.launch(headless=True, args=launch_args)
            ctx = browser.new_context(viewport={"width":1366,"height":768}, user_agent=HEADERS["User-Agent"], locale="cs-CZ")
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page.goto(f"https://www.google.com/maps/search/{query.replace(' ','+')+'/'}",wait_until="domcontentloaded",timeout=30000)
            time.sleep(random.uniform(2,4))
            for sel in ['button:has-text("Přijmout vše")','button:has-text("Accept all")','button:has-text("Souhlasím")']:
                try:
                    btn = page.locator(sel)
                    if btn.count() > 0: btn.first.click(); time.sleep(1.5); break
                except: pass
            for _ in range(5):
                try:
                    feed = page.locator('div[role="feed"]')
                    if feed.count() > 0: feed.evaluate("el => el.scrollBy(0, 1000)"); time.sleep(random.uniform(1.2, 2.0))
                except: break
            cards = page.locator('div[role="feed"] a.hfpxzc, div[role="feed"] > div > div > a')
            total = min(cards.count(), max_results)
            log(f"  Найдено карточек: {total}")
            for i in range(total):
                if not state["running"]: break
                try:
                    card = cards.nth(i)
                    log(f"  [{i+1}/{total}] {(card.get_attribute('aria-label') or '#'+str(i+1))[:45]}")
                    card.click(); time.sleep(random.uniform(2.5, 4.0))
                    try: page.wait_for_selector('h1.DUwDvf, h1[class*="fontHeadlineLarge"]', timeout=8000)
                    except PWTimeout:
                        page.go_back(); time.sleep(2); continue
                    def gtxt(sel):
                        try:
                            el = page.locator(sel)
                            return el.first.inner_text(timeout=3000).strip() if el.count() > 0 else None
                        except: return None
                    name = gtxt('h1.DUwDvf, h1[class*="fontHeadlineLarge"]')
                    if not name: page.go_back(); time.sleep(1.5); continue
                    cat = gtxt('button.DkEaL, span[class*="fontBodyMedium"] button')
                    phone = gtxt('[data-item-id*="phone"] .Io6YTe')
                    addr = gtxt('[data-item-id="address"] .Io6YTe')
                    rating = gtxt('div.F7nice span[aria-hidden="true"]')
                    site = None
                    try:
                        se = page.locator('[data-item-id*="authority"] a')
                        if se.count() > 0: site = se.first.get_attribute("href") or se.first.inner_text(timeout=2000)
                    except: pass
                    email = get_email_from_site(site) if site else None
                    results.append({"name":name,"cat":cat or query,"phone":phone,"email":email,"site":site,"addr":addr or "Praha","rating":rating,"source":"Google Maps"})
                    log(f"  ✓ {name} | ☎ {phone or '—'} | ✉ {email or '—'}", "ok")
                    page.go_back(); time.sleep(random.uniform(1.5, 3.0))
                except Exception as e:
                    log(f"  ✗ {e}", "err")
                    try: page.go_back(); time.sleep(2)
                    except: pass
            browser.close()
    except Exception as e:
        log(f"  Google Maps ошибка: {e}", "err")
    return results

CATEGORY_MAP = {
    "Рестораны":     ("restaurace",    "restaurace Praha"),
    "IT / Tech":     ("it-firmy",      "IT software firma Praha"),
    "Строительство": ("stavebnictvi",  "stavební firma Praha"),
    "Маркетинг":     ("marketing",     "marketingová agentura Praha"),
    "Бухгалтерия":   ("ucetnictvi",    "účetní firma Praha"),
    "Недвижимость":  ("reality",       "realitní kancelář Praha"),
    "Юридические":   ("pravni-sluzby", "advokátní kancelář Praha"),
    "Транспорт":     ("doprava",       "dopravní firma Praha"),
    "Медицина":      ("zdravotnictvi", "soukromá klinika Praha"),
    "Отели":         ("hotely",        "hotel Praha centrum"),
    "Фитнес":        ("fitness",       "fitness centrum Praha"),
    "Авто":          ("autoservisy",   "autoservis Praha"),
    "Красота":       ("kosmetika",     "kosmetický salon Praha"),
    "Образование":   ("vzdelavani",    "vzdělávací centrum Praha"),
    "Туризм":        ("cestovni-ruch", "cestovní kancelář Praha"),
    "Финансы":       ("finance",       "finanční poradenství Praha"),
    "Логистика":     ("logistika",     "logistická firma Praha"),
}

def run_collection(params):
    state["running"] = True; state["progress"] = 0; state["log"] = []; state["last_results"] = []
    categories = params.get("categories", ["Рестораны"])
    sources    = params.get("sources", ["Google Maps", "Firmy.cz"])
    max_total  = int(params.get("max_total", 10))
    log("🚀 Сбор запущен", "info")
    log(f"📂 {', '.join(categories)}", "info")
    log(f"🔌 {', '.join(sources)}", "info")
    existing_db = load_db()
    existing_names = {c.get("name","").lower().strip() for c in existing_db}
    all_new = []; seen = set()
    per_cat = max(3, max_total // max(len(categories), 1))
    total_steps = len(categories) * len(sources); step = 0
    for cat in categories:
        if not state["running"]: break
        firmy_slug, gmaps_query = CATEGORY_MAP.get(cat, (cat.lower(), f"{cat} Praha"))
        log(f"\n📂 ── {cat} ──", "info")
        for source in sources:
            if not state["running"]: break
            step += 1; state["progress"] = int(step / max(total_steps,1) * 88)
            state["status_msg"] = f"{cat} — {source}..."
            raw = []
            if source == "Google Maps":
                raw = scrape_google_maps(gmaps_query, max_results=per_cat)
            elif source in ("Firmy.cz", "Firmy.cz / Zlaté stránky"):
                raw = scrape_firmy_cz(firmy_slug, max_results=per_cat)
                raw.extend(scrape_zlate_stranky(gmaps_query.split(" Praha")[0], max_results=per_cat))
            for c in raw:
                key = c.get("name","").lower().strip()
                if key and key not in seen and key not in existing_names:
                    seen.add(key)
                    c["date"] = datetime.date.today().strftime("%d.%m.%Y")
                    c["id"]   = f"{int(time.time()*1000)}-{random.randint(1000,9999)}"
                    c["status"]  = "new"
                    c["quality"] = calc_quality(c)
                    all_new.append(c)
            rand_sleep(3, 6)
        if len(all_new) >= max_total:
            all_new = all_new[:max_total]; break
    state["progress"] = 96
    if all_new: save_db(all_new + existing_db)
    emails = sum(1 for c in all_new if c.get("email"))
    log(f"\n✅ ГОТОВО: {len(all_new)} контактов | ✉ email: {emails}", "ok")
    state["last_results"] = all_new; state["progress"] = 100
    state["running"] = False; state["status_msg"] = f"Готово: +{len(all_new)} контактов"

@app.route("/api/status")
def api_status(): return jsonify({"running":state["running"],"progress":state["progress"],"status_msg":state["status_msg"],"playwright":PLAYWRIGHT_OK,"bs4":BS4_OK})

@app.route("/api/start", methods=["POST"])
def api_start():
    if state["running"]: return jsonify({"error":"Уже запущен"}),400
    threading.Thread(target=run_collection, args=(request.json or {},), daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/stop", methods=["POST"])
def api_stop(): state["running"]=False; log("⛔ Остановлено","err"); return jsonify({"ok":True})

@app.route("/api/log")
def api_log(): return jsonify(state["log"][int(request.args.get("since",0)):])

@app.route("/api/progress")
def api_progress(): return jsonify({"progress":state["progress"],"running":state["running"],"status_msg":state["status_msg"]})

@app.route("/api/contacts")
def api_contacts(): return jsonify(load_db())

@app.route("/api/contacts", methods=["POST"])
def api_add():
    data=request.json; db=load_db()
    data["id"]=f"{int(time.time()*1000)}-{random.randint(1000,9999)}"
    data["date"]=datetime.date.today().strftime("%d.%m.%Y"); data["quality"]=calc_quality(data)
    db.insert(0,data); save_db(db); return jsonify({"ok":True,"contact":data})

@app.route("/api/contacts/<cid>", methods=["PUT"])
def api_update(cid):
    db=load_db(); data=request.json
    for i,c in enumerate(db):
        if str(c.get("id"))==str(cid):
            data["id"]=cid; data["quality"]=calc_quality(data); db[i]=data; save_db(db); return jsonify({"ok":True})
    return jsonify({"error":"Не найден"}),404

@app.route("/api/contacts/<cid>", methods=["DELETE"])
def api_delete(cid):
    db=[c for c in load_db() if str(c.get("id"))!=str(cid)]; save_db(db); return jsonify({"ok":True})

@app.route("/api/export/csv")
def api_csv():
    db=load_db()
    cols=["name","cat","email","phone","site","addr","person","status","quality","rating","source","date"]
    head=["Название","Категория","Email","Телефон","Сайт","Адрес","Контакт","Статус","Качество","Рейтинг","Источник","Дата"]
    rows=[head]+[[str(r.get(k,"") or "") for k in cols] for r in db]
    csv="\n".join([",".join([f'"{v.replace(chr(34),chr(34)*2)}"' for v in row]) for row in rows])
    return Response(csv,mimetype="text/csv; charset=utf-8",headers={"Content-Disposition":"attachment; filename=iceberg_contacts.csv"})

@app.route("/api/export/email-list")
def api_email():
    db=[c for c in load_db() if c.get("email")]
    rows=[["Email","Название","Контактное лицо","Категория"]]+[[c.get("email",""),c.get("name",""),c.get("person",""),c.get("cat","")] for c in db]
    csv="\n".join([",".join([f'"{v}"' for v in r]) for r in rows])
    return Response(csv,mimetype="text/csv; charset=utf-8",headers={"Content-Disposition":"attachment; filename=iceberg_email_list.csv"})

@app.route("/api/export/xlsx")
def api_xlsx():
    if not XLSX_OK: return jsonify({"error":"pip install openpyxl"}),500
    from io import BytesIO
    db=load_db(); wb=openpyxl.Workbook(); ws=wb.active; ws.title="Контакты ICEBERG"
    cols=["name","cat","email","phone","site","addr","person","status","quality","rating","source","date"]
    heads=["Название","Категория","Email","Телефон","Сайт","Адрес","Контакт","Статус","Качество %","Рейтинг","Источник","Дата"]
    widths=[35,20,32,18,30,28,22,14,12,10,16,14]
    hfill=PatternFill("solid",fgColor="1B4F72"); hfont=Font(bold=True,color="FFFFFF",name="Calibri",size=11)
    for i,(h,w) in enumerate(zip(heads,widths),1):
        cell=ws.cell(row=1,column=i,value=h); cell.font=hfont; cell.fill=hfill
        cell.alignment=Alignment(horizontal="center",vertical="center")
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width=w
    ws.row_dimensions[1].height=28; ws.freeze_panes="A2"
    ws.auto_filter.ref=f"A1:{openpyxl.utils.get_column_letter(len(cols))}1"
    for ri,r in enumerate(db,2):
        bg=PatternFill("solid",fgColor="EBF5FB" if ri%2==0 else "FFFFFF")
        for ci,k in enumerate(cols,1):
            cell=ws.cell(row=ri,column=ci,value=r.get(k,"") or ""); cell.fill=bg
            cell.font=Font(name="Calibri",size=10); cell.alignment=Alignment(vertical="center")
        ws.row_dimensions[ri].height=18
    buf=BytesIO(); wb.save(buf); buf.seek(0)
    return Response(buf.read(),mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",headers={"Content-Disposition":"attachment; filename=iceberg_contacts.xlsx"})

@app.route("/api/db/clear", methods=["POST"])
def api_clear(): save_db([]); return jsonify({"ok":True})

@app.route("/")
def index():
    db=load_db()
    return f'<html><body style="background:#1a1a18;color:#e8e6df;font-family:monospace;padding:40px"><h2 style="color:#4d9fff">✅ ICEBERG OFFICE Server</h2><p>База: <b style="color:#4d9fff">{len(db)} контактов</b></p><p>Playwright: {"✅" if PLAYWRIGHT_OK else "❌"}</p><p>BS4: {"✅" if BS4_OK else "❌"}</p></body></html>'

if __name__=="__main__":
    PORT=int(os.environ.get("PORT",5000))
    print(f"\n{'='*40}\n  ICEBERG OFFICE Server\n  Playwright: {'✅' if PLAYWRIGHT_OK else '❌'}\n  BS4: {'✅' if BS4_OK else '❌'}\n  http://localhost:{PORT}\n{'='*40}\n")
    app.run(host="0.0.0.0",port=PORT,debug=False,threaded=True)
