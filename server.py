"""
ICEBERG OFFICE — Server
========================
Запуск: python server.py
Установка: pip install flask flask-cors requests beautifulsoup4 openpyxl
"""
import os, json, threading, datetime, io
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from scraper import (
    init_db, scrape_firmy_cz, save_contacts, save_session,
    get_all_contacts, get_stats, update_contact, delete_contact,
    clear_all, calc_quality, CATEGORIES, CITIES, DB_FILE
)

app = Flask(__name__)
CORS(app)

# ── State ──────────────────────────────────────
state = {
    "running": False,
    "progress": 0,
    "status": "Ожидание",
    "log": [],
    "stop": False,
}

def log(msg, t="info"):
    entry = {"time": datetime.datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": t}
    state["log"].append(entry)
    print(f"[{entry['time']}] {msg}")

def run_scrape(params):
    state["running"] = True
    state["stop"] = False
    state["progress"] = 0
    state["log"] = []

    category_slug = params.get("category", "restaurace-bary-a-kavarny")
    city = params.get("city", "Praha")
    city_slug = CITIES.get(city, city)
    category_name = CATEGORIES.get(category_slug, category_slug)

    log(f"🚀 Сбор запущен", "info")
    log(f"📂 Категория: {category_name}", "info")
    log(f"📍 Город: {city}", "info")

    def update_progress(msg, t="info"):
        log(msg, t)
        # Rough progress update based on log count
        state["progress"] = min(95, len(state["log"]) * 2)
        state["status"] = msg[:60]

    try:
        results = scrape_firmy_cz(
            category_slug=category_slug,
            city_slug=city_slug,
            log_fn=update_progress,
            stop_flag=lambda: state["stop"],
        )

        state["progress"] = 95
        log(f"💾 Сохранение в базу данных...", "info")
        saved = save_contacts(results, city=city, category=category_name)

        emails = sum(1 for r in results if r.get("email"))
        phones = sum(1 for r in results if r.get("phone"))

        save_session(
            dt=datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
            category=category_name,
            city=city,
            total_found=len(results),
            total_saved=saved,
            emails=emails,
            phones=phones,
        )

        log(f"✅ ГОТОВО!", "ok")
        log(f"   Найдено всего: {len(results)}", "ok")
        log(f"   Новых сохранено: {saved}", "ok")
        log(f"   С email: {emails}", "ok")
        log(f"   С телефоном: {phones}", "ok")

    except Exception as e:
        log(f"❌ Ошибка: {e}", "err")

    state["progress"] = 100
    state["running"] = False
    state["status"] = f"Готово"


# ── API ────────────────────────────────────────

@app.route("/api/status")
def api_status():
    return jsonify({
        "running":  state["running"],
        "progress": state["progress"],
        "status":   state["status"],
        "db_file":  DB_FILE,
    })

@app.route("/api/categories")
def api_categories():
    return jsonify(CATEGORIES)

@app.route("/api/cities")
def api_cities():
    return jsonify(list(CITIES.keys()))

@app.route("/api/start", methods=["POST"])
def api_start():
    if state["running"]:
        return jsonify({"error": "Уже запущен"}), 400
    params = request.json or {}
    threading.Thread(target=run_scrape, args=(params,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    state["stop"] = True
    state["running"] = False
    log("⛔ Остановлено", "err")
    return jsonify({"ok": True})

@app.route("/api/log")
def api_log():
    since = int(request.args.get("since", 0))
    return jsonify(state["log"][since:])

@app.route("/api/progress")
def api_progress():
    return jsonify({"progress": state["progress"], "running": state["running"], "status": state["status"]})

@app.route("/api/contacts")
def api_contacts():
    contacts = get_all_contacts(
        city=request.args.get("city") or None,
        category=request.args.get("category") or None,
        status=request.args.get("status") or None,
        search=request.args.get("search") or None,
        sort=request.args.get("sort", "date"),
        limit=int(request.args.get("limit", 2000)),
        offset=int(request.args.get("offset", 0)),
    )
    return jsonify(contacts)

@app.route("/api/contacts/<int:cid>", methods=["PUT"])
def api_update(cid):
    update_contact(cid, request.json or {})
    return jsonify({"ok": True})

@app.route("/api/contacts/<int:cid>", methods=["DELETE"])
def api_delete(cid):
    delete_contact(cid)
    return jsonify({"ok": True})

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/db/clear", methods=["POST"])
def api_clear():
    clear_all()
    return jsonify({"ok": True})

@app.route("/api/export/csv")
def api_csv():
    contacts = get_all_contacts(
        city=request.args.get("city") or None,
        category=request.args.get("category") or None,
    )
    cols = ["id","name","category","city","address","phone","email","website","description","status","quality","date_added"]
    heads = ["#","Название","Категория","Город","Адрес","Телефон","Email","Сайт","Описание","Статус","Качество","Дата"]
    rows = [heads] + [[str(c.get(k,"") or "") for k in cols] for c in contacts]
    csv = "\n".join([",".join([f'"{v.replace(chr(34), chr(34)*2)}"' for v in row]) for row in rows])
    return Response("\ufeff" + csv, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": "attachment; filename=iceberg_contacts.csv"})

@app.route("/api/export/email-list")
def api_email_list():
    contacts = get_all_contacts(city=request.args.get("city") or None,
                                 category=request.args.get("category") or None)
    with_email = [c for c in contacts if c.get("email")]
    rows = [["Email","Название","Город","Категория","Телефон","Сайт"]]
    rows += [[c.get("email",""), c.get("name",""), c.get("city",""),
              c.get("category",""), c.get("phone",""), c.get("website","")] for c in with_email]
    csv = "\n".join([",".join([f'"{v}"' for v in r]) for r in rows])
    return Response("\ufeff" + csv, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": "attachment; filename=email_list.csv"})

@app.route("/api/export/xlsx")
def api_xlsx():
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        return jsonify({"error": "pip install openpyxl"}), 500

    contacts = get_all_contacts(
        city=request.args.get("city") or None,
        category=request.args.get("category") or None,
    )
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "ICEBERG Contacts"

    cols   = ["id","name","category","city","address","phone","email","website","description","status","quality","date_added"]
    heads  = ["#","Название","Категория","Город","Адрес","Телефон","Email","Сайт","Описание","Статус","Качество %","Дата"]
    widths = [6,35,22,16,28,18,30,30,40,14,12,14]

    hfill = PatternFill("solid", fgColor="111827")
    hfont = Font(bold=True, color="3D8EFF", name="Calibri", size=11)
    for i,(h,w) in enumerate(zip(heads,widths),1):
        cell = ws.cell(row=1,column=i,value=h)
        cell.font = hfont; cell.fill = hfill
        cell.alignment = Alignment(horizontal="center",vertical="center")
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w
    ws.row_dimensions[1].height = 26
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{openpyxl.utils.get_column_letter(len(cols))}1"

    for ri,c in enumerate(contacts,2):
        bg = PatternFill("solid", fgColor="161614" if ri%2==0 else "1C1C1A")
        for ci,k in enumerate(cols,1):
            cell = ws.cell(row=ri,column=ci,value=c.get(k,"") or "")
            cell.fill = bg
            cell.font = Font(name="Calibri",size=10,color="E0DDD5")
            cell.alignment = Alignment(vertical="center")
        ws.row_dimensions[ri].height = 18

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    return Response(buf.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":"attachment; filename=iceberg_contacts.xlsx"})

@app.route("/")
def index():
    s = get_stats()
    return f"""<html><body style="background:#111;color:#e0ddd5;font-family:monospace;padding:40px">
    <h2 style="color:#3d8eff">✅ ICEBERG OFFICE — Server Running</h2>
    <p>База: <b style="color:#3d8eff">{s['total']} контактов</b> | 
       Email: {s['with_email']} | Телефон: {s['with_phone']}</p>
    <p>DB: {DB_FILE}</p>
    </body></html>"""

if __name__ == "__main__":
    init_db()
    PORT = int(os.environ.get("PORT", 5000))
    print(f"\n{'='*44}\n  ICEBERG OFFICE — Server\n  http://localhost:{PORT}\n  DB: {DB_FILE}\n{'='*44}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
