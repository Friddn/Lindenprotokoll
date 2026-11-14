from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

APP_TZ = ZoneInfo("Europe/Berlin")
from io import BytesIO
import json
from flask import Flask, flash, redirect, render_template, request, send_file, session, url_for, abort
import mimetypes, os, csv, sqlite3, secrets, hashlib
from pathlib import Path
from werkzeug.utils import secure_filename
import re
import urllib.parse, urllib.request
from werkzeug.security import check_password_hash, generate_password_hash

from config import DEBUG, HOST, PORT, SECRET_KEY
from db import *
from device import detect_device_type

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(
    MAX_CONTENT_LENGTH=15 * 1024 * 1024,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=__import__("datetime").timedelta(days=30),
)

@app.context_processor
def inject_csrf_token():
    def csrf_token():
        token = session.get("_csrf_token")
        if not token:
            token = secrets.token_urlsafe(32)
            session["_csrf_token"] = token
        return token
    return {"csrf_token": csrf_token}

@app.before_request
def protect_post_requests():
    if request.method != "POST":
        return None
    # JSON requests are not submitted by ordinary HTML forms; browsers enforce
    # a CORS preflight for application/json. Validate Origin whenever supplied.
    if request.is_json:
        origin = request.headers.get("Origin")
        if origin and urllib.parse.urlparse(origin).netloc != request.host:
            abort(400, description="Ungültiger Request-Ursprung.")
        return None
    expected = session.get("_csrf_token")
    supplied = request.form.get("_csrf_token", "")
    if not expected or not secrets.compare_digest(expected, supplied):
        abort(400, description="Sicherheitsprüfung fehlgeschlagen. Bitte Seite neu laden.")

def now_date_time():
    n = datetime.now(APP_TZ)
    return n.strftime("%Y-%m-%d"), n.strftime("%H:%M")

def base_context():
    return {"app_name": "Lindenprotokoll"}

def current_device_meta():
    ua = request.headers.get("User-Agent")
    return detect_device_type(ua), ua

def csv_resp(content: str, filename: str):
    return send_file(BytesIO(content.encode("utf-8-sig")), mimetype="text/csv", as_attachment=True, download_name=filename)

@app.before_request
def setup():
    init_db()

# Shared PIN gate. Static assets and the two PIN endpoints remain accessible.
_PIN_EXEMPT_ENDPOINTS = {"pin_login", "pin_setup", "static"}
@app.before_request
def require_pin():
    if request.endpoint in _PIN_EXEMPT_ENDPOINTS or request.endpoint is None:
        return None
    if not session.get("pin_authenticated"):
        return redirect(url_for("pin_login", next=request.path))
    if get_setting("security_pin_change_required", "1") == "1" and request.endpoint != "pin_setup":
        return redirect(url_for("pin_setup"))
    return None

@app.route("/pin", methods=["GET", "POST"])
def pin_login():
    if session.get("pin_authenticated") and get_setting("security_pin_change_required", "1") != "1":
        return redirect(url_for("index"))
    if request.method == "POST":
        now = int(datetime.now().timestamp())
        # Store only a keyed hash of the client address, not the raw IP.
        client_key = hashlib.sha256((SECRET_KEY + ":" + (request.remote_addr or "unknown")).encode("utf-8")).hexdigest()
        with connect() as conn:
            state = conn.execute("SELECT failed_attempts, locked_until FROM security_login_attempts WHERE client_key=?", (client_key,)).fetchone()
            failures = int(state["failed_attempts"] if state else 0)
            locked_until = int(state["locked_until"] if state else 0)
            if now < locked_until:
                flash("Zu viele Fehlversuche. Bitte später erneut versuchen.", "error")
            else:
                pin = request.form.get("pin", "")
                stored = get_setting("security_pin_hash", "")
                if re.fullmatch(r"\d{6}", pin) and stored and check_password_hash(stored, pin):
                    conn.execute("INSERT INTO security_login_attempts(client_key,failed_attempts,locked_until) VALUES(?,0,0) ON CONFLICT(client_key) DO UPDATE SET failed_attempts=0,locked_until=0,updated_at=CURRENT_TIMESTAMP", (client_key,))
                    session.clear()
                    session["pin_authenticated"] = True
                    session.permanent = True
                    if get_setting("security_pin_change_required", "1") == "1":
                        return redirect(url_for("pin_setup"))
                    target = request.args.get("next", "")
                    return redirect(target if target.startswith("/") and not target.startswith("//") else url_for("index"))
                failures += 1
                delay = min(300, 30 * max(1, failures - 4)) if failures >= 5 else 0
                conn.execute("INSERT INTO security_login_attempts(client_key,failed_attempts,locked_until) VALUES(?,?,?) ON CONFLICT(client_key) DO UPDATE SET failed_attempts=excluded.failed_attempts,locked_until=excluded.locked_until,updated_at=CURRENT_TIMESTAMP",
                             (client_key, failures, now + delay if delay else 0))
                flash("PIN ist nicht korrekt.", "error")
    return render_template("pin_login.html", **base_context())

@app.route("/pin/setup", methods=["GET", "POST"])
def pin_setup():
    # Initial setup is allowed only while the one-time migration flag is set.
    # Later PIN changes must go through Settings and require the current PIN.
    if get_setting("security_pin_change_required", "1") != "1":
        return redirect(url_for("security_settings"))
    if not session.get("pin_authenticated"):
        return redirect(url_for("pin_login"))
    if request.method == "POST":
        pin = request.form.get("pin", "")
        confirm = request.form.get("confirm_pin", "")
        if not re.fullmatch(r"\d{6}", pin):
            flash("Die PIN muss genau sechs Ziffern enthalten.", "error")
        elif pin == "000000":
            flash("Bitte nicht die Standard-PIN verwenden.", "error")
        elif pin != confirm:
            flash("Die PIN-Eingaben stimmen nicht überein.", "error")
        else:
            set_setting("security_pin_hash", generate_password_hash(pin))
            set_setting("security_pin_change_required", "0")
            session.clear()
            session["pin_authenticated"] = True
            session.permanent = True
            flash("Neue PIN wurde gespeichert.", "success")
            return redirect(url_for("index"))
    return render_template("pin_setup.html", **base_context())

@app.post("/pin/logout")
def pin_logout():
    session.clear()
    return redirect(url_for("pin_login"))

@app.route("/")
def index():
    d, t = now_date_time()
    return render_template("start.html", date_value=d, time_value=t, **base_context())

@app.post("/start/<module>")
def start_module(module):
    session["date"] = request.form.get("date", "")
    session["time"] = request.form.get("time", "")
    if module == "meal":
        return redirect(url_for("meal_person"))
    if module == "illness":
        return redirect(url_for("illness_person"))
    if module == "consumption":
        return redirect(url_for("consumption_type"))
    return redirect(url_for("index"))

# --- meal create/edit ---
@app.route("/meal/person", methods=["GET", "POST"])
def meal_person():
    if request.method == "POST":
        session["person_id"] = int(request.form["person_id"])
        return redirect(url_for("meal_entry"))
    return render_template("meal_person.html", people=get_people(), default_person_id=get_last_person_id(), **base_context())

@app.route("/meal/entry", methods=["GET", "POST"])
def meal_entry():
    if "person_id" not in session:
        return redirect(url_for("meal_person"))
    if request.method == "POST" and "new_food_name" in request.form:
        ok, msg = add_food_item(request.form.get("new_food_name", ""))
        flash(msg, "success" if ok else "error")
        return redirect(url_for("meal_entry"))
    return render_template("meal_entry.html", food_items=get_food_items(), food_sort_mode=get_setting("food_sort_mode", "usage"),
                           date_value=session.get("date") or now_date_time()[0], time_value=session.get("time") or now_date_time()[1],
                           **base_context())

@app.post("/meal/add_food_ajax")
def meal_add_food_ajax():
    import json
    name = request.json.get("name", "").strip() if request.is_json else request.form.get("name", "").strip()
    if not name:
        return json.dumps({"ok": False, "error": "Kein Name angegeben."}), 400, {"Content-Type": "application/json"}
    ok, msg = add_food_item(name)
    # Fetch the new/existing item to get its id
    with connect() as conn:
        row = conn.execute("SELECT food_id, food_name FROM food_items_master WHERE lower(food_name)=lower(?)", (name,)).fetchone()
    if row:
        return json.dumps({"ok": True, "food_id": row["food_id"], "food_name": row["food_name"]}), 200, {"Content-Type": "application/json"}
    return json.dumps({"ok": False, "error": msg}), 400, {"Content-Type": "application/json"}

@app.post("/meal/save")
def meal_save():
    dev, ua = current_device_meta()
    entry_id = save_meal(int(session["person_id"]), request.form.get("date",""), request.form.get("time",""),
                         [int(v) for v in request.form.getlist("food_ids")], dev, ua)
    session["undo_entry_id"] = entry_id
    flash("Gespeichert.", "success")
    return redirect(url_for("index"))

@app.route("/edit/meal/<int:entry_id>", methods=["GET", "POST"])
def edit_meal(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "meal":
        return redirect(url_for("history"))
    if request.method == "POST":
        update_meal(entry_id, int(request.form["person_id"]), request.form.get("date",""), request.form.get("time",""),
                    [int(v) for v in request.form.getlist("food_ids")])
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    selected_food_ids = set()
    with connect() as conn:
        selected_food_ids = {r["food_id"] for r in conn.execute("SELECT food_id FROM meal_food_links WHERE entry_id=?", (entry_id,))}
    return render_template("edit_meal.html", details=details, people=get_people(), food_items=get_food_items(),
                           selected_food_ids=selected_food_ids, **base_context())

# --- illness create ---
@app.route("/illness/person", methods=["GET", "POST"])
def illness_person():
    if request.method == "POST":
        session["person_id"] = int(request.form["person_id"])
        return redirect(url_for("illness_type"))
    return render_template("illness_person.html", people=get_people(), default_person_id=get_last_person_id(), **base_context())

@app.route("/illness/type")
def illness_type():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    return render_template("illness_type.html", **base_context())

@app.route("/illness/abdominal", methods=["GET", "POST"])
def illness_abdominal():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    if request.method == "POST":
        regions = [int(v) for v in request.form.getlist("regions")]
        if not regions:
            flash("Bitte mindestens einen Bereich auswählen.", "error")
            return redirect(url_for("illness_abdominal"))
        dev, ua = current_device_meta()
        entry_id = save_abdominal_pain(int(session["person_id"]), request.form.get("date",""), request.form.get("time",""), regions, request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        return redirect(url_for("index"))
    return render_template("illness_abdominal.html", image_url=get_setting("abdominal_image_url", ""),
                           date_value=session.get("date") or now_date_time()[0], time_value=session.get("time") or now_date_time()[1],
                           **base_context())

@app.route("/edit/abdominal/<int:entry_id>", methods=["GET", "POST"])
def edit_abdominal(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "abdominal_pain":
        return redirect(url_for("history"))
    if request.method == "POST":
        regions = [int(v) for v in request.form.getlist("regions")]
        if not regions:
            flash("Bitte mindestens einen Bereich auswählen.", "error")
            return redirect(url_for("edit_abdominal", entry_id=entry_id))
        update_abdominal_pain(entry_id, int(request.form["person_id"]), request.form.get("date",""), request.form.get("time",""), regions, request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_abdominal.html", details=details, people=get_people(), image_url=get_setting("abdominal_image_url", ""), **base_context())

@app.route("/illness/fever", methods=["GET", "POST"])
def illness_fever():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    if request.method == "POST":
        temp = request.form.get("temperature_c","").replace(",", ".")
        try: temp = round(float(temp),1)
        except: 
            flash("Bitte einen gültigen Temperaturwert eingeben.", "error")
            return redirect(url_for("illness_fever"))
        if temp < 34 or temp > 44:
            flash("Temperatur muss zwischen 34,0 und 44,0 liegen.", "error")
            return redirect(url_for("illness_fever"))
        dev, ua = current_device_meta()
        entry_id = save_fever(int(session["person_id"]), request.form.get("date",""), request.form.get("time",""), temp, request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        return redirect(url_for("index"))
    return render_template("illness_fever.html", date_value=session.get("date") or now_date_time()[0], time_value=session.get("time") or now_date_time()[1], **base_context())

@app.route("/edit/fever/<int:entry_id>", methods=["GET", "POST"])
def edit_fever(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "fever":
        return redirect(url_for("history"))
    if request.method == "POST":
        temp = request.form.get("temperature_c","").replace(",", ".")
        try: temp = round(float(temp),1)
        except:
            flash("Bitte einen gültigen Temperaturwert eingeben.", "error")
            return redirect(url_for("edit_fever", entry_id=entry_id))
        update_fever(entry_id, int(request.form["person_id"]), request.form.get("date",""), request.form.get("time",""), temp, request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_fever.html", details=details, people=get_people(), **base_context())

@app.route("/illness/medication", methods=["GET", "POST"])
def illness_medication():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    if request.method == "POST" and "new_medication_name" in request.form:
        ok, msg = add_medication(request.form.get("new_medication_name",""))
        flash(msg, "success" if ok else "error")
        return redirect(url_for("illness_medication"))
    return render_template("illness_medication.html", medications=get_medications(),
                           date_value=session.get("date") or now_date_time()[0], time_value=session.get("time") or now_date_time()[1],
                           **base_context())

@app.post("/illness/medication/save")
def illness_medication_save():
    med_ids = [int(v) for v in request.form.getlist("medication_ids")]
    if not med_ids:
        flash("Bitte mindestens ein Medikament auswählen.", "error")
        return redirect(url_for("illness_medication"))
    dev, ua = current_device_meta()
    entry_id = save_medications(int(session["person_id"]), request.form.get("date",""), request.form.get("time",""), med_ids, request.form.get("notes",""), dev, ua)
    session["undo_entry_id"] = entry_id
    flash("Gespeichert.", "success")
    return redirect(url_for("index"))

@app.route("/edit/medication/<int:entry_id>", methods=["GET", "POST"])
def edit_medication(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "medication":
        return redirect(url_for("history"))
    if request.method == "POST":
        med_ids = [int(v) for v in request.form.getlist("medication_ids")]
        if not med_ids:
            flash("Bitte mindestens ein Medikament auswählen.", "error")
            return redirect(url_for("edit_medication", entry_id=entry_id))
        update_medications(entry_id, int(request.form["person_id"]), request.form.get("date",""), request.form.get("time",""), med_ids, request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_medication.html", details=details, people=get_people(), medications=get_medications(), **base_context())

@app.route("/illness/symptoms", methods=["GET", "POST"])
def illness_symptoms():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    if request.method == "POST" and "new_symptom_name" in request.form:
        ok, msg = add_symptom(request.form.get("new_symptom_name", ""))
        flash(msg, "success" if ok else "error")
        return redirect(url_for("illness_symptoms"))
    return render_template("illness_symptoms.html", symptoms=get_symptoms(),
                           symptom_sort_mode=get_setting("symptom_sort_mode", "usage"),
                           date_value=session.get("date") or now_date_time()[0],
                           time_value=session.get("time") or now_date_time()[1],
                           **base_context())

@app.post("/illness/symptoms/save")
def illness_symptoms_save():
    symptom_ids = [int(v) for v in request.form.getlist("symptom_ids")]
    if not symptom_ids:
        flash("Bitte mindestens ein Symptom auswählen.", "error")
        return redirect(url_for("illness_symptoms"))
    dev, ua = current_device_meta()
    entry_id = save_symptoms(int(session["person_id"]), request.form.get("date", ""),
                             request.form.get("time", ""), symptom_ids,
                             request.form.get("notes", ""), dev, ua)
    session["undo_entry_id"] = entry_id
    flash("Gespeichert.", "success")
    return redirect(url_for("index"))

@app.route("/edit/symptoms/<int:entry_id>", methods=["GET", "POST"])
def edit_symptoms(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "symptoms":
        return redirect(url_for("history"))
    if request.method == "POST":
        symptom_ids = [int(v) for v in request.form.getlist("symptom_ids")]
        if not symptom_ids:
            flash("Bitte mindestens ein Symptom auswählen.", "error")
            return redirect(url_for("edit_symptoms", entry_id=entry_id))
        update_symptoms(entry_id, int(request.form["person_id"]), request.form.get("date", ""),
                        request.form.get("time", ""), symptom_ids, request.form.get("notes", ""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_symptoms.html", details=details, people=get_people(),
                           symptoms=get_symptoms(), selected_symptom_ids=set(details["symptoms"]),
                           **base_context())

@app.route("/illness/other", methods=["GET", "POST"])
def illness_other():
    if "person_id" not in session:
        return redirect(url_for("illness_person"))
    if request.method == "POST":
        dev, ua = current_device_meta()
        entry_id = save_other_illness(int(session["person_id"]), request.form.get("date",""), request.form.get("time",""), request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        return redirect(url_for("index"))
    return render_template("illness_other.html", date_value=session.get("date") or now_date_time()[0], time_value=session.get("time") or now_date_time()[1], **base_context())

@app.route("/edit/other/<int:entry_id>", methods=["GET", "POST"])
def edit_other(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "other":
        return redirect(url_for("history"))
    if request.method == "POST":
        update_other_illness(entry_id, int(request.form["person_id"]), request.form.get("date",""), request.form.get("time",""), request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_other.html", details=details, people=get_people(), **base_context())

# --- consumption create/edit ---
@app.route("/consumption/type")
def consumption_type():
    return render_template("consumption_type.html", **base_context())

@app.route("/consumption/electricity", methods=["GET", "POST"])
def consumption_electricity():
    d = session.get("date") or now_date_time()[0]
    t = session.get("time") or now_date_time()[1]
    if request.method == "POST":
        cons = request.form.get("consumption_meter_kwh","").strip()
        feed = request.form.get("feedin_meter_kwh","").strip()
        cons = int(cons) if cons else None
        feed = int(feed) if feed else None
        if cons is None and feed is None:
            flash("Bitte mindestens einen Zählerstand eingeben.", "error")
            return redirect(url_for("consumption_electricity"))
        dev, ua = current_device_meta()
        entry_id = save_electricity(request.form.get("date",""), request.form.get("time",""), cons, feed, request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        if request.form.get("save_mode") == "stay":
            return redirect(url_for("consumption_electricity"))
        return redirect(url_for("index"))
    return render_template("consumption_electricity.html", date_value=d, time_value=t, **base_context())

@app.route("/edit/electricity/<int:entry_id>", methods=["GET", "POST"])
def edit_electricity(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "electricity":
        return redirect(url_for("history"))
    if request.method == "POST":
        cons = request.form.get("consumption_meter_kwh","").strip()
        feed = request.form.get("feedin_meter_kwh","").strip()
        cons = int(cons) if cons else None
        feed = int(feed) if feed else None
        if cons is None and feed is None:
            flash("Bitte mindestens einen Zählerstand eingeben.", "error")
            return redirect(url_for("edit_electricity", entry_id=entry_id))
        update_electricity(entry_id, request.form.get("date",""), request.form.get("time",""), cons, feed, request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_electricity.html", details=details, **base_context())

@app.route("/consumption/water", methods=["GET", "POST"])
def consumption_water():
    d = session.get("date") or now_date_time()[0]
    t = session.get("time") or now_date_time()[1]
    if request.method == "POST":
        raw = request.form.get("water_meter_m3","").strip()
        if not raw:
            flash("Bitte einen Wasser Zählerstand eingeben.", "error")
            return redirect(url_for("consumption_water"))
        dev, ua = current_device_meta()
        entry_id = save_water(request.form.get("date",""), request.form.get("time",""), int(raw), request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        if request.form.get("save_mode") == "stay":
            return redirect(url_for("consumption_water"))
        return redirect(url_for("index"))
    return render_template("consumption_water.html", date_value=d, time_value=t, **base_context())

@app.route("/edit/water/<int:entry_id>", methods=["GET", "POST"])
def edit_water(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "water":
        return redirect(url_for("history"))
    if request.method == "POST":
        raw = request.form.get("water_meter_m3","").strip()
        if not raw:
            flash("Bitte einen Wasser Zählerstand eingeben.", "error")
            return redirect(url_for("edit_water", entry_id=entry_id))
        update_water(entry_id, request.form.get("date",""), request.form.get("time",""), int(raw), request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_water.html", details=details, **base_context())

@app.route("/consumption/fuel", methods=["GET", "POST"])
def consumption_fuel():
    d = session.get("date") or now_date_time()[0]
    t = session.get("time") or now_date_time()[1]
    if request.method == "POST":
        def pi(n):
            raw = request.form.get(n,"").strip()
            return int(raw) if raw else None
        def pf(n):
            raw = request.form.get(n,"").strip().replace(",", ".")
            return float(raw) if raw else None
        odometer = pi("odometer_km")
        total = pf("total_price_eur")
        liters = pf("liters")
        ppl = pf("price_per_liter")
        if all(v is None for v in [odometer, total, liters, ppl]):
            flash("Bitte mindestens ein Feld ausfüllen.", "error")
            return redirect(url_for("consumption_fuel"))
        if total is not None and liters is not None and ppl is not None and abs((liters * ppl) - total) > 0.05:
            flash("Warnung: Gesamtbetrag passt nicht ganz zu Menge × Literpreis.", "warning")
        dev, ua = current_device_meta()
        entry_id = save_fuel(request.form.get("date",""), request.form.get("time",""), "Kangoo", odometer, total, liters, ppl, request.form.get("notes",""), dev, ua)
        session["undo_entry_id"] = entry_id
        flash("Gespeichert.", "success")
        if request.form.get("save_mode") == "stay":
            return redirect(url_for("consumption_fuel"))
        return redirect(url_for("index"))
    return render_template("consumption_fuel.html", date_value=d, time_value=t, **base_context())

@app.route("/edit/fuel/<int:entry_id>", methods=["GET", "POST"])
def edit_fuel(entry_id):
    details = get_entry_details(entry_id)
    if not details or details["entry"]["subtype"] != "fuel":
        return redirect(url_for("history"))
    if request.method == "POST":
        def pi(n):
            raw = request.form.get(n,"").strip()
            return int(raw) if raw else None
        def pf(n):
            raw = request.form.get(n,"").strip().replace(",", ".")
            return float(raw) if raw else None
        update_fuel(entry_id, request.form.get("date",""), request.form.get("time",""), "Kangoo",
                    pi("odometer_km"), pf("total_price_eur"), pf("liters"), pf("price_per_liter"), request.form.get("notes",""))
        flash("Eintrag aktualisiert.", "success")
        return redirect(url_for("history"))
    return render_template("edit_fuel.html", details=details, **base_context())

# --- history / export / admin / import ---
@app.route("/history")
def history():
    person_id = request.args.get("person_id", "").strip()
    subtype = request.args.get("subtype", "").strip()
    period = request.args.get("period", "").strip()
    entries = get_history_entries(person_id=int(person_id) if person_id else None, subtype=subtype or None, period=period or None)
    return render_template("history.html", entries=entries, people=get_people(), selected_person_id=person_id,
                           selected_subtype=subtype, selected_period=period, **base_context())

@app.route("/history/<int:entry_id>")
def history_detail(entry_id):
    details = get_entry_details(entry_id)
    if not details:
        return redirect(url_for("history"))
    return render_template("history_detail.html", details=details, **base_context())

@app.post("/entry/<int:entry_id>/delete")
def entry_delete(entry_id):
    delete_entry(entry_id)
    flash("Eintrag gelöscht.", "success")
    return redirect(url_for("history"))

@app.route("/export")
def export_page():
    return render_template("export.html", **base_context())

@app.route("/statistik")
def statistik():
    import json
    person_id = request.args.get("person_id", type=int)
    period_days = request.args.get("period", 90, type=int)
    show_pain_free = bool(request.args.get("show_pain_free"))
    data = get_stats_data(person_id=person_id, period_days=period_days)
    return render_template("statistik.html",
        stats_json=json.dumps(data, ensure_ascii=False),
        people=data["people"],
        selected_person_id=person_id,
        selected_period=period_days,
        show_pain_free=show_pain_free,
        **base_context())



@app.route("/statistik/waermepumpe", methods=["GET", "POST"])
def heatpump_analysis():
    """Local import and review for hourly heat-pump/weather data and graph images."""
    from db import connect
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    upload_dir = data_dir / "graph_screenshots"
    upload_dir.mkdir(parents=True, exist_ok=True)
    message = None
    if request.method == "POST":
        action = request.form.get("action")
        if action == "fetch_weather":
            from datetime import date
            start = request.form.get("start_date", "2025-01-01")
            end = request.form.get("end_date", date.today().isoformat())
            # Tornesch, Schleswig-Holstein; Open-Meteo historical archive (ERA5/ERA5-Land).
            params = urllib.parse.urlencode({"latitude":53.70,"longitude":9.72,"start_date":start,"end_date":end,"hourly":"temperature_2m","timezone":"Europe/Berlin"})
            try:
                url = "https://archive-api.open-meteo.com/v1/archive?" + params
                req = urllib.request.Request(url, headers={"User-Agent":"Lindenprotokoll/1.0"})
                with urllib.request.urlopen(req, timeout=45) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                times = payload.get("hourly",{}).get("time",[])
                temps = payload.get("hourly",{}).get("temperature_2m",[])
                count = 0
                with connect() as conn:
                    for ts,temp in zip(times,temps):
                        if temp is None: continue
                        conn.execute("INSERT INTO outdoor_hourly(ts,temperature_c,source) VALUES(?,?,'Open-Meteo ERA5') ON CONFLICT(ts) DO UPDATE SET temperature_c=excluded.temperature_c,source=excluded.source", (ts,float(temp)))
                        count += 1
                message = f"{count} stündliche Tornesch-Temperaturwerte geladen ({start} bis {end}; Open-Meteo ERA5/ERA5-Land)."
            except Exception as exc:
                message = "Wetterdaten konnten nicht geladen werden: " + str(exc)
        elif action == "ocr_screenshot":
            # Optional local OCR. Tesseract is not required for the app to run.
            try:
                shot_id = int(request.form.get("screenshot_id"))
                with connect() as conn:
                    shot = conn.execute("SELECT * FROM graph_screenshots WHERE id=?", (shot_id,)).fetchone()
                if not shot:
                    raise ValueError("Screenshot nicht gefunden.")
                import shutil, subprocess
                if not shutil.which("tesseract"):
                    raise RuntimeError("Tesseract OCR ist nicht installiert. Bitte Tageswerte manuell aus dem Screenshot übernehmen.")
                proc = subprocess.run(["tesseract", shot["stored_path"], "stdout", "-l", "eng+deu", "--psm", "11"],
                                      capture_output=True, text=True, timeout=30, check=False)
                if proc.returncode != 0:
                    raise RuntimeError((proc.stderr or "OCR fehlgeschlagen.")[:300])
                ocr_text = proc.stdout[:12000]
                with connect() as conn:
                    conn.execute("UPDATE graph_screenshots SET ocr_text=? WHERE id=?", (ocr_text, shot_id))
                message = "OCR-Text wurde lokal erkannt. Bitte Datum und kWh-Zahlen anhand des Originalbildes prüfen und manuell bestätigen."
            except Exception as exc:
                message = "OCR nicht verfügbar/fehlgeschlagen: " + str(exc)
        elif action == "save_daily_screenshot_totals":
            try:
                shot_id = int(request.form.get("screenshot_id"))
                day = request.form.get("daily_date", "").strip()
                def optional_nonnegative(field):
                    raw = request.form.get(field, "").strip().replace(",", ".")
                    if not raw: return None
                    value = float(raw)
                    if not __import__("math").isfinite(value) or value < 0:
                        raise ValueError("Tageswerte müssen gültige, nicht-negative Zahlen sein.")
                    return value
                electricity = optional_nonnegative("daily_electricity_kwh")
                thermal = optional_nonnegative("daily_thermal_kwh")
                try:
                    datetime.strptime(day, "%Y-%m-%d")
                except ValueError:
                    raise ValueError("Bitte ein gültiges Datum angeben.")
                if electricity is None and thermal is None:
                    raise ValueError("Bitte mindestens einen abgelesenen Tageswert eingeben.")
                with connect() as conn:
                    shot = conn.execute("SELECT id FROM graph_screenshots WHERE id=?", (shot_id,)).fetchone()
                    if not shot: raise ValueError("Screenshot nicht gefunden.")
                    conn.execute("INSERT INTO daily_hp_totals(date,electricity_kwh,thermal_kwh,source,screenshot_id,confidence) VALUES(?,?,?,'screenshot-manual',?,'confirmed') "
                                 "ON CONFLICT(date) DO UPDATE SET electricity_kwh=excluded.electricity_kwh,thermal_kwh=excluded.thermal_kwh,source=excluded.source,screenshot_id=excluded.screenshot_id,confidence='confirmed',updated_at=CURRENT_TIMESTAMP",
                                 (day, electricity, thermal, shot_id))
                    conn.execute("UPDATE graph_screenshots SET graph_date=? WHERE id=?", (day, shot_id))
                message = f"Bestätigte Tageswerte für {day} gespeichert. Diese Werte sind maßgeblich für die Tagesstatistik."
            except Exception as exc:
                message = "Tageswerte konnten nicht gespeichert werden: " + str(exc)
        elif action == "digitize_screenshot":
            try:
                from graph_digitizer import digitize, auto_calibrate, resample_15min
                from ocr_extract import parse_ocr_fields
                shot_id = int(request.form.get("screenshot_id"))
                with connect() as conn:
                    shot = conn.execute("SELECT * FROM graph_screenshots WHERE id=?",(shot_id,)).fetchone()
                if not shot: raise ValueError("Screenshot nicht gefunden.")
                ocr_text = shot["ocr_text"] or ""
                if not ocr_text:
                    import shutil, subprocess
                    if not shutil.which("tesseract"):
                        raise RuntimeError("Tesseract OCR ist für die automatische Erkennung erforderlich.")
                    proc=subprocess.run(["tesseract",shot["stored_path"],"stdout","-l","eng+deu","--psm","11"],
                                        capture_output=True,text=True,timeout=45,check=False)
                    if proc.returncode: raise RuntimeError("Screenshot-OCR fehlgeschlagen.")
                    ocr_text=proc.stdout[:12000]
                fields=parse_ocr_fields(ocr_text)
                date_value=request.form.get("digitize_date") or shot["graph_date"] or fields.get("date")
                if not date_value: raise ValueError("Das Datum wurde nicht sicher erkannt. Bitte Datum im Screenshot-Datensatz ergänzen.")
                total=0; cadences=[]; extracted=[]; calibrations={}
                for curve in ("green","red"):
                    cal=auto_calibrate(shot["stored_path"],curve)
                    calibrations[curve]=cal
                    pts,cadence=digitize(shot["stored_path"],date_value,cal["x_left"],cal["x_right"],cal["y_top"],cal["y_bottom"],cal["y_min"],cal["y_max"],curve)
                    if cadence: cadences.append(cadence)
                    for pt in pts:
                        extracted.append((shot_id, pt['ts'], pt['value_kw'] if curve=='green' else None,
                                          pt['value_kw'] if curve=='red' else None, pt['confidence'],
                                          'estimated-'+curve+'-'+pt.get('point_kind','corner')))
                    regular = resample_15min(pts, date_value)
                    for pt in regular:
                        extracted.append((shot_id, pt['ts'], pt['value_kw'] if curve=='green' else None,
                                          pt['value_kw'] if curve=='red' else None, pt['confidence'],
                                          'estimated-'+curve+'-'+pt['point_kind']))
                    total += len(pts) + len(regular)
                # Replace this screenshot's prior extraction atomically. Re-running
                # calibration must never accumulate duplicate candidate points.
                with connect() as conn:
                    conn.execute("DELETE FROM hp_graph_points WHERE screenshot_id=?", (shot_id,))
                    conn.executemany("INSERT INTO hp_graph_points(screenshot_id,ts,electricity_kw,thermal_kw,confidence,point_kind) VALUES(?,?,?,?,?,?)", extracted)
                    conn.execute("UPDATE graph_screenshots SET status='digitized',graph_date=? WHERE id=?",(date_value,shot_id))
                cadence_text = f"wiederkehrender Punktabstand etwa {round(sum(cadences)/len(cadences))} Minuten (nur Hinweis)" if cadences else "keine verlässliche Abtastrate erkennbar"
                message = f"{total} geschätzte Kurvenpunkte automatisch erkannt (Achsen-OCR: {min(c['tick_count'] for c in calibrations.values())} y-Ticks); {cadence_text}. 15-Minuten-Werte sind interpolierte Schätzungen, keine Originalmessungen."
            except Exception as exc:
                message = "Digitalisierung fehlgeschlagen: " + str(exc)
        elif action == "upload_screenshots":
            files = request.files.getlist("screenshots")
            saved = 0
            for f in files:
                if not f or not f.filename: continue
                name = secure_filename(f.filename)
                if not name.lower().endswith((".png", ".jpg", ".jpeg", ".webp")): continue
                date_value = request.form.get("graph_date", "").strip()
                if date_value:
                    try: datetime.strptime(date_value, "%Y-%m-%d")
                    except ValueError: raise ValueError("Ungültiges Screenshot-Datum.")
                unique = secrets.token_hex(8) + "_" + name
                path = upload_dir / unique
                f.save(path)
                try:
                    from PIL import Image
                    with Image.open(path) as image:
                        if image.width * image.height > 20_000_000:
                            raise ValueError("Bild ist größer als 20 Megapixel.")
                        image.verify()
                except Exception:
                    path.unlink(missing_ok=True)
                    raise ValueError(f"{name}: ungültige oder nicht unterstützte Bilddatei.")
                with connect() as conn:
                    conn.execute("INSERT INTO graph_screenshots(filename,stored_path,graph_date) VALUES(?,?,?)", (name, str(path), date_value or None))
                saved += 1
            message = f"{saved} Screenshot(s) gespeichert. Bitte Datum und Achsenkalibrierung prüfen, bevor Werte übernommen werden."
        elif action in ("import_hp_csv", "import_weather_csv", "import_daily_csv"):
            f = request.files.get("csv_file")
            if not f or not f.filename:
                message = "Bitte eine CSV-Datei auswählen."
            else:
                try:
                    rows = csv.DictReader(f.stream.read().decode("utf-8-sig").splitlines())
                    count = 0
                    with connect() as conn:
                        for row in rows:
                            if action == "import_hp_csv":
                                ts = (row.get("timestamp") or row.get("datetime") or "").strip()
                                e = row.get("electricity_kwh", "").strip(); th = row.get("thermal_kwh", "").strip()
                                if not ts: continue
                                datetime.fromisoformat(ts)
                                ev = float(e) if e else None; tv = float(th) if th else None
                                if any(v is not None and (not __import__("math").isfinite(v) or v < 0) for v in (ev,tv)):
                                    raise ValueError("Energie-Werte müssen endlich und nicht-negativ sein.")
                                conn.execute("INSERT INTO hp_hourly(ts,electricity_kwh,thermal_kwh,source) VALUES(?,?,?,'csv') ON CONFLICT(ts) DO UPDATE SET electricity_kwh=excluded.electricity_kwh,thermal_kwh=excluded.thermal_kwh,source='csv'", (ts, ev, tv)); count += 1
                            elif action == "import_weather_csv":
                                ts = (row.get("timestamp") or row.get("datetime") or "").strip(); temp = row.get("temperature_c", row.get("temperature_2m", "")).strip()
                                if not ts or not temp: continue
                                datetime.fromisoformat(ts)
                                tv = float(temp)
                                if not __import__("math").isfinite(tv) or tv < -80 or tv > 60: raise ValueError("Ungültige Außentemperatur.")
                                conn.execute("INSERT INTO outdoor_hourly(ts,temperature_c,source) VALUES(?,?, 'csv') ON CONFLICT(ts) DO UPDATE SET temperature_c=excluded.temperature_c,source='csv'", (ts, tv)); count += 1
                            else:
                                day = (row.get("date") or "").strip(); val = row.get("kwh", row.get("consumption_kwh", "")).strip()
                                if not day or not val: continue
                                datetime.strptime(day, "%Y-%m-%d")
                                vv = float(val)
                                if not __import__("math").isfinite(vv) or vv < 0: raise ValueError("Tagesverbrauch muss endlich und nicht-negativ sein.")
                                conn.execute("INSERT INTO daily_non_hp_electricity(date,kwh,notes) VALUES(?,?,?) ON CONFLICT(date) DO UPDATE SET kwh=excluded.kwh,notes=excluded.notes", (day, vv, row.get("notes", ""))); count += 1
                    message = f"{count} Datensätze importiert."
                except Exception as exc:
                    message = "CSV konnte nicht importiert werden: " + str(exc)
    from db import connect
    with connect() as conn:
        hp = [dict(r) for r in conn.execute("SELECT * FROM hp_hourly ORDER BY ts DESC LIMIT 500")]
        weather = [dict(r) for r in conn.execute("SELECT * FROM outdoor_hourly ORDER BY ts DESC LIMIT 500")]
        daily = [dict(r) for r in conn.execute("SELECT * FROM daily_non_hp_electricity ORDER BY date DESC LIMIT 200")]
        shots = [dict(r) for r in conn.execute("SELECT * FROM graph_screenshots ORDER BY id DESC")]
        daily_hp = [dict(r) for r in conn.execute("SELECT * FROM daily_hp_totals ORDER BY date DESC LIMIT 200")]
        graph_points = [dict(r) for r in conn.execute("SELECT * FROM hp_graph_points ORDER BY ts DESC LIMIT 300")]
    from ocr_extract import parse_ocr_fields
    for shot in shots:
        shot["ocr_fields"] = parse_ocr_fields(shot.get("ocr_text") or "")
    return render_template("heatpump_analysis.html", hp=hp, weather=weather, daily=daily, shots=shots, daily_hp=daily_hp, graph_points=graph_points, message=message, **base_context())

@app.get("/statistik/waermepumpe/data")
def heatpump_data():
    from db import connect
    with connect() as conn:
        rows = conn.execute("""SELECT h.ts,h.electricity_kwh,h.thermal_kwh,w.temperature_c
          FROM hp_hourly h LEFT JOIN outdoor_hourly w ON w.ts=h.ts ORDER BY h.ts""").fetchall()
        estimates = conn.execute("""SELECT ts, MAX(electricity_kw) AS electricity_kw,
          MAX(thermal_kw) AS thermal_kw, MAX(confidence) AS confidence
          FROM hp_graph_points GROUP BY ts ORDER BY ts""").fetchall()
    return {"data": [dict(r) for r in rows], "estimates": [dict(r) for r in estimates]}


@app.get("/statistik/waermepumpe/daily")
def heatpump_daily_data():
    from db import connect
    with connect() as conn:
        # Confirmed screenshot totals take precedence over sums of hourly/imported data.
        rows = conn.execute("""
          SELECT dates.date,
            COALESCE(t.electricity_kwh, h.hp_kwh) AS hp_kwh,
            d.kwh AS non_hp_kwh,
            CASE WHEN COALESCE(t.electricity_kwh, h.hp_kwh) IS NOT NULL AND d.kwh IS NOT NULL
                 THEN COALESCE(t.electricity_kwh, h.hp_kwh) + d.kwh ELSE NULL END AS total_kwh,
            t.thermal_kwh AS thermal_kwh,
            CASE WHEN t.electricity_kwh IS NOT NULL THEN 'screenshot-confirmed'
                 WHEN h.hp_kwh IS NOT NULL THEN 'hourly-sum' ELSE NULL END AS hp_source
          FROM (
            SELECT date FROM daily_non_hp_electricity
            UNION SELECT date FROM daily_hp_totals
            UNION SELECT substr(ts,1,10) FROM hp_hourly
          ) dates
          LEFT JOIN daily_hp_totals t ON t.date=dates.date
          LEFT JOIN daily_non_hp_electricity d ON d.date=dates.date
          LEFT JOIN (SELECT substr(ts,1,10) AS date,SUM(electricity_kwh) AS hp_kwh
                     FROM hp_hourly GROUP BY substr(ts,1,10)) h ON h.date=dates.date
          ORDER BY dates.date
        """).fetchall()
    return {"data": [dict(r) for r in rows]}

@app.route("/statistik/regression/dataset", methods=["POST"])
def regression_dataset():
    import json
    person_id = request.json.get("person_id") or None
    if person_id:
        person_id = int(person_id)
    hours_after     = int(request.json.get("hours_after", 4))
    min_occurrences = int(request.json.get("min_occurrences", 2))
    exclude_empty   = bool(request.json.get("exclude_empty_days", True))
    result = build_regression_dataset(
        person_id=person_id, hours_after=hours_after,
        min_occurrences=min_occurrences, exclude_empty_days=exclude_empty)
    return json.dumps({
        "n_pain":       result["n_pain"],
        "n_no_pain":    result["n_no_pain"],
        "n_total":      len(result["dataset"]),
        "foods":        result["foods"],
        "hours_after":  result["hours_after"],
        "n_excluded":   result["n_excluded"],
        "excluded_foods": result["excluded_foods"][:10],
    }, ensure_ascii=False), 200, {"Content-Type": "application/json"}

@app.route("/statistik/regression/run", methods=["POST"])
def regression_run():
    import json
    person_id = request.json.get("person_id") or None
    if person_id:
        person_id = int(person_id)
    hours_after     = int(request.json.get("hours_after", 4))
    min_occurrences = int(request.json.get("min_occurrences", 2))
    exclude_empty   = bool(request.json.get("exclude_empty_days", True))
    dataset_info = build_regression_dataset(
        person_id=person_id, hours_after=hours_after,
        min_occurrences=min_occurrences, exclude_empty_days=exclude_empty)
    result = run_logistic_regression(dataset_info)
    return json.dumps(result, ensure_ascii=False), 200, {"Content-Type": "application/json"}

@app.route("/export/all.csv")
def export_all(): return csv_resp(export_entries_csv(), f"lindenprotokoll_all_{datetime.now(APP_TZ).strftime('%Y-%m-%d_%H-%M-%S')}.csv")
@app.route("/export/meal.csv")
def export_meal(): return csv_resp(export_meal_csv(), f"lindenprotokoll_meal_{datetime.now(APP_TZ).strftime('%Y-%m-%d_%H-%M-%S')}.csv")
@app.route("/export/illness.csv")
def export_illness(): return csv_resp(export_illness_csv(), f"lindenprotokoll_illness_{datetime.now(APP_TZ).strftime('%Y-%m-%d_%H-%M-%S')}.csv")
@app.route("/export/consumption.csv")
def export_consumption(): return csv_resp(export_consumption_csv(), f"lindenprotokoll_consumption_{datetime.now(APP_TZ).strftime('%Y-%m-%d_%H-%M-%S')}.csv")

@app.route("/verwaltung")
def verwaltung(): return render_template("admin_index.html", **base_context())

@app.route("/settings/security", methods=["GET", "POST"])
def security_settings():
    if request.method == "POST":
        current = request.form.get("current_pin", "")
        new_pin = request.form.get("new_pin", "")
        confirm = request.form.get("confirm_pin", "")
        stored = get_setting("security_pin_hash", "")
        if not stored or not check_password_hash(stored, current):
            flash("Aktuelle PIN ist nicht korrekt.", "error")
        elif not re.fullmatch(r"\d{6}", new_pin) or new_pin == "000000":
            flash("Die neue PIN muss sechs Ziffern haben und darf nicht 000000 sein.", "error")
        elif new_pin != confirm:
            flash("Die neuen PIN-Eingaben stimmen nicht überein.", "error")
        else:
            set_setting("security_pin_hash", generate_password_hash(new_pin))
            flash("PIN wurde geändert.", "success")
            return redirect(url_for("security_settings"))
    return render_template("security_settings.html", **base_context())

@app.route("/settings/diagnostics")
def system_diagnostics():
    """Report actual runtime availability of OCR and image dependencies."""
    import importlib.metadata
    import shutil
    import subprocess

    checks = []
    tesseract_path = shutil.which("tesseract")
    checks.append({"name": "Tesseract OCR", "ok": bool(tesseract_path),
                   "detail": tesseract_path or "Programm nicht gefunden"})
    languages = set()
    version = "Nicht verfügbar"
    if tesseract_path:
        try:
            v = subprocess.run([tesseract_path, "--version"], capture_output=True, text=True, timeout=8)
            version = (v.stdout or v.stderr).splitlines()[0][:160] if (v.stdout or v.stderr) else "Keine Versionsausgabe"
            r = subprocess.run([tesseract_path, "--list-langs"], capture_output=True, text=True, timeout=8)
            languages = set((r.stdout or "").splitlines()[1:])
        except Exception as exc:
            version = f"Prüfung fehlgeschlagen: {str(exc)[:120]}"
    checks.append({"name": "Tesseract-Version", "ok": version.startswith("tesseract "), "detail": version})
    for lang in ("deu", "eng"):
        checks.append({"name": f"Sprachdaten {lang}", "ok": lang in languages,
                       "detail": "Verfügbar" if lang in languages else "Nicht gefunden"})
    try:
        from PIL import Image
        import PIL
        checks.append({"name": "Pillow", "ok": True, "detail": f"Version {PIL.__version__}"})
    except Exception as exc:
        checks.append({"name": "Pillow", "ok": False, "detail": str(exc)[:160]})
    ocr_test = False
    ocr_detail = "Übersprungen: Tesseract oder Sprachdaten fehlen"
    if tesseract_path and "eng" in languages:
        try:
            from PIL import Image, ImageDraw
            from io import BytesIO
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
                im = Image.new("RGB", (420, 90), "white")
                ImageDraw.Draw(im).text((12, 24), "Lindenprotokoll OCR 2025-11-13", fill="black")
                im.save(tmp.name)
                result = subprocess.run([tesseract_path, tmp.name, "stdout", "-l", "eng"], capture_output=True, text=True, timeout=15)
                ocr_detail = (result.stdout or "").strip().replace("\n", " ")[:160]
                ocr_test = result.returncode == 0 and "Lindenprotokoll" in ocr_detail
        except Exception as exc:
            ocr_detail = f"Funktionstest fehlgeschlagen: {str(exc)[:120]}"
    checks.append({"name": "OCR-Funktionstest", "ok": ocr_test, "detail": ocr_detail})
    return render_template("system_diagnostics.html", checks=checks, all_ok=all(c["ok"] for c in checks), **base_context())

@app.route("/admin/duplicates", methods=["GET", "POST"])
def admin_duplicates():
    exact = request.args.get("exact", "1") != "0"
    if request.method == "POST":
        ids = request.form.getlist("entry_ids")
        deleted = 0
        for eid in ids:
            try:
                delete_entry(int(eid))
                deleted += 1
            except Exception:
                pass
        flash(f"{deleted} Eintrag/Einträge gelöscht.", "success")
        return redirect(url_for("admin_duplicates", exact="1" if exact else "0"))
    duplicates = find_duplicates(exact_content=exact)
    return render_template("admin_duplicates.html", duplicates=duplicates, exact=exact, **base_context())

@app.route("/verwaltung/personen", methods=["GET", "POST"])
def admin_people():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            ok, msg = add_person(request.form.get("name","")); flash(msg, "success" if ok else "error")
        elif action == "rename":
            ok, msg = rename_person(int(request.form["person_id"]), request.form.get("name","")); flash(msg, "success" if ok else "error")
        elif action == "deactivate":
            set_person_active(int(request.form["person_id"]), False); flash("Person deaktiviert.", "success")
        elif action == "reactivate":
            set_person_active(int(request.form["person_id"]), True); flash("Person reaktiviert.", "success")
        return redirect(url_for("admin_people"))
    return render_template("admin_people.html", active_people=get_people(True), inactive_people=[r for r in get_people(False) if r["is_active"] == 0], **base_context())

@app.route("/verwaltung/listen", methods=["GET", "POST"])
def admin_lists():
    if request.method == "POST":
        entity = request.form.get("entity"); action = request.form.get("action")
        if entity == "food":
            if action == "add":
                ok, msg = add_food_item(request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "rename":
                ok, msg = rename_food_item(int(request.form["item_id"]), request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "deactivate":
                set_food_item_active(int(request.form["item_id"]), False); flash("Essen deaktiviert.", "success")
            elif action == "reactivate":
                set_food_item_active(int(request.form["item_id"]), True); flash("Essen reaktiviert.", "success")
            elif action == "set_sort_mode":
                set_setting("food_sort_mode", "alpha" if request.form.get("sort_mode") == "alpha" else "usage"); flash("Sortierung gespeichert.", "success")
        if entity == "medication":
            if action == "add":
                ok, msg = add_medication(request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "rename":
                ok, msg = rename_medication(int(request.form["item_id"]), request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "deactivate":
                set_medication_active(int(request.form["item_id"]), False); flash("Medikament deaktiviert.", "success")
            elif action == "reactivate":
                set_medication_active(int(request.form["item_id"]), True); flash("Medikament reaktiviert.", "success")
        if entity == "symptom":
            if action == "add":
                ok, msg = add_symptom(request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "rename":
                ok, msg = rename_symptom(int(request.form["item_id"]), request.form.get("name","")); flash(msg, "success" if ok else "error")
            elif action == "deactivate":
                set_symptom_active(int(request.form["item_id"]), False); flash("Symptom deaktiviert.", "success")
            elif action == "reactivate":
                set_symptom_active(int(request.form["item_id"]), True); flash("Symptom reaktiviert.", "success")
            elif action == "set_sort_mode":
                set_setting("symptom_sort_mode", "alpha" if request.form.get("sort_mode") == "alpha" else "usage"); flash("Sortierung gespeichert.", "success")
        return redirect(url_for("admin_lists"))
    return render_template("admin_lists.html",
                           food_sort_mode=get_setting("food_sort_mode", "usage"),
                           active_food=get_food_items(True), inactive_food=[r for r in get_food_items(False) if r["is_active"] == 0],
                           active_medications=get_medications(True), inactive_medications=[r for r in get_medications(False) if r["is_active"] == 0],
                           symptom_sort_mode=get_setting("symptom_sort_mode", "usage"),
                           active_symptoms=get_symptoms(True), inactive_symptoms=[r for r in get_symptoms(False) if r["is_active"] == 0],
                           **base_context())

@app.route("/abdominal-image")
def abdominal_image():
    from config import DATA_DIR
    import mimetypes, os
    for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"]:
        p = DATA_DIR / ("abdominal_image" + ext)
        if p.exists():
            mime = mimetypes.guess_type(str(p))[0] or "image/png"
            return send_file(str(p), mimetype=mime)
    return "", 404

@app.route("/verwaltung/bauchschmerzen", methods=["GET", "POST"])
def admin_abdominal():
    from config import DATA_DIR
    import os
    upload_error = None
    if request.method == "POST":
        f = request.files.get("abdominal_image_file")
        if f and f.filename:
            ext = os.path.splitext(f.filename)[1].lower()
            if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp"):
                upload_error = "Nur Bildformate erlaubt (png, jpg, gif, webp)."
            else:
                for old_ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"]:
                    old_p = DATA_DIR / ("abdominal_image" + old_ext)
                    if old_p.exists():
                        old_p.unlink()
                save_path = DATA_DIR / ("abdominal_image" + ext)
                f.save(str(save_path))
                set_setting("abdominal_image_url", url_for("abdominal_image"))
                flash("Bild hochgeladen.", "success")
                return redirect(url_for("admin_abdominal"))
        else:
            set_setting("abdominal_image_url", request.form.get("abdominal_image_url", "").strip())
            flash("Gespeichert.", "success")
            return redirect(url_for("admin_abdominal"))
    has_upload = any(
        (DATA_DIR / ("abdominal_image" + ext)).exists()
        for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"]
    )
    return render_template("admin_abdominal.html",
        abdominal_image_url=get_setting("abdominal_image_url", ""),
        has_upload=has_upload,
        upload_error=upload_error,
        **base_context())

def _import_preview_dir():
    from config import DATA_DIR
    path = DATA_DIR / "import_previews"
    path.mkdir(parents=True, exist_ok=True)
    # Remove abandoned previews after 2 hours; previews are temporary, not user data.
    try:
        import time
        cutoff = time.time() - 2 * 60 * 60
        for item in path.glob("*.json"):
            try:
                if item.stat().st_mtime < cutoff:
                    item.unlink()
            except OSError:
                pass
    except OSError:
        pass
    return path


def _valid_import_preview_token(token):
    return isinstance(token, str) and re.fullmatch(r"[A-Za-z0-9_-]{32,64}", token)


def _load_import_preview(token=None):
    # The preview token is submitted as a hidden form field. Do not rely on
    # Flask's browser cookie to carry the reference between preview and commit.
    token = token or session.get("import_preview_id")
    if not _valid_import_preview_token(token):
        return {}
    path = _import_preview_dir() / (token + ".json")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("rows"), list):
            return {}
        return data
    except (OSError, ValueError, TypeError):
        return {}


def _clear_import_preview(token=None):
    token = token or session.get("import_preview_id")
    if session.get("import_preview_id") == token:
        session.pop("import_preview_id", None)
    if _valid_import_preview_token(token):
        try:
            (_import_preview_dir() / (token + ".json")).unlink(missing_ok=True)
        except OSError:
            pass


@app.route("/verwaltung/import", methods=["GET", "POST"])
def admin_import():
    preview_rows = None
    warnings = []
    errors = []
    preview_id = None
    kind = request.form.get("kind") if request.method == "POST" else request.args.get("kind", "electricity")
    mode = request.form.get("mode") if request.method == "POST" else None

    if request.method == "POST":
        if mode == "preview":
            upload = request.files.get("csv_file")
            if not upload or not upload.filename:
                flash("Bitte CSV-Datei auswählen.", "error")
            else:
                preview_rows, warnings, errors = parse_import_csv(kind, upload)
                _clear_import_preview()
                preview_id = secrets.token_urlsafe(32)
                preview_data = {"kind": kind, "rows": preview_rows, "warnings": warnings, "errors": errors}
                preview_path = _import_preview_dir() / (preview_id + ".json")
                temp_path = preview_path.with_suffix(".tmp")
                temp_path.write_text(json.dumps(preview_data, ensure_ascii=False), encoding="utf-8")
                os.replace(temp_path, preview_path)
                # Retain for page refresh convenience, but commit uses the hidden form token.
                session["import_preview_id"] = preview_id
                if not errors:
                    flash("Vorschau erstellt.", "success")
        elif mode == "commit":
            preview_id = request.form.get("preview_id", "")
            data = _load_import_preview(preview_id)
            if not data:
                flash("Import-Vorschau ist abgelaufen oder nicht mehr vorhanden. Bitte CSV erneut auswählen und Vorschau erstellen.", "error")
            elif data.get("kind") != kind:
                flash("Der Importtyp wurde geändert. Bitte für diesen Importtyp eine neue Vorschau erstellen.", "error")
            elif data.get("errors"):
                flash("Import enthält Fehler.", "error")
            else:
                count = import_rows(kind, data.get("rows", []))
                _clear_import_preview(preview_id)
                flash(f"Import abgeschlossen: {count} Zeilen importiert.", "success")
                return redirect(url_for("admin_import", kind=kind))
        elif mode != "preview":
            _clear_import_preview()
    else:
        preview_id = request.args.get("preview_id") or session.get("import_preview_id")
        data = _load_import_preview(preview_id)
        if data.get("kind") == kind:
            preview_rows = data.get("rows")
            warnings = data.get("warnings", [])
            errors = data.get("errors", [])

    return render_template(
        "admin_import.html",
        kind=kind,
        preview_rows=preview_rows,
        warnings=warnings,
        errors=errors,
        preview_id=preview_id,
        **base_context()
    )

@app.route("/verwaltung/import/template/<kind>.csv")
def import_template(kind):
    return csv_resp(template_csv(kind), f"vorlage_{kind}.csv")

@app.post("/undo")
def undo():
    entry_id = session.pop("undo_entry_id", None)
    if entry_id:
        delete_entry(int(entry_id))
        flash("Letzter Eintrag wurde gelöscht.", "success")
    else:
        flash("Kein Eintrag zum Rückgängig machen vorhanden.", "error")
    return redirect(url_for("index"))

@app.errorhandler(413)
def upload_too_large(_error):
    return "Datei zu groß. Maximale Uploadgröße: 15 MB.", 413

if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=DEBUG)
