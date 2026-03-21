from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

APP_TZ = ZoneInfo("Europe/Berlin")
from io import BytesIO
import json
from flask import Flask, flash, redirect, render_template, request, send_file, session, url_for

from config import DEBUG, HOST, PORT, SECRET_KEY
from db import *
from device import detect_device_type

app = Flask(__name__)
app.secret_key = SECRET_KEY

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

@app.route("/admin/duplicates", methods=["GET", "POST"])
def admin_duplicates():
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
        return redirect(url_for("admin_duplicates"))
    duplicates = find_duplicates()
    return render_template("admin_duplicates.html", duplicates=duplicates, **base_context())

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

@app.route("/verwaltung/bauchschmerzen", methods=["GET", "POST"])
def admin_abdominal():
    if request.method == "POST":
        set_setting("abdominal_image_url", request.form.get("abdominal_image_url","").strip())
        flash("Bild-URL gespeichert.", "success")
        return redirect(url_for("admin_abdominal"))
    return render_template("admin_abdominal.html", abdominal_image_url=get_setting("abdominal_image_url", ""), **base_context())

@app.route("/verwaltung/import", methods=["GET", "POST"])
def admin_import():
    preview_rows = None; warnings = []; errors = []; kind = request.form.get("kind") if request.method == "POST" else request.args.get("kind","electricity")
    if request.method == "POST":
        if request.form.get("mode") == "preview":
            upload = request.files.get("csv_file")
            if not upload or not upload.filename:
                flash("Bitte CSV-Datei auswählen.", "error")
            else:
                preview_rows, warnings, errors = parse_import_csv(kind, upload)
                session["import_preview"] = json.dumps({"kind": kind, "rows": preview_rows, "warnings": warnings, "errors": errors})
                if not errors:
                    flash("Vorschau erstellt.", "success")
        elif request.form.get("mode") == "commit":
            data = json.loads(session.get("import_preview", "{}") or "{}")
            if data.get("kind") != kind:
                flash("Import-Vorschau passt nicht mehr.", "error")
            elif data.get("errors"):
                flash("Import enthält Fehler.", "error")
            else:
                count = import_rows(kind, data.get("rows", []))
                session.pop("import_preview", None)
                flash(f"Import abgeschlossen: {count} Zeilen importiert.", "success")
                return redirect(url_for("admin_import", kind=kind))
    else:
        data = json.loads(session.get("import_preview", "{}") or "{}") if session.get("import_preview") else {}
        if data.get("kind") == kind:
            preview_rows = data.get("rows"); warnings = data.get("warnings", []); errors = data.get("errors", [])
    return render_template("admin_import.html", kind=kind, preview_rows=preview_rows, warnings=warnings, errors=errors, **base_context())

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

if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=DEBUG)
