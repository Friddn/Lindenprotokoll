from __future__ import annotations
import csv, io, sqlite3
from config import DB_PATH, DEFAULT_FOOD_ITEMS, DEFAULT_MEDICATIONS, DEFAULT_PERSONS, DEFAULT_SETTINGS, DEFAULT_SYMPTOMS

def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db() -> None:
    with connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS people (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            display_name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS food_items_master (
            food_id INTEGER PRIMARY KEY AUTOINCREMENT,
            food_name TEXT NOT NULL UNIQUE,
            usage_count INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS medications_master (
            medication_id INTEGER PRIMARY KEY AUTOINCREMENT,
            medication_name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS app_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS entries (
            entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            subtype TEXT NOT NULL,
            person_id INTEGER NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            device_type TEXT,
            user_agent_raw TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT,
            FOREIGN KEY(person_id) REFERENCES people(person_id)
        );
        CREATE TABLE IF NOT EXISTS meal_food_links (
            entry_id INTEGER NOT NULL,
            food_id INTEGER NOT NULL,
            PRIMARY KEY(entry_id, food_id),
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE,
            FOREIGN KEY(food_id) REFERENCES food_items_master(food_id)
        );
        CREATE TABLE IF NOT EXISTS illness_fever (
            entry_id INTEGER PRIMARY KEY,
            temperature_c REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS illness_abdominal_regions (
            entry_id INTEGER NOT NULL,
            region INTEGER NOT NULL,
            PRIMARY KEY(entry_id, region),
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS illness_abdominal_notes (
            entry_id INTEGER PRIMARY KEY,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS illness_medication_links (
            entry_id INTEGER NOT NULL,
            medication_id INTEGER NOT NULL,
            PRIMARY KEY(entry_id, medication_id),
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE,
            FOREIGN KEY(medication_id) REFERENCES medications_master(medication_id)
        );
        CREATE TABLE IF NOT EXISTS illness_medication_notes (
            entry_id INTEGER PRIMARY KEY,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS illness_other (
            entry_id INTEGER PRIMARY KEY,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS consumption_electricity (
            entry_id INTEGER PRIMARY KEY,
            consumption_meter_kwh INTEGER,
            feedin_meter_kwh INTEGER,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS consumption_water (
            entry_id INTEGER PRIMARY KEY,
            water_meter_m3 INTEGER NOT NULL,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS consumption_fuel (
            entry_id INTEGER PRIMARY KEY,
            vehicle TEXT NOT NULL,
            odometer_km INTEGER,
            total_price_eur REAL,
            liters REAL,
            price_per_liter REAL,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        """)
        # Migration: add missing columns to existing tables
        for tbl in ("consumption_electricity", "consumption_water", "consumption_fuel",
                    "illness_fever", "illness_other"):
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({tbl})")}
            if "notes" not in cols:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN notes TEXT")

        # Symptoms master list
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS symptoms_master (
            symptom_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symptom_name TEXT NOT NULL UNIQUE,
            usage_count INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS illness_symptom_links (
            entry_id INTEGER NOT NULL,
            symptom_id INTEGER NOT NULL,
            PRIMARY KEY(entry_id, symptom_id),
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE,
            FOREIGN KEY(symptom_id) REFERENCES symptoms_master(symptom_id)
        );
        CREATE TABLE IF NOT EXISTS illness_symptom_notes (
            entry_id INTEGER PRIMARY KEY,
            notes TEXT,
            FOREIGN KEY(entry_id) REFERENCES entries(entry_id) ON DELETE CASCADE
        );
        """)

        existing_people = {r["display_name"].lower(): r for r in conn.execute("SELECT * FROM people")}
        for name in DEFAULT_PERSONS:
            if name.lower() not in existing_people:
                conn.execute("INSERT INTO people(display_name, is_active) VALUES (?,1)", (name,))
        existing_food = {r["food_name"].lower(): r for r in conn.execute("SELECT * FROM food_items_master")}
        for name in DEFAULT_FOOD_ITEMS:
            if name.lower() not in existing_food:
                conn.execute("INSERT INTO food_items_master(food_name, usage_count, is_active) VALUES (?,0,1)", (name,))
        existing_med = {r["medication_name"].lower(): r for r in conn.execute("SELECT * FROM medications_master")}
        for name in DEFAULT_MEDICATIONS:
            if name.lower() not in existing_med:
                conn.execute("INSERT INTO medications_master(medication_name, is_active) VALUES (?,1)", (name,))
        existing_symptoms = {r["symptom_name"].lower(): r for r in conn.execute("SELECT * FROM symptoms_master")}
        for name in DEFAULT_SYMPTOMS:
            if name.lower() not in existing_symptoms:
                conn.execute("INSERT INTO symptoms_master(symptom_name, usage_count, is_active) VALUES (?,0,1)", (name,))
        existing_settings = {r["setting_key"] for r in conn.execute("SELECT setting_key FROM app_settings")}
        for k, v in DEFAULT_SETTINGS.items():
            if k not in existing_settings:
                conn.execute("INSERT INTO app_settings(setting_key, setting_value) VALUES (?,?)", (k, v))

def get_setting(key: str, fallback: str = "") -> str:
    with connect() as conn:
        row = conn.execute("SELECT setting_value FROM app_settings WHERE setting_key=?", (key,)).fetchone()
    return row["setting_value"] if row else fallback

def set_setting(key: str, value: str) -> None:
    with connect() as conn:
        conn.execute("""INSERT INTO app_settings(setting_key, setting_value)
                     VALUES (?, ?) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value""", (key, value))

def get_people(active_only=True):
    sql = "SELECT * FROM people"
    if active_only:
        sql += " WHERE is_active=1"
    sql += " ORDER BY display_name COLLATE NOCASE"
    with connect() as conn:
        return conn.execute(sql).fetchall()

def _reactivate_or_insert(table, id_field, name_field, name):
    cleaned = " ".join(name.split()).strip()
    if not cleaned:
        return False, "Bitte einen Namen eingeben."
    with connect() as conn:
        row = conn.execute(f"SELECT {id_field}, is_active FROM {table} WHERE lower({name_field})=lower(?)", (cleaned,)).fetchone()
        if row and row["is_active"] == 1:
            return False, "Eintrag existiert bereits."
        if row:
            conn.execute(f"UPDATE {table} SET {name_field}=?, is_active=1 WHERE {id_field}=?", (cleaned, row[id_field]))
            return True, "Eintrag reaktiviert."
        if table == "food_items_master":
            conn.execute(f"INSERT INTO {table}({name_field}, usage_count, is_active) VALUES (?,0,1)", (cleaned,))
        else:
            conn.execute(f"INSERT INTO {table}({name_field}, is_active) VALUES (?,1)", (cleaned,))
        return True, "Eintrag hinzugefügt."

def add_person(name): return _reactivate_or_insert("people", "person_id", "display_name", name)
def add_food_item(name): return _reactivate_or_insert("food_items_master", "food_id", "food_name", name)
def add_medication(name): return _reactivate_or_insert("medications_master", "medication_id", "medication_name", name)

def _rename(table, id_field, name_field, item_id, name):
    cleaned = " ".join(name.split()).strip()
    if not cleaned:
        return False, "Bitte einen Namen eingeben."
    with connect() as conn:
        dup = conn.execute(f"SELECT {id_field} FROM {table} WHERE lower({name_field})=lower(?) AND {id_field} != ?",
                           (cleaned, item_id)).fetchone()
        if dup:
            return False, "Name existiert bereits."
        conn.execute(f"UPDATE {table} SET {name_field}=? WHERE {id_field}=?", (cleaned, item_id))
    return True, "Umbenannt."

def rename_person(i, n): return _rename("people", "person_id", "display_name", i, n)
def rename_food_item(i, n): return _rename("food_items_master", "food_id", "food_name", i, n)
def rename_medication(i, n): return _rename("medications_master", "medication_id", "medication_name", i, n)

def _set_active(table, id_field, item_id, active):
    with connect() as conn:
        conn.execute(f"UPDATE {table} SET is_active=? WHERE {id_field}=?", (1 if active else 0, item_id))
def set_person_active(i, a): _set_active("people", "person_id", i, a)
def set_food_item_active(i, a): _set_active("food_items_master", "food_id", i, a)
def set_medication_active(i, a): _set_active("medications_master", "medication_id", i, a)

def get_food_items(active_only=True):
    sort_mode = get_setting("food_sort_mode", "usage")
    sql = "SELECT * FROM food_items_master"
    if active_only:
        sql += " WHERE is_active=1"
    if sort_mode == "alpha":
        sql += " ORDER BY food_name COLLATE NOCASE"
    else:
        sql += " ORDER BY usage_count DESC, food_name COLLATE NOCASE"
    with connect() as conn:
        return conn.execute(sql).fetchall()

def get_medications(active_only=True):
    sql = "SELECT * FROM medications_master"
    if active_only:
        sql += " WHERE is_active=1"
    sql += " ORDER BY medication_name COLLATE NOCASE"
    with connect() as conn:
        return conn.execute(sql).fetchall()

def get_last_person_id():
    with connect() as conn:
        row = conn.execute("SELECT person_id FROM entries WHERE person_id IS NOT NULL ORDER BY entry_id DESC LIMIT 1").fetchone()
    return int(row["person_id"]) if row else None

def create_entry(category, subtype, person_id, date, time, device_type, ua):
    with connect() as conn:
        cur = conn.execute("INSERT INTO entries(category, subtype, person_id, date, time, device_type, user_agent_raw) VALUES (?,?,?,?,?,?,?)",
                           (category, subtype, person_id, date, time, device_type, ua))
        return int(cur.lastrowid)

def save_meal(person_id, date, time, food_ids, device_type, ua):
    if not food_ids:
        with connect() as conn:
            row = conn.execute("SELECT food_id FROM food_items_master WHERE lower(food_name)='kein essen'").fetchone()
            if row:
                food_ids = [int(row["food_id"])]
    entry_id = create_entry("meal", "meal", person_id, date, time, device_type, ua)
    with connect() as conn:
        for food_id in set(food_ids):
            conn.execute("INSERT OR IGNORE INTO meal_food_links(entry_id, food_id) VALUES (?,?)", (entry_id, food_id))
            conn.execute("UPDATE food_items_master SET usage_count = usage_count + 1 WHERE food_id=?", (food_id,))
    return entry_id

def update_meal(entry_id, person_id, date, time, food_ids):
    if not food_ids:
        with connect() as conn:
            row = conn.execute("SELECT food_id FROM food_items_master WHERE lower(food_name)='kein essen'").fetchone()
            if row:
                food_ids = [int(row["food_id"])]
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("DELETE FROM meal_food_links WHERE entry_id=?", (entry_id,))
        for food_id in set(food_ids):
            conn.execute("INSERT OR IGNORE INTO meal_food_links(entry_id, food_id) VALUES (?,?)", (entry_id, food_id))
            conn.execute("UPDATE food_items_master SET usage_count = usage_count + 1 WHERE food_id=?", (food_id,))

def save_abdominal_pain(person_id, date, time, regions, notes, device_type, ua):
    entry_id = create_entry("illness", "abdominal_pain", person_id, date, time, device_type, ua)
    with connect() as conn:
        for region in sorted(set(regions)):
            conn.execute("INSERT INTO illness_abdominal_regions(entry_id, region) VALUES (?,?)", (entry_id, region))
        conn.execute("INSERT INTO illness_abdominal_notes(entry_id, notes) VALUES (?,?)", (entry_id, notes or None))
    return entry_id

def update_abdominal_pain(entry_id, person_id, date, time, regions, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("DELETE FROM illness_abdominal_regions WHERE entry_id=?", (entry_id,))
        for region in sorted(set(regions)):
            conn.execute("INSERT INTO illness_abdominal_regions(entry_id, region) VALUES (?,?)", (entry_id, region))
        conn.execute("UPDATE illness_abdominal_notes SET notes=? WHERE entry_id=?", (notes or None, entry_id))

def save_fever(person_id, date, time, temperature_c, notes, device_type, ua):
    entry_id = create_entry("illness", "fever", person_id, date, time, device_type, ua)
    with connect() as conn:
        conn.execute("INSERT INTO illness_fever(entry_id, temperature_c, notes) VALUES (?,?,?)", (entry_id, temperature_c, notes or None))
    return entry_id

def update_fever(entry_id, person_id, date, time, temperature_c, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("UPDATE illness_fever SET temperature_c=?, notes=? WHERE entry_id=?", (temperature_c, notes or None, entry_id))

def save_medications(person_id, date, time, medication_ids, notes, device_type, ua):
    entry_id = create_entry("illness", "medication", person_id, date, time, device_type, ua)
    with connect() as conn:
        for med_id in sorted(set(medication_ids)):
            conn.execute("INSERT INTO illness_medication_links(entry_id, medication_id) VALUES (?,?)", (entry_id, med_id))
        conn.execute("INSERT INTO illness_medication_notes(entry_id, notes) VALUES (?,?)", (entry_id, notes or None))
    return entry_id

def update_medications(entry_id, person_id, date, time, medication_ids, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("DELETE FROM illness_medication_links WHERE entry_id=?", (entry_id,))
        for med_id in sorted(set(medication_ids)):
            conn.execute("INSERT INTO illness_medication_links(entry_id, medication_id) VALUES (?,?)", (entry_id, med_id))
        conn.execute("UPDATE illness_medication_notes SET notes=? WHERE entry_id=?", (notes or None, entry_id))

def add_symptom(name): return _reactivate_or_insert("symptoms_master", "symptom_id", "symptom_name", name)
def rename_symptom(i, n): return _rename("symptoms_master", "symptom_id", "symptom_name", i, n)
def set_symptom_active(i, a): _set_active("symptoms_master", "symptom_id", i, a)

def get_symptoms(active_only=True):
    sort_mode = get_setting("symptom_sort_mode", "usage")
    sql = "SELECT * FROM symptoms_master"
    if active_only:
        sql += " WHERE is_active=1"
    if sort_mode == "alpha":
        sql += " ORDER BY symptom_name COLLATE NOCASE"
    else:
        sql += " ORDER BY usage_count DESC, symptom_name COLLATE NOCASE"
    with connect() as conn:
        return conn.execute(sql).fetchall()

def save_symptoms(person_id, date, time, symptom_ids, notes, device_type, ua):
    entry_id = create_entry("illness", "symptoms", person_id, date, time, device_type, ua)
    with connect() as conn:
        for sid in sorted(set(symptom_ids)):
            conn.execute("INSERT INTO illness_symptom_links(entry_id, symptom_id) VALUES (?,?)", (entry_id, sid))
            conn.execute("UPDATE symptoms_master SET usage_count = usage_count + 1 WHERE symptom_id=?", (sid,))
        conn.execute("INSERT INTO illness_symptom_notes(entry_id, notes) VALUES (?,?)", (entry_id, notes or None))
    return entry_id

def update_symptoms(entry_id, person_id, date, time, symptom_ids, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("DELETE FROM illness_symptom_links WHERE entry_id=?", (entry_id,))
        for sid in sorted(set(symptom_ids)):
            conn.execute("INSERT INTO illness_symptom_links(entry_id, symptom_id) VALUES (?,?)", (entry_id, sid))
            conn.execute("UPDATE symptoms_master SET usage_count = usage_count + 1 WHERE symptom_id=?", (sid,))
        conn.execute("UPDATE illness_symptom_notes SET notes=? WHERE entry_id=?", (notes or None, entry_id))

def save_other_illness(person_id, date, time, notes, device_type, ua):
    entry_id = create_entry("illness", "other", person_id, date, time, device_type, ua)
    with connect() as conn:
        conn.execute("INSERT INTO illness_other(entry_id, notes) VALUES (?,?)", (entry_id, notes or None))
    return entry_id

def update_other_illness(entry_id, person_id, date, time, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET person_id=?, date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?",
                     (person_id, date, time, entry_id))
        conn.execute("UPDATE illness_other SET notes=? WHERE entry_id=?", (notes or None, entry_id))

def save_electricity(date, time, consumption, feedin, notes, device_type, ua):
    entry_id = create_entry("consumption", "electricity", None, date, time, device_type, ua)
    with connect() as conn:
        conn.execute("INSERT INTO consumption_electricity(entry_id, consumption_meter_kwh, feedin_meter_kwh, notes) VALUES (?,?,?,?)",
                     (entry_id, consumption, feedin, notes or None))
    return entry_id

def update_electricity(entry_id, date, time, consumption, feedin, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?", (date, time, entry_id))
        conn.execute("UPDATE consumption_electricity SET consumption_meter_kwh=?, feedin_meter_kwh=?, notes=? WHERE entry_id=?",
                     (consumption, feedin, notes or None, entry_id))

def save_water(date, time, water_m3, notes, device_type, ua):
    entry_id = create_entry("consumption", "water", None, date, time, device_type, ua)
    with connect() as conn:
        conn.execute("INSERT INTO consumption_water(entry_id, water_meter_m3, notes) VALUES (?,?,?)", (entry_id, water_m3, notes or None))
    return entry_id

def update_water(entry_id, date, time, water_m3, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?", (date, time, entry_id))
        conn.execute("UPDATE consumption_water SET water_meter_m3=?, notes=? WHERE entry_id=?", (water_m3, notes or None, entry_id))

def save_fuel(date, time, vehicle, odometer_km, total_price_eur, liters, price_per_liter, notes, device_type, ua):
    entry_id = create_entry("consumption", "fuel", None, date, time, device_type, ua)
    with connect() as conn:
        conn.execute("""INSERT INTO consumption_fuel(entry_id, vehicle, odometer_km, total_price_eur, liters, price_per_liter, notes)
                     VALUES (?,?,?,?,?,?,?)""",
                     (entry_id, vehicle, odometer_km, total_price_eur, liters, price_per_liter, notes or None))
    return entry_id

def update_fuel(entry_id, date, time, vehicle, odometer_km, total_price_eur, liters, price_per_liter, notes):
    with connect() as conn:
        conn.execute("UPDATE entries SET date=?, time=?, updated_at=CURRENT_TIMESTAMP WHERE entry_id=?", (date, time, entry_id))
        conn.execute("""UPDATE consumption_fuel SET vehicle=?, odometer_km=?, total_price_eur=?, liters=?, price_per_liter=?, notes=?
                     WHERE entry_id=?""",
                     (vehicle, odometer_km, total_price_eur, liters, price_per_liter, notes or None, entry_id))

def entry_summary_row(entry):
    subtype_labels = {
        "meal": "Essen",
        "abdominal_pain": "Bauchschmerzen",
        "fever": "Fieber",
        "medication": "Medikamente",
        "symptoms": "Symptome",
        "other": "Anderes",
        "electricity": "Strom",
        "water": "Wasser",
        "fuel": "Auto",
    }
    label = subtype_labels.get(entry["subtype"], entry["subtype"])
    extra = ""
    if entry["subtype"] == "fever":
        extra = f'{entry["temperature_c"]:.1f} °C'
    elif entry["subtype"] == "water" and entry["water_meter_m3"] is not None:
        extra = str(entry["water_meter_m3"])
    elif entry["subtype"] == "fuel" and entry["odometer_km"] is not None:
        extra = f'{entry["odometer_km"]} km'
    elif entry["subtype"] == "electricity":
        parts = []
        if entry["consumption_meter_kwh"] is not None:
            parts.append(f'V {entry["consumption_meter_kwh"]}')
        if entry["feedin_meter_kwh"] is not None:
            parts.append(f'E {entry["feedin_meter_kwh"]}')
        extra = " | ".join(parts)
    return {"label": label, "extra": extra}

def get_history_entries(person_id=None, subtype=None, period=None):
    where = []
    params = []
    if person_id:
        where.append("e.person_id = ?")
        params.append(person_id)
    if subtype:
        where.append("e.subtype = ?")
        params.append(subtype)
    if period == "today":
        where.append("e.date = date('now', 'localtime')")
    elif period == "7d":
        where.append("e.date >= date('now', '-6 day', 'localtime')")
    elif period == "30d":
        where.append("e.date >= date('now', '-29 day', 'localtime')")
    sql = """
    SELECT e.entry_id, e.category, e.subtype, e.date, e.time, e.created_at, e.updated_at,
           p.display_name AS person_name,
           f.temperature_c, f.notes AS fever_notes,
           w.water_meter_m3, w.notes AS water_notes,
           c.consumption_meter_kwh, c.feedin_meter_kwh, c.notes AS electricity_notes,
           fu.vehicle, fu.odometer_km, fu.total_price_eur, fu.liters, fu.price_per_liter, fu.notes AS fuel_notes
    FROM entries e
    LEFT JOIN people p ON e.person_id = p.person_id
    LEFT JOIN illness_fever f ON e.entry_id = f.entry_id
    LEFT JOIN consumption_water w ON e.entry_id = w.entry_id
    LEFT JOIN consumption_electricity c ON e.entry_id = c.entry_id
    LEFT JOIN consumption_fuel fu ON e.entry_id = fu.entry_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY e.date DESC, e.time DESC, e.entry_id DESC"
    with connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        for r in rows:
            r["summary"] = entry_summary_row(r)
    return rows

def get_entry_details(entry_id):
    with connect() as conn:
        e = conn.execute("""
            SELECT e.*, p.display_name AS person_name
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id
            WHERE e.entry_id=?""", (entry_id,)).fetchone()
        if not e:
            return None
        e = dict(e)
        subtype = e["subtype"]
        details = {"entry": e, "foods": [], "regions": [], "medications": []}
        if subtype == "meal":
            details["foods"] = [r["food_name"] for r in conn.execute("""SELECT f.food_name FROM meal_food_links l
                JOIN food_items_master f ON l.food_id=f.food_id WHERE l.entry_id=? ORDER BY f.food_name COLLATE NOCASE""", (entry_id,))]
        elif subtype == "abdominal_pain":
            details["regions"] = [r["region"] for r in conn.execute("SELECT region FROM illness_abdominal_regions WHERE entry_id=? ORDER BY region", (entry_id,))]
            row = conn.execute("SELECT notes FROM illness_abdominal_notes WHERE entry_id=?", (entry_id,)).fetchone()
            details["notes"] = row["notes"] if row else ""
        elif subtype == "fever":
            row = conn.execute("SELECT temperature_c, notes FROM illness_fever WHERE entry_id=?", (entry_id,)).fetchone()
            details["temperature_c"] = row["temperature_c"]; details["notes"] = row["notes"] if row else ""
        elif subtype == "medication":
            details["medications"] = [r["medication_id"] for r in conn.execute("SELECT medication_id FROM illness_medication_links WHERE entry_id=?", (entry_id,))]
            details["medication_names"] = [r["medication_name"] for r in conn.execute("""SELECT m.medication_name FROM illness_medication_links l
                JOIN medications_master m ON l.medication_id=m.medication_id WHERE l.entry_id=? ORDER BY m.medication_name COLLATE NOCASE""", (entry_id,))]
            row = conn.execute("SELECT notes FROM illness_medication_notes WHERE entry_id=?", (entry_id,)).fetchone()
            details["notes"] = row["notes"] if row else ""
        elif subtype == "symptoms":
            details["symptoms"] = [r["symptom_id"] for r in conn.execute("SELECT symptom_id FROM illness_symptom_links WHERE entry_id=?", (entry_id,))]
            details["symptom_names"] = [r["symptom_name"] for r in conn.execute("""SELECT s.symptom_name FROM illness_symptom_links l
                JOIN symptoms_master s ON l.symptom_id=s.symptom_id WHERE l.entry_id=? ORDER BY s.symptom_name COLLATE NOCASE""", (entry_id,))]
            row = conn.execute("SELECT notes FROM illness_symptom_notes WHERE entry_id=?", (entry_id,)).fetchone()
            details["notes"] = row["notes"] if row else ""
        elif subtype == "other":
            row = conn.execute("SELECT notes FROM illness_other WHERE entry_id=?", (entry_id,)).fetchone()
            details["notes"] = row["notes"] if row else ""
        elif subtype == "electricity":
            row = conn.execute("SELECT * FROM consumption_electricity WHERE entry_id=?", (entry_id,)).fetchone()
            details.update(dict(row))
        elif subtype == "water":
            row = conn.execute("SELECT * FROM consumption_water WHERE entry_id=?", (entry_id,)).fetchone()
            details.update(dict(row))
        elif subtype == "fuel":
            row = conn.execute("SELECT * FROM consumption_fuel WHERE entry_id=?", (entry_id,)).fetchone()
            details.update(dict(row))
    return details

def delete_entry(entry_id):
    with connect() as conn:
        conn.execute("DELETE FROM entries WHERE entry_id=?", (entry_id,))

def _csv_string(rows, fieldnames):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow(row)
    return buf.getvalue()

def export_entries_csv():
    with connect() as conn:
        rows = conn.execute("""SELECT e.entry_id, e.category, e.subtype, p.display_name AS person, e.date, e.time,
            e.device_type, e.user_agent_raw, e.created_at, e.updated_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id ORDER BY e.date, e.time, e.entry_id""").fetchall()
    return _csv_string([dict(r) for r in rows], ["entry_id","category","subtype","person","date","time","device_type","user_agent_raw","created_at","updated_at"])

def export_meal_csv():
    with connect() as conn:
        rows = conn.execute("""SELECT e.entry_id, p.display_name AS person, e.date, e.time,
            group_concat(f.food_name, ' | ') AS foods, e.device_type, e.created_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id
            LEFT JOIN meal_food_links l ON e.entry_id=l.entry_id
            LEFT JOIN food_items_master f ON l.food_id=f.food_id
            WHERE e.category='meal'
            GROUP BY e.entry_id ORDER BY e.date, e.time, e.entry_id""").fetchall()
    return _csv_string([dict(r) for r in rows], ["entry_id","person","date","time","foods","device_type","created_at"])

def export_illness_csv():
    rows = []
    with connect() as conn:
        for r in conn.execute("""SELECT e.entry_id, e.subtype, p.display_name AS person, e.date, e.time,
            f.temperature_c, '' AS regions, '' AS medications, f.notes AS notes, e.device_type, e.created_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id JOIN illness_fever f ON e.entry_id=f.entry_id"""):
            rows.append(dict(r))
        for r in conn.execute("""SELECT e.entry_id, e.subtype, p.display_name AS person, e.date, e.time,
            '' AS temperature_c, group_concat(r.region, ' | ') AS regions, '' AS medications, n.notes AS notes, e.device_type, e.created_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id
            JOIN illness_abdominal_notes n ON e.entry_id=n.entry_id
            LEFT JOIN illness_abdominal_regions r ON e.entry_id=r.entry_id
            WHERE e.subtype='abdominal_pain' GROUP BY e.entry_id"""):
            rows.append(dict(r))
        for r in conn.execute("""SELECT e.entry_id, e.subtype, p.display_name AS person, e.date, e.time,
            '' AS temperature_c, '' AS regions, group_concat(m.medication_name, ' | ') AS medications, n.notes AS notes, e.device_type, e.created_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id
            JOIN illness_medication_notes n ON e.entry_id=n.entry_id
            LEFT JOIN illness_medication_links l ON e.entry_id=l.entry_id
            LEFT JOIN medications_master m ON l.medication_id=m.medication_id
            WHERE e.subtype='medication' GROUP BY e.entry_id"""):
            rows.append(dict(r))
        for r in conn.execute("""SELECT e.entry_id, e.subtype, p.display_name AS person, e.date, e.time,
            '' AS temperature_c, '' AS regions, '' AS medications, o.notes AS notes, e.device_type, e.created_at
            FROM entries e LEFT JOIN people p ON e.person_id=p.person_id JOIN illness_other o ON e.entry_id=o.entry_id"""):
            rows.append(dict(r))
    rows.sort(key=lambda r:(r["date"], r["time"], r["entry_id"]))
    return _csv_string(rows, ["entry_id","subtype","person","date","time","temperature_c","regions","medications","notes","device_type","created_at"])

def export_consumption_csv():
    rows = []
    with connect() as conn:
        for r in conn.execute("""SELECT e.entry_id, e.subtype, e.date, e.time, c.consumption_meter_kwh, c.feedin_meter_kwh,
            '' AS water_meter_m3, '' AS vehicle, '' AS odometer_km, '' AS total_price_eur, '' AS liters, '' AS price_per_liter,
            c.notes, e.device_type, e.created_at
            FROM entries e JOIN consumption_electricity c ON e.entry_id=c.entry_id"""):
            rows.append(dict(r))
        for r in conn.execute("""SELECT e.entry_id, e.subtype, e.date, e.time, '' AS consumption_meter_kwh, '' AS feedin_meter_kwh,
            w.water_meter_m3, '' AS vehicle, '' AS odometer_km, '' AS total_price_eur, '' AS liters, '' AS price_per_liter,
            w.notes, e.device_type, e.created_at
            FROM entries e JOIN consumption_water w ON e.entry_id=w.entry_id"""):
            rows.append(dict(r))
        for r in conn.execute("""SELECT e.entry_id, e.subtype, e.date, e.time, '' AS consumption_meter_kwh, '' AS feedin_meter_kwh,
            '' AS water_meter_m3, f.vehicle, f.odometer_km, f.total_price_eur, f.liters, f.price_per_liter,
            f.notes, e.device_type, e.created_at
            FROM entries e JOIN consumption_fuel f ON e.entry_id=f.entry_id"""):
            rows.append(dict(r))
    rows.sort(key=lambda r:(r["date"], r["time"], r["entry_id"]))
    return _csv_string(rows, ["entry_id","subtype","date","time","consumption_meter_kwh","feedin_meter_kwh","water_meter_m3","vehicle","odometer_km","total_price_eur","liters","price_per_liter","notes","device_type","created_at"])

def template_csv(kind):
    if kind == "electricity":
        return "date,time,consumption_meter_kwh,feedin_meter_kwh,notes\n2024-01-15,18:30,12345,678,Ablesung\n"
    if kind == "water":
        return "date,time,water_meter_m3,notes\n2024-01-15,18:30,5432,Ablesung\n"
    if kind == "fuel":
        return "date,time,vehicle,odometer_km,total_price_eur,liters,price_per_liter,notes\n2024-01-10,17:20,Kangoo,63420,74.20,41.00,1.810,Tankstelle XY\n"
    if kind == "meal":
        return "person,date,time,foods\nClara,2024-01-15,08:00,Brot | Butter | Apfel\n"
    if kind == "illness":
        return "subtype,person,date,time,temperature_c,medications,notes\nabdominal_pain,Clara,2024-01-15,18:00,,,Bauchschmerzen nach dem Essen\nfever,Clara,2024-01-16,08:00,38.5,,\nmedication,Clara,2024-01-16,09:00,,Ibuprofen | Paracetamol,\nother,Clara,2024-01-16,18:00,,,Allgemeines Unwohlsein\n"
    return ""

def _get_or_create_person(conn, name):
    cleaned = " ".join(name.split()).strip()
    if not cleaned:
        return None
    row = conn.execute("SELECT person_id FROM people WHERE lower(display_name)=lower(?)", (cleaned,)).fetchone()
    if row:
        return row["person_id"]
    cur = conn.execute("INSERT INTO people(display_name, is_active) VALUES (?,1)", (cleaned,))
    return cur.lastrowid

def _get_or_create_food(conn, name):
    cleaned = " ".join(name.split()).strip()
    if not cleaned:
        return None
    row = conn.execute("SELECT food_id FROM food_items_master WHERE lower(food_name)=lower(?)", (cleaned,)).fetchone()
    if row:
        return row["food_id"]
    cur = conn.execute("INSERT INTO food_items_master(food_name, usage_count, is_active) VALUES (?,0,1)", (cleaned,))
    return cur.lastrowid

def _get_or_create_medication(conn, name):
    cleaned = " ".join(name.split()).strip()
    if not cleaned:
        return None
    row = conn.execute("SELECT medication_id FROM medications_master WHERE lower(medication_name)=lower(?)", (cleaned,)).fetchone()
    if row:
        return row["medication_id"]
    cur = conn.execute("INSERT INTO medications_master(medication_name, is_active) VALUES (?,1)", (cleaned,))
    return cur.lastrowid

def parse_import_csv(kind, file_storage):
    text = file_storage.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows, warnings, errors = [], [], []

    def parse_int(v):
        v = (v or "").strip()
        return int(v) if v else None

    def parse_float(v):
        v = (v or "").strip().replace(",", ".")
        return float(v) if v else None

    for idx, row in enumerate(reader, start=2):
        if not any((v or "").strip() for v in row.values()):
            continue
        try:
            date = (row.get("date") or "").strip()
            time = (row.get("time") or "12:00").strip() or "12:00"
            if not date:
                raise ValueError("Datum fehlt")
            item = {"date": date, "time": time}

            if kind == "meal":
                person = (row.get("person") or "").strip()
                if not person:
                    raise ValueError("Person fehlt")
                foods_raw = (row.get("foods") or "").strip()
                food_names = [f.strip() for f in foods_raw.split("|") if f.strip()] if foods_raw else []
                item["person"] = person
                item["food_names"] = food_names
                # duplicate check
                with connect() as conn:
                    pid = conn.execute("SELECT person_id FROM people WHERE lower(display_name)=lower(?)", (person,)).fetchone()
                    if pid:
                        dup = conn.execute("""SELECT e.entry_id FROM entries e
                            WHERE e.subtype='meal' AND e.person_id=? AND e.date=? AND e.time=?""",
                            (pid["person_id"], date, time)).fetchone()
                        if dup:
                            warnings.append(f"Zeile {idx}: mögliches Duplikat ({person} {date} {time})")

            elif kind == "illness":
                subtype = (row.get("subtype") or "").strip()
                person = (row.get("person") or "").strip()
                if not subtype:
                    raise ValueError("Subtyp fehlt")
                if subtype not in ("abdominal_pain", "fever", "medication", "other", "symptoms"):
                    raise ValueError(f"Unbekannter Subtyp: {subtype} (übersprungen)")
                if not person:
                    raise ValueError("Person fehlt")
                item["subtype"] = subtype
                item["person"] = person
                item["notes"] = (row.get("notes") or "").strip()
                if subtype == "fever":
                    temp = parse_float(row.get("temperature_c"))
                    if temp is None:
                        raise ValueError("Temperatur fehlt")
                    if temp < 34 or temp > 44:
                        raise ValueError(f"Temperatur {temp} außerhalb 34–44")
                    item["temperature_c"] = round(temp, 1)
                elif subtype == "medication":
                    meds_raw = (row.get("medications") or "").strip()
                    med_names = [m.strip() for m in meds_raw.split("|") if m.strip()] if meds_raw else []
                    if not med_names:
                        raise ValueError("Medikamente fehlen")
                    item["medication_names"] = med_names
                elif subtype == "symptoms":
                    syms_raw = (row.get("symptoms") or "").strip()
                    sym_names = [s.strip() for s in syms_raw.split("|") if s.strip()] if syms_raw else []
                    if not sym_names:
                        raise ValueError("Symptome fehlen")
                    item["symptom_names"] = sym_names

            elif kind == "electricity":
                item["consumption_meter_kwh"] = parse_int(row.get("consumption_meter_kwh"))
                item["feedin_meter_kwh"] = parse_int(row.get("feedin_meter_kwh"))
                item["notes"] = (row.get("notes") or "").strip()
                if item["consumption_meter_kwh"] is None and item["feedin_meter_kwh"] is None:
                    raise ValueError("mindestens ein Zählerstand fehlt")
                with connect() as conn:
                    dup = conn.execute("""SELECT e.entry_id FROM entries e JOIN consumption_electricity c ON e.entry_id=c.entry_id
                        WHERE e.subtype='electricity' AND e.date=? AND e.time=?
                        AND ifnull(c.consumption_meter_kwh,-1)=ifnull(?, -1)
                        AND ifnull(c.feedin_meter_kwh,-1)=ifnull(?, -1)""",
                        (date, time, item["consumption_meter_kwh"], item["feedin_meter_kwh"])).fetchone()
                    if dup:
                        warnings.append(f"Zeile {idx}: mögliches Duplikat")

            elif kind == "water":
                item["water_meter_m3"] = parse_int(row.get("water_meter_m3"))
                item["notes"] = (row.get("notes") or "").strip()
                if item["water_meter_m3"] is None:
                    raise ValueError("Wasser Zählerstand fehlt")
                with connect() as conn:
                    dup = conn.execute("""SELECT e.entry_id FROM entries e JOIN consumption_water w ON e.entry_id=w.entry_id
                        WHERE e.subtype='water' AND e.date=? AND e.time=? AND w.water_meter_m3=?""",
                        (date, time, item["water_meter_m3"])).fetchone()
                    if dup:
                        warnings.append(f"Zeile {idx}: mögliches Duplikat")

            elif kind == "fuel":
                item["vehicle"] = (row.get("vehicle") or "Kangoo").strip() or "Kangoo"
                item["odometer_km"] = parse_int(row.get("odometer_km"))
                item["total_price_eur"] = parse_float(row.get("total_price_eur"))
                item["liters"] = parse_float(row.get("liters"))
                item["price_per_liter"] = parse_float(row.get("price_per_liter"))
                item["notes"] = (row.get("notes") or "").strip()
                if all(v is None for v in [item["odometer_km"], item["total_price_eur"], item["liters"], item["price_per_liter"]]):
                    raise ValueError("alle Werte leer")
                with connect() as conn:
                    dup = conn.execute("""SELECT e.entry_id FROM entries e JOIN consumption_fuel f ON e.entry_id=f.entry_id
                        WHERE e.subtype='fuel' AND e.date=? AND e.time=? AND ifnull(f.odometer_km,-1)=ifnull(?, -1)""",
                        (date, time, item["odometer_km"])).fetchone()
                    if dup:
                        warnings.append(f"Zeile {idx}: mögliches Duplikat")

            rows.append(item)
        except Exception as ex:
            errors.append(f"Zeile {idx}: {ex}")
    return rows, warnings, errors

def import_rows(kind, rows, device_type="import", ua="csv import"):
    created = 0
    for row in rows:
        if kind == "meal":
            with connect() as conn:
                pid = _get_or_create_person(conn, row["person"])
                food_ids = []
                for fname in row["food_names"]:
                    fid = _get_or_create_food(conn, fname)
                    if fid:
                        food_ids.append(fid)
            save_meal(pid, row["date"], row["time"], food_ids, device_type, ua)
        elif kind == "illness":
            subtype = row["subtype"]
            with connect() as conn:
                pid = _get_or_create_person(conn, row["person"])
            notes = row.get("notes", "")
            if subtype == "abdominal_pain":
                save_abdominal_pain(pid, row["date"], row["time"], [], notes, device_type, ua)
            elif subtype == "fever":
                save_fever(pid, row["date"], row["time"], row["temperature_c"], notes, device_type, ua)
            elif subtype == "medication":
                with connect() as conn:
                    med_ids = [_get_or_create_medication(conn, m) for m in row["medication_names"]]
                    med_ids = [m for m in med_ids if m]
                save_medications(pid, row["date"], row["time"], med_ids, notes, device_type, ua)
            elif subtype == "symptoms":
                with connect() as conn:
                    sym_ids = []
                    for sname in row.get("symptom_names", []):
                        sname = sname.strip()
                        if not sname:
                            continue
                        r2 = conn.execute("SELECT symptom_id FROM symptoms_master WHERE lower(symptom_name)=lower(?)", (sname,)).fetchone()
                        if r2:
                            sym_ids.append(r2["symptom_id"])
                        else:
                            cur = conn.execute("INSERT INTO symptoms_master(symptom_name, usage_count, is_active) VALUES (?,0,1)", (sname,))
                            sym_ids.append(cur.lastrowid)
                save_symptoms(pid, row["date"], row["time"], sym_ids, notes, device_type, ua)
            elif subtype == "other":
                save_other_illness(pid, row["date"], row["time"], notes, device_type, ua)
        elif kind == "electricity":
            save_electricity(row["date"], row["time"], row["consumption_meter_kwh"], row["feedin_meter_kwh"], row["notes"], device_type, ua)
        elif kind == "water":
            save_water(row["date"], row["time"], row["water_meter_m3"], row["notes"], device_type, ua)
        elif kind == "fuel":
            save_fuel(row["date"], row["time"], row["vehicle"], row["odometer_km"], row["total_price_eur"], row["liters"], row["price_per_liter"], row["notes"], device_type, ua)
        created += 1
    return created
