import sys
import types
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1] / "lindenprotokoll" / "app"
sys.path.insert(0, str(APP_DIR))
# db imports this helper at module load; dataset construction does not use it.
werkzeug = types.ModuleType("werkzeug")
security = types.ModuleType("werkzeug.security")
security.generate_password_hash = lambda value: value
werkzeug.security = security
sys.modules.setdefault("werkzeug", werkzeug)
sys.modules.setdefault("werkzeug.security", security)
import db  # noqa: E402


def _insert_meal(conn, person_id, date, time, foods):
    cur = conn.execute(
        "INSERT INTO entries(category, subtype, person_id, date, time) VALUES ('meal', 'meal', ?, ?, ?)",
        (person_id, date, time),
    )
    entry_id = cur.lastrowid
    for food in foods:
        conn.execute("INSERT OR IGNORE INTO food_items_master(food_name) VALUES (?)", (food,))
        food_id = conn.execute("SELECT food_id FROM food_items_master WHERE food_name=?", (food,)).fetchone()[0]
        conn.execute("INSERT INTO meal_food_links(entry_id, food_id) VALUES (?, ?)", (entry_id, food_id))
    return entry_id


def test_same_person_and_timestamp_are_one_regression_observation(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.sqlite")
    db.init_db()
    with db.connect() as conn:
        p1 = conn.execute("INSERT INTO people(display_name) VALUES ('A')").lastrowid
        p2 = conn.execute("INSERT INTO people(display_name) VALUES ('B')").lastrowid
        _insert_meal(conn, p1, "2026-03-15", "14:00", ["Apfel"])
        _insert_meal(conn, p1, "2026-03-15", "14:00:00", ["Banane"])
        _insert_meal(conn, p2, "2026-03-15", "14:00", ["Brot"])

    result = db.build_regression_dataset(min_occurrences=1)
    assert result["n_meal_entries"] == 3
    assert result["n_meal_groups"] == 2
    assert result["n_combined_entries"] == 1
    assert len(result["dataset"]) == 2
    assert {"Apfel", "Banane"}.issubset(result["foods"])
    assert "Brot" in result["foods"]
    combined = next(row for row in result["dataset"] if row["Apfel"] == 1)
    assert combined["Banane"] == 1
