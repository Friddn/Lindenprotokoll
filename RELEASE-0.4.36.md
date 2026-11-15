# Lindenprotokoll 0.4.36 – Fix für Screenshot-Review-Fehler 500

## Ursache
Bei bestehenden Installationen konnte `daily_hp_totals` aus einer älteren Datenbankversion stammen. Die Screenshot-Review-Seite verwendet inzwischen die Spalte `screenshot_id` für die Verknüpfung gespeicherter Tageswerte mit dem Screenshot. Fehlte diese Spalte in einer bestehenden Datenbank, führte das Laden der Seite zu einem Internal Server Error.

## Korrektur
- `db.init_db()` ergänzt fehlende Spalten `screenshot_id`, `confidence` und `updated_at` additiv.
- Bestehende Tageswerte bleiben erhalten; es werden keine Tabellen gelöscht oder neu aufgebaut.
- Versionsnummer auf 0.4.36 erhöht.
