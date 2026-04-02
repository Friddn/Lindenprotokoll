# Lindenprotokoll – Home Assistant Add-on

Mehrzweck-Protokoll-App für Essen, Krankheit und Verbrauch.

## Installation in Home Assistant

1. Gehe zu **Einstellungen → Add-ons → Add-on Store**
2. Klicke oben rechts auf die **drei Punkte → Repositories**
3. Füge folgende URL hinzu:
   ```
   https://github.com/DEIN-USERNAME/DEIN-REPO-NAME
   ```
4. Das Add-on „Lindenprotokoll" erscheint jetzt im Store
5. Installieren → Starten

## Update

Sobald eine neue Version auf GitHub verfügbar ist, zeigt Home Assistant automatisch „Update verfügbar" an.

## Datensicherheit

Alle Daten liegen in `/data/lindenprotokoll.db` — dieses Verzeichnis wird von Home Assistant persistent gespeichert und bei Updates **nicht** überschrieben.
