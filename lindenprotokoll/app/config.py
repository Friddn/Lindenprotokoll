from pathlib import Path
DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "lindenprotokoll.db"
HOST = "0.0.0.0"
PORT = 8100
DEBUG = False
# Persist a random Flask signing key in Home Assistant's persistent /data volume.
# Never ship a fixed signing key in the source repository.
import secrets
_SECRET_FILE = DATA_DIR / "flask_secret_key"
def _load_secret_key():
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if _SECRET_FILE.exists():
            value = _SECRET_FILE.read_text(encoding="utf-8").strip()
            if len(value) >= 32:
                return value
        value = secrets.token_hex(32)
        _SECRET_FILE.write_text(value, encoding="utf-8")
        try:
            _SECRET_FILE.chmod(0o600)
        except OSError:
            pass
        return value
    except OSError:
        # Startup should fail safely rather than fall back to a public constant.
        raise RuntimeError("Cannot create persistent Flask secret key in /data")
SECRET_KEY = _load_secret_key()
DEFAULT_PERSONS = ["Livi", "Clara", "Claas", "Caro", "Clemens"]
DEFAULT_FOOD_ITEMS = ["kein Essen", "Brot", "Milch", "Nudeln", "Reis"]
DEFAULT_MEDICATIONS = ["Ibuprofen", "Paracetamol"]
DEFAULT_SYMPTOMS = ["Kopfschmerzen", "Übelkeit", "Schwindel", "Husten", "Schnupfen", "Halsschmerzen", "Müdigkeit", "Gliederschmerzen"]
DEFAULT_SETTINGS = {
    "food_sort_mode": "usage",
    "symptom_sort_mode": "usage",
    "abdominal_image_url": "",
}
