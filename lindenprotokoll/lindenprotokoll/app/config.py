from pathlib import Path
DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "lindenprotokoll.db"
HOST = "0.0.0.0"
PORT = 8100
DEBUG = False
SECRET_KEY = "lindenprotokoll-secret"
DEFAULT_PERSONS = ["Livi", "Clara", "Claas", "Caro", "Clemens"]
DEFAULT_FOOD_ITEMS = ["kein Essen", "Brot", "Milch", "Nudeln", "Reis"]
DEFAULT_MEDICATIONS = ["Ibuprofen", "Paracetamol"]
DEFAULT_SYMPTOMS = ["Kopfschmerzen", "Übelkeit", "Schwindel", "Husten", "Schnupfen", "Halsschmerzen", "Müdigkeit", "Gliederschmerzen"]
DEFAULT_SETTINGS = {
    "food_sort_mode": "usage",
    "symptom_sort_mode": "usage",
    "abdominal_image_url": "",
}
