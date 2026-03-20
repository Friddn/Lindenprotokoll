from pathlib import Path
DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "lindenprotokoll.db"
HOST = "0.0.0.0"
PORT = 8100
DEBUG = False
SECRET_KEY = "lindenprotokoll-secret"
DEFAULT_PERSONS = ["Person-A", "Person-B", "Person-C", "Person-D", "Person-E"]
DEFAULT_FOOD_ITEMS = ["kein Essen", "Brot", "Milch", "Nudeln", "Reis"]
DEFAULT_MEDICATIONS = ["Ibuprofen", "Paracetamol"]
DEFAULT_SETTINGS = {
    "food_sort_mode": "usage",
    "abdominal_image_url": "",
}
