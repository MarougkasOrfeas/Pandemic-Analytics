from enum import Enum
from pathlib import Path


class Paths(Enum):
    BACKGROUND_IMAGE = Path.cwd() / ".." / ".." / "assets" / "images" / "bg.jpg"
    CSV_FILE = Path.cwd() / ".." / ".." / "assets" / "csv" / "covid_data.csv"
    DATASET_ONLINE = "https://www.kaggle.com/datasets/meirnizri/covid19-dataset"
