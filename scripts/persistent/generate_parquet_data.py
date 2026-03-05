import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
import string

rng = np.random.default_rng(42)
n_rows = 100_000


def string_generator(length: int = 24) -> str:
    char_list = [*list(string.ascii_letters), " "]
    random_integers = rng.integers(0, len(char_list), length)
    random_characters = [char_list[i] for i in random_integers]
    return "".join(random_characters).strip()


fake_data = pd.DataFrame({
    "id": range(1, n_rows + 1),
    "name": [f"item_{i}" for i in range(1, n_rows + 1)],
    "category": rng.choice(["A", "B", "C"], size=n_rows),
    "value": rng.normal(100, 15, n_rows),
    "count": rng.integers(1, 100, n_rows),
    "active": rng.choice([True, False], size=n_rows),
    "description": [string_generator() for _ in range(n_rows)],
})


parquet_dir = Path("data/persistent/parquet_files")
parquet_dir.mkdir(parents=True, exist_ok=True)

# Write multiple parquet files
for i, category in enumerate(fake_data["category"].unique()):
    category_data = fake_data[fake_data["category"] == category]
    file_path = parquet_dir / f"file_{i}.parquet"
    category_data.to_parquet(file_path, index=False)

print("Parquet files written to:", parquet_dir)
