from scripts.data_generators.tests.base import IcebergTest
import pathlib
import tempfile
import duckdb
import numpy as np
import pandas as pd
import string

import os
from typing import Type, List, Optional
import shutil

SCRIPT_DIR = os.path.dirname(__file__)
FILES_DIR = os.path.join(SCRIPT_DIR, '..', '..', '..', 'data', 'generated', 'files')

def generate_files():
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

	parquet_dir = pathlib.Path(FILES_DIR)
	parquet_dir.mkdir(parents=True, exist_ok=True)

	# Write multiple parquet files
	for i, category in enumerate(fake_data["category"].unique()):
		category_data = fake_data[fake_data["category"] == category]
		file_path = parquet_dir / f"file_{i}.parquet"
		category_data.to_parquet(file_path, index=False)


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

    def generate(self, catalog: str, *, target: Optional[str] = None, connection_kwargs: Optional[dict] = None):
        generate_files()

        if catalog != "local":
            return
        con = self.get_connection(catalog, target=target, **(connection_kwargs or {}))

        con.con.sql("CREATE NAMESPACE IF NOT EXISTS test_add_files")

        table_schema = """
        id bigint,
        name string,
        category string,
        value double,
        count bigint,
        active boolean,
        description string
        """

        # ----------------------------------------------------
        # add_files
        # ----------------------------------------------------

        con.con.sql(f"""
        CREATE OR REPLACE TABLE test_add_files.fake_data_test ({table_schema})
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """)

        con.con.sql(f"""
        CALL iceberg_catalog.system.add_files(
        table => 'test_add_files.fake_data_test',
        source_table => 'parquet.`{FILES_DIR}`'
        )
        """)

    #def setup(self, con):
    #    con.con.read.parquet(self.parquet_file.as_posix()).createOrReplaceTempView('parquet_file_view')
