
from typing import Optional, Union, Dict, List
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import logging
from enum import StrEnum
import sqlite3

from pydantic import BaseModel, Field, field_validator


logger = logging.getLogger(__name__)

class MetadataTypes(StrEnum):
    TABULAR = "tabular"
    '''When the data is in columnar, tabular format'''

    TEXT_FILE = "text"
    '''When the data is in text format, human readable'''


class TabularData(BaseModel):
    table_name: Optional[str] = ""
    '''Name of the table'''

    row_start: Optional[int] = 0
    '''Row number data starting'''

    row_end: Optional[int] = 0
    '''Row number data ending'''

@dataclass
class FileData:
    # TODO: 
    pass


# @dataclass
class Metadata(BaseModel):
    """Meta data to be stored after every run of extraction"""
    
    table_name: Optional[str] = ""
    '''Name of the table'''

    date_started: Optional[datetime] = Field(default_factory=datetime.now)
    '''Date time extraction started'''

    date_ended: Optional[datetime] = None
    '''Date time extraction ended'''

    data_type: Optional[MetadataTypes] = None
    '''Type of Metadata'''

    src_data: Optional[str] = None
    '''Source of the data, database-url, path, etc'''

    dest_data: Optional[str] = None
    '''Destination of data, file-path, database-url, etc'''

    content: Optional[Dict] = Field(default_factory=dict)
    '''Info about the content of the data for which this is the metadata'''

    @field_validator("date_started", "date_ended")
    @classmethod
    def validate_datetime(cls, v):
        if v is None:
            return v
        if isinstance(v, datetime):
            return v.isoformat()  # or str(v)
        return v


class MetadataHandler:
    """Creates Metadata, reads/writes to JSON + SQLite DB."""

    def __init__(
        self,
        file: str | Path = Path(".data.metadata.json"),
        db_path: str | Path = Path(".metadata/metadata.db")
    ):
        self._file = Path(file)
        self._db_path = Path(db_path)

        # Ensure folder exists
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize storage
        self._metadata = self.load_json()

        # Initialize DB schema
        self._init_db()

    # =====================================================
    #                       JSON MODE
    # =====================================================

    def load_json(self) -> List[Dict]:
        try:
            with open(self._file, "r") as fin:
                return json.load(fin)
        except FileNotFoundError:
            return []

    def write_json(self, metadata: Metadata):
        """Append metadata to JSON file."""
        self._metadata.append(metadata.model_dump())
        with open(self._file, "w") as fout:
            json.dump(self._metadata, fout)

    # =====================================================
    #                       SQLite MODE
    # =====================================================

    def _init_db(self):
        conn = sqlite3.connect(self._db_path)
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_started TEXT,
                date_ended TEXT,
                data_type TEXT,
                src_data TEXT,
                dest_data TEXT,
                content TEXT
            )
            """
        )

        conn.commit()
        conn.close()

    def write_db(self, metadata: Metadata):
        conn = sqlite3.connect(self._db_path)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO metadata (
                date_started, date_ended, data_type,
                src_data, dest_data, content
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                metadata.date_started,
                metadata.date_ended,
                metadata.data_type,
                metadata.src_data,
                metadata.dest_data,
                json.dumps(metadata.content),
            ),
        )

        conn.commit()
        conn.close()

    def write(self, **kwargs):
        metadata = kwargs.get("metadata")

        if not metadata:
            metadata = Metadata(**kwargs)

        # Construct default content if not supplied
        if not metadata.content:
            content = TabularData(
                table_name=metadata.table_name,
                row_start=kwargs.get("row_start", 0),
                row_end=kwargs.get("row_end", 0)
            )
            metadata.content = content.model_dump()

        # Write JSON + DB
        self.write_json(metadata)
        self.write_db(metadata)

        return metadata
    
    def get_last_row_end(self, table_name: str) -> int:
        conn = sqlite3.connect(self._db_path)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT content
            FROM metadata
            WHERE content LIKE ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (f'%"table_name": "{table_name}"%',),
        )

        row = cur.fetchone()
        conn.close()

        if not row:
            return 0  # No metadata yet → full load

        content = json.loads(row[0])
        return content.get("row_end", 0)

    def get_incremental_range(self, table_name: str) -> Dict[str, int]:
        last_end = self.get_last_row_end(table_name)
        return {
            "row_start": last_end + 1,
            "row_end": None  # Your extractor decides final row_end
        }
