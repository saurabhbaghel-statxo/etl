
from typing import Optional, Union, Dict, List
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import logging
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


logger = logging.getLogger(__name__)

class MetadataTypes(StrEnum):
    TABULAR = "tabular"
    '''When the data is in columnar, tabular format'''

    TEXT_FILE = "text"
    '''When the data is in text format, human readable'''


class TabularData(BaseModel):
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
    """Creates Metadata, reads-writes it. This is not a runnable"""

    def __init__(self, file: str | Path = Path(".data.metadata")):
        self._file = Path(file) if type(file) is str else file
        
        # initialize the metadata
        self._metadata = self.load()
    
    @property
    def file(self) -> Path:
        return self._file
    
    def load(self) -> List[Dict]:
        """Loads metadata already stored"""
        try:
            with open(self._file, "r") as fin:
                prev_metadata = json.load(fin)
        except FileNotFoundError:
            logger.info("Metadata json not present. Creating new one")
            prev_metadata = []

        finally:
            return prev_metadata

    def write(self, **kwargs):
        """Writes the metadata to the file"""
        
        _metadata = kwargs.get("metadata")

        if not _metadata:
            _metadata = Metadata(**kwargs)

        if not _metadata.content:
            _row_start = kwargs.get("row_start", 0)
            _row_end = kwargs.get("row_end", 0)

            table_content = TabularData(row_start=_row_start, row_end=_row_end)

            # update the table content
            _metadata.content = table_content.model_dump()

        logger.debug("Metadata = %s", str(_metadata))

        if not self._metadata:
            self._metadata = self.load()
        
        self._metadata.append(_metadata.model_dump())

        logger.info("Writing the metadata to %s", self._file)
        with open(self._file, "w") as fout:
            json.dump(self._metadata, fout)
        
    
    def __repr__(self):
        if not self._metadata:
            self._metadata = self.load()

        return (f"File = {self._file}"
                f"{self._metadata[:min(len(self._metadata, 2))]}"   # first two entries
                "..."
                f"{self._metadata[max(-len(self._metadata, -2)):]}"  # bottom two entries
                )

    
