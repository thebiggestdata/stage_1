import datetime
from pathlib import Path
from typing import Optional


class DatalakePathBuilder:
    def __init__(self, base_path: str = "datalake"):
        self.base_path = Path(base_path)

    def get_book_directory(self, timestamp: Optional[datetime.datetime] = None) -> Path:
        if timestamp is None:
            timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")
        return self.base_path / date_str / hour_str