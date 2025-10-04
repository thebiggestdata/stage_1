from dataclasses import dataclass
from typing import Optional


@dataclass
class BookMetadata:
    book_id: int
    title: Optional[str] = None
    author: Optional[str] = None
    language: Optional[str] = None
    release_date: Optional[str] = None

    def __post_init__(self):
        if self.title:
            self.title = self.title.strip()
        if self.author:
            self.author = self.author.strip()
        if self.language:
            self.language = self.language.strip()
        if self.release_date:
            self.release_date = self.release_date.strip()

    def is_complete(self) -> bool:
        return self.title is not None and self.author is not None

    def __repr__(self) -> str:
        return (
            f"BookMetadata(book_id={self.book_id}, "
            f"title='{self.title}', "
            f"author='{self.author}', "
            f"language='{self.language}')"
        )