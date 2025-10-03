import re
from typing import List


class TextTokenizer:
    def __init__(self):
        self.word_pattern = re.compile(r"\b[a-zA-Z0-9']+\b")

    def tokenize(self, text: str) -> List[str]:
        tokens = self.word_pattern.findall(text)
        normalized_tokens = [token.lower() for token in tokens]

        return normalized_tokens