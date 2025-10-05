from typing import List

from src.indexer.TextTokenizer import TextTokenizer
from src.indexer.StopwordsFilter import StopwordsFilter


class TextProcessor:
    def __init__(self, tokenizer: TextTokenizer = None, stopwords_filter: StopwordsFilter = None):
        self.tokenizer = tokenizer if tokenizer else TextTokenizer()
        self.stopwords_filter = stopwords_filter if stopwords_filter else StopwordsFilter()

    def process(self, text: str) -> List[str]:
        tokens = self.tokenizer.tokenize(text)
        filtered_tokens = self.stopwords_filter.filter(tokens)
        return filtered_tokens