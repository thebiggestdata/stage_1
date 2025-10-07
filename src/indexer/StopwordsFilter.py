from typing import List, Set


class StopwordsFilter:

    DEFAULT_STOPWORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'but', 'by',
        'for', 'from', 'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on',
        'that', 'the', 'to', 'was', 'were', 'will', 'with', 'the', 'this',
        'i', 'you', 'we', 'they', 'she', 'him', 'her', 'his', 'their',
        'what', 'which', 'who', 'when', 'where', 'why', 'how',
        'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
        'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
        'than', 'too', 'very', 'can', 'just', 'should', 'now'
    }

    def __init__(self, custom_stopwords: Set[str] = None):
        self.stopwords = custom_stopwords if custom_stopwords else self.DEFAULT_STOPWORDS

    def filter(self, tokens: List[str]) -> List[str]:
        filtered_tokens = [token for token in tokens if token not in self.stopwords]
        return filtered_tokens

    def is_stopword(self, token: str) -> bool:
        return token in self.stopwords