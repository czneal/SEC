import re

import nltk  # type: ignore

from typing import List, Dict, Union, Any, cast, Tuple, Iterable

from mysqlio.readers import MySQLReader


class QueryParser():
    def __init__(self):
        self.tokenizer = nltk.word_tokenize
        self.alphanum = re.compile(r'[\w\d]+', re.I)

    def tokenize(self, query: str) -> List[str]:
        return cast(List[str], self.tokenizer(query))

    def clean_tokens(self, tokens: List[str]) -> List[str]:
        new_tokens: List[str] = []
        for t in tokens:
            if len(t) == 0:
                continue
            if len(t) > 1:
                new_tokens.append(t)
                continue

            if self.alphanum.fullmatch(t):
                new_tokens.append(t)

        return new_tokens

    def parse(self, query: str) -> List[str]:
        return self.clean_tokens(self.tokenize(query))


class FullTextSearch(MySQLReader):
    def search_framed(
            self,
            tokens: List[str],
            frame_size: int,
            max_results: int) -> List[Dict[str, Any]]:

        phrase = '"' + ' '.join(tokens) + f'" @{frame_size}'
        query = """select * from html_reports \
            where match(report_text) against(%(phrase)s in boolean mode) limit %(limit)s"""

        data = self.fetch(query, {'phrase': phrase,
                                  'limit': max_results})

        return data

    def search_phrase(
            self,
            phrase: str,
            max_results: int) -> List[Dict[str, Any]]:
        query = """select * from html_reports \
            where match(report_text) against(%(phrase)s in natural language mode) \
            limit %(limit)s"""

        return self.fetch(query, {'phrase': phrase, 'limit': max_results})


def find_snippet(
        tokens: Iterable[str],
        text: List[str],
        frame_size: int) -> str:
    tks = set([t.lower() for t in tokens])

    # (frame_end, word_count, frame_start)
    best_match: Tuple[int, int, int] = (-1, 0, -1)
    tokens_in_frame: Dict[str, int] = {}

    for i, w in enumerate(text):
        if w.lower() in tks:
            tokens_in_frame[w.lower()] = i

        to_remove = [(k, v) for k, v in tokens_in_frame.items()
                     if v < i - frame_size]
        for t, t_i in to_remove:
            if t_i < i - frame_size:
                tokens_in_frame.pop(t)

        if len(tokens_in_frame) > best_match[1]:
            best_match = (
                i, len(tokens_in_frame), min(
                    tokens_in_frame.values()))

    if best_match[0] != -1:
        middle = int((best_match[0] + best_match[2]) / 2)
        left = int(frame_size / 2)
        return ' '.join(text[middle - left: middle + left])

    return ''


def search(query: str,
           search_frame_size: int,
           snippet_frame_size: int,
           max_results: int) -> List[Tuple[str, str]]:
    q = QueryParser()
    r = FullTextSearch()

    tokens = q.parse(query)
    if len(tokens) == 0:
        return []

    if len(tokens) == 1:
        data = r.search_phrase(
            phrase=tokens[0],
            max_results=max_results)
    else:
        data = r.search_framed(
            tokens=tokens,
            frame_size=search_frame_size,
            max_results=max_results)
        if not data:
            data = r.search_phrase(phrase=' '.join(tokens),
                                   max_results=max_results)

    retval: List[Tuple[str, str]] = []
    for row in data:
        text = row['report_text'].split(' ')
        snippet = find_snippet(tokens,
                               text,
                               frame_size=snippet_frame_size)
        if snippet:
            retval.append((row['adsh'], snippet))

    return retval


if __name__ == '__main__':
    query = input('input your search query: ')
    snippets = search(query=query,
                      search_frame_size=5,
                      snippet_frame_size=25,
                      max_results=5)

    print(snippets)
