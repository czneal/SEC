import re
import json

from collections import defaultdict
from typing import List, Dict

from bs4 import BeautifulSoup
from bs4.element import Comment  # type: ignore

from urltools import fetch_with_delay


def tag_visible(element):
    if element.parent.name in [
        'style',
        'script',
        'head',
        'title',
        'meta',
            '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def extract_words(text: str) -> List[str]:
    if not text:
        return []

    special = ['']

    text = text.lower()
    text = text.replace('\u201a', ' ').replace('\u201c', ' ').replace(
        '\u2014', '-').replace('\u2019', "'").replace('\u00ae', ' ')
    text = re.sub(r'[\s]+', ' ', text)
    text = re.sub(r'[\.\,\?\!=;:\-]+\s+', ' ', text)
    text = re.sub(r'[<>\(\)\{\}/]+', ' ', text)

    return text.split(' ')


def main():
    body = fetch_with_delay(
        "https://www.sec.gov/Archives/edgar/data/1067983/000156459020005874/brka-10k_20191231.htm")
    with open('outputs/html_page.txt', 'wb') as f:
        f.write(body)

    soup = BeautifulSoup(body, 'lxml')

    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)

    words_freq = defaultdict(int)

    for t in visible_texts:
        words = extract_words(t.strip())
        for w in words:
            words_freq[w] += 1

    words_freq = dict(
        sorted(
            words_freq.items(),
            key=lambda x: x[1],
            reverse=True))

    with open('outputs/html_text.txt', 'w', encoding='utf8') as f:
        f.write(json.dumps(words_freq, indent=2))


if __name__ == '__main__':
    main()
