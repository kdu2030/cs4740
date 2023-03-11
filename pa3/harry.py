from mrjob.job import MRJob
from typing import List
import re

WORD_RE = re.compile(r"[\w']+")

class MRHarryWords(MRJob):

    def mapper(self, _: None, line: str):
        for word in WORD_RE.findall(line):
            lowercase_word = word.lower()
            if lowercase_word in ("magical", "soaring", "lopsided"):
                yield lowercase_word, 1
    
    def combiner(self, key: str, values: List[int]):
        yield key, sum(values)
    
    def reducer(self, key: str, values: List[int]):
        yield key, sum(values)

def main():
    MRHarryWords.run()

if __name__ == "__main__":
    main()
