from mrjob.job import MRJob, MRStep
from typing import List, Tuple
import re

WORD_RE = re.compile(r"[\w']+")


class MRHarryFrequent(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_count),
                MRStep(reducer=self.reducer_sort)]

    def mapper(self, _: None, line: str):
        for word in WORD_RE.findall(line):
            lowercase_word = word.lower()
            yield lowercase_word, 1

    def combiner(self, key: str, values: List[int]):
        yield key, sum(values)

    def reducer_count(self, key: str, values: List[int]):
        yield None, (key, sum(values))

    def reducer_sort(self, _: None, values: List[Tuple]):
        values_list = list(values)
        values_list.sort(key=lambda element: element[1], reverse=True)
        for word_data in values_list[0:10]:
            yield word_data[0], word_data[1]

def main():
    MRHarryFrequent.run()

if __name__ == "__main__":
    main()
