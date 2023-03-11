from mrjob.job import MRJob
from typing import List

class MRWordFrequencyCount(MRJob):

    def mapper(self, _: None, line: str):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1
    
    def reducer(self, key: str, values: List[int]):
        yield key, sum(values)
    

def main():
    MRWordFrequencyCount.run()

if __name__ == "__main__":
    main()