from typing import Dict, List, Union, Tuple
from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

def get_line_data(line: str) -> Dict[str, int]:
    data = line.split(",")
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
    flower_data = {}
    for i, data_pt in enumerate(data):
        flower_data[columns[i]] = data_pt
    return flower_data

class MRFlowerData(MRJob):

    def mapper(self, _: None, line: str):
        flower_data = line.split(",")
        yield "sepal_length", float(flower_data[0])
        yield "petal_width", float(flower_data[3])
        if flower_data[-1] == "Iris-setosa":
            yield "is_data", (float(flower_data[1]), 1)
        else:
            yield "others_data", (float(flower_data[0]), float(flower_data[2]), 1)
    
    def combiner(self, key: str, values: Union[List[float], List[Tuple]]):
        if key == "sepal_length":
            yield "sepal_length", min(values)
        if key == "petal_width":
            yield "petal_width", max(values)
        if key == "is_data":
            sum_sepal_width = 0
            count = 0
            for value in values:
                sum_sepal_width += value[0]
                count += value[1]
            yield "is_data", (sum_sepal_width, count)
        if key == "others_data":
            sum_sepal_length = 0
            sum_petal_length = 0
            count = 0
            for value in values:
                sum_sepal_length += value[0]
                sum_petal_length += value[1]
                count += value[2]
            yield "others_data", (sum_sepal_length, sum_petal_length, count)
    
    def reducer(self, key: str, values: Union[List[float], List[Tuple]]):
        if key == "sepal_length":
            yield f"sepal_length", min(values)
        if key == "petal_width":
            yield f"petal_width", max(values)
        if key == "is_data":
            sum_sepal_width = 0
            count = 0
            for value in values:
                sum_sepal_width += value[0]
                count += value[1]
            yield "is_avg_sepal_width", sum_sepal_width / count
        if key == "others_data":
            sum_sepal_length = 0
            sum_petal_length = 0
            count = 0
            for value in values:
                sum_sepal_length += value[0]
                sum_petal_length += value[1]
                count += value[2]
            yield "others_avg_sepal_petal_diff", (sum_sepal_length / count) - (sum_petal_length / count)



def main():
    #print(get_line_data("5.1,3.5,1.4,0.2,Iris-setosa"))
    MRFlowerData.run()

if __name__ == "__main__":
    main()