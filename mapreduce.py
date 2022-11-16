from glob import glob
import threading

def loadData():

    fnames = glob("data/*")
    data = []

    for fn in fnames:

        with open(fn,'r', encoding='utf-8') as data_file:
            dataTemp = data_file.read()

        dataTemp = dataTemp.replace("."," ")
        dataTemp = dataTemp.replace("\n"," ")

        data.append(dataTemp)

    return data

class MapReduce():

    def __init__(self):
        pass

    def set_data(self, data):
        self.data = data

    def run(self):
        thread = [None] * len(self.data)
        results = [None] * len(self.data)

        for i in range(len(self.data)):
            thread[i] = threading.Thread(target=self._mapper_and_combiner, args=(self.data[i], i, results))
            thread[i].start()

        for i in range(len(self.data)):
            thread[i].join()

        mapped = []
        for res in results:
            mapped += res
        
        reducer = Reducer()
        reducer.set_data(mapped)
        reduced = reducer.run()

        return reduced

    def _mapper_and_combiner(self, data:str, map_id:int, results:list):

        # Run Mapper
        mapper = Mapper()
        mapper.set_data(data)
        res = mapper.run()
        
        # Run Combiner
        comb = Combiner(res)
        res = comb.run()

        results[map_id] = res.items()

    def _reduce(self):
        pass

    def _combiner(self):
        pass

class Mapper():
    
    def __init__(self):
        pass

    def set_data(self, data:str):
        self.data = data

    def run(self):
        counts = []
        last_space_idx = -1
        for idx in range(len(self.data)):
            if(self.data[idx] == " "):
                counts.append((self.data[last_space_idx+1:idx], 1))
                last_space_idx = idx
            else: continue

        self.counts = counts
        return counts

class Combiner():
    def __init__(self, counts):
        self.counts = counts

    def run(self):
        dic = {}
        for count in self.counts:
            if count[0] in dic.keys(): dic[count[0]] += count[1]
            else: dic[count[0]] = count[1]

        return dic

class Reducer():
    def __init__(self):
        self.data = []

    def set_data(self, data):
        self.data = data
    
    def run(self):
        dic = {}
        for count in self.data:
            if count[0] in dic.keys(): dic[count[0]] += count[1]
            else: dic[count[0]] = count[1]

        return dic
