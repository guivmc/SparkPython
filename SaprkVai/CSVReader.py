import csv
import os

class CSVReader:
    filePath = ""

    def __init__(self):
        self.filePath = os.path.abspath("CSV/Test.csv")


    def readFile(self):
        print(self.filePath)
        with open(self.filePath, 'r') as file:
            readLine = csv.reader(file)
            for line in readLine:
                print(line)