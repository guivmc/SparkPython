from SparkSessionBuilder import SparkSessionBuilder

def main():
    sparkRef = SparkSessionBuilder()
    #sparkRef.csv_to_SparkDataFrame()
    #sparkRef.generateRandomParquet()
    sparkRef.sumRandomParquet()

if __name__ == '__main__': main()
