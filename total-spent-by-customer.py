from pyspark import SparkConf, SparkContext

conf = conf = SparkConf().setMaster("local").setAppName("CustomerBill")
sc = SparkContext(conf = conf)

def parseline(text):
    fields= text.split(',')
    customerId=int(fields[0])
    dollars=float(fields[2])
    return (customerId, dollars)

lines = sc.textFile("C:/SparkCourse/customer-orders.csv")
rdd = lines.map(parseline)
reduction = rdd.reduceByKey(lambda x, y: x + y)               

flipped = reduction.map(lambda x: (x[1], x[0]))

totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print(result)

 