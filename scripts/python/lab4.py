# To launch pyspark shell:
# pyspark --master local[2]
#
# To run this script:
# spark-submit --master local[2] lab4.py

from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab4");
sc = SparkContext(conf = conf);


print "========================================================================"
print "Lab 4.1.2 - Load Data into Apache Spark"
# Map input variables
IncidntNum = 0
Category = 1
Descript = 2
DayOfWeek = 3
Date = 4
Time = 5
PdDistrict = 6
Resolution = 7
Address = 8
X = 9
Y = 10
PdId = 11

# Load SFPD data into RDD
sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(lambda line:line.split(","))

print "========================================================================"
print "LAB 4.1.3 Explore Data Using RDD Operations"
print "1 -------------"
# 1. How do you see the first element of the inputRDD?
print sfpdRDD.first()

print "2 -------------"
# 2.What do you use to see the first 5 elements of the RDD?
print sfpdRDD.take(5)

print "3 -------------"
# 3. What is the total number of incidents?
totincs = sfpdRDD.count()
print totincs

print "4 -------------"
# 4. What is the total number of distinct resolutionss?
totres = sfpdRDD.map(lambda inc:inc[Resolution]).distinct().count()
print totres

print "5 -------------"
# 5. List all the Districts.
dists = sfpdRDD.map(lambda inc:inc[PdDistrict]).distinct()
dists.collect()
print dists.count()
print dists.take(dists.count())

print "========================================================================"
print "LAB 4.2.1 - Create PAIR RDDs & apply pairRDD operations"
print "1 -------------"
# 1. Which five districts have the highest incidents?
top5Dists=sfpdRDD.map(lambda incident:(incident[PdDistrict],1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(5)
print top5Dists

print "2 -------------"
# 2. Which five addresses have the highest number of incidents?
top5Adds=sfpdRDD.map(lambda incident:(incident[Address],1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(5)
print top5Adds

print "3 -------------"
# 3. What are the top three catefories of incidents?
top3Cat=sfpdRDD.map(lambda incident:(incident[Category],1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(3)
print top3Cat

print "4 -------------"
# 4. What is the count of incidents by district?
num_inc_dist=sfpdRDD.map(lambda incident:(incident[PdDistrict],1)).countByKey()
print num_inc_dist

print "========================================================================"
print "Lab 4.2.2. Join PairRDD"
print "5 -------------"
# 5. Load each dataset into separate pairRDDs with "address" being the key?
catAdd=sc.textFile("/user/user01/data/J_AddCat.csv").map(lambda x:x.split(",")).map(lambda x:(x[1],x[0]))
distAdd=sc.textFile("/user/user01/data/J_AddDist.csv").map(lambda x:x.split(",")).map(lambda x:(x[1],x[0]))


print "6 -------------"
# 6. List the incident category and district for for those addresses that have both category and district information. Verify that the size estimated earlier is correct.
catJdist=catAdd.join(distAdd)
catJdist.collect()
print catJdist.count()
print catJdist.take(10)

print "7 -------------"
# 7.	List the incident category and district for all addresses irrespective of whether each address has category and district information. Verify that the size estimated earlier is correct.
catJdist1 = catAdd.leftOuterJoin(distAdd)
catJdist1.collect()
print catJdist1.count()

print "8 -------------"
# 8.	List the incident district and category for all addresses irrespective of whether each address has category and district information. Verify that the size estimated earlier is correct.
catJdist2 = catAdd.rightOuterJoin(distAdd)
catJdist2.collect()
print catJdist2.count()


print "========================================================================"
print "Lab 4.3.1. Explore Partitioning"
print "1 -------------"
# 1. How many partitions are there in the sfpdRDD?
print sfpdRDD.getNumPartitions()

print "2 -------------"
# 2. How do you find the type of partitioner?
print sfpdRDD.partitioner

print "3 -------------"
# 3. Create a pair RDD - INcidents by district
incByDists = sfpdRDD.map(lambda incident:(incident[PdDistrict],1)).reduceByKey(lambda (x,y): x+y)
# How many partitions does this have?
print incByDists.getNumPartitions()
# What is the type of partitioner?
print incByDists.partitioner

for property, value in vars(incByDists.partitioner).iteritems():
    print property, ": ", value

print "4 -------------"
# 4. Now add a map transformation
inc_map = incByDists.map(lambda incident: (incident[1], incident[0]))
# Is there a change in the size?
print inc_map.getNumPartitions()
# What about the type of partitioner?
print inc_map.partitioner

print "5 -------------"
try:
    # 5.Add sortByKey()
    inc_sort = inc_map.sortByKey(False)
    # type of partitioner
    print inc_sort.partitioner

    for property, value in vars(inc_sort.partitioner).iteritems():
        print property, ": ", value
except:
    print "sortByKey raises an error... :-s"

print "6 -------------"
# 6. Add groupByKey
inc_group = sfpdRDD.map(lambda incident: (incident(PdDistrict),1)).groupByKey()
# type of partitioner
print inc_group.partitioner

for property, value in vars(inc_group.partitioner).iteritems():
    print property, ": ", value

print "7 -------------"
# 7. specify partition size in the transformation
incByDists = sfpdRDD.map(lambda incident: (incident(PdDistrict),1)).reduceByKey(lambda (x,y): x+y, 10)
# number of partitions
print incByDists.getNumPartitions()

print "8 -------------"
# 8. Create 2 pairRDD
catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(lambda x:x.split(",")).map(lambda x: (x[1], x[0]))
distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(lambda x:x.split(",")).map(lambda x: (x[1], x[0]))

print "9 -------------"
# 9. join and specify partitions- then check the number of partitions and the partitioner
catJdist=catAdd.join(distAdd,8)
catJdist.getNumPartitions()
print catJdist.partitioner

for property, value in vars(catJdist.partitioner).iteritems():
    print property, ": ", value
