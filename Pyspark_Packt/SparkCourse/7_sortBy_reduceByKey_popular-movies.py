from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext.getOrCreate(conf = conf)

lines = sc.textFile("./ml-100k/u.data")
#lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

#flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
#sortedMovies = flipped.sortByKey()

sortedMovies = movieCounts.sortBy(lambda x : x[1])
results = sortedMovies.collect()

for result in results:
    print(result)
