#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add
from collections import defaultdict

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: netflix <file> <userID> <output>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("NetflixRecommendation")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    movieRatingsToUsers = lines.map(lambda x: (x[1]+"-"+x[2], x[0])) \
                  .groupByKey().mapValues(list)

    usersToRatings = lines.map(lambda x: (x[0], x[1]+"-"+x[2])) \
                  .groupByKey().mapValues(list)
    userMovies = usersToRatings.lookup(sys.argv[2])

    similar = defaultdict(int)
    for movie in userMovies:
      usersThatRatedSame = movieRatingsToUsers.lookup(movie)
      for user in usersThatRatedSame:
        similar[user] += 1

    similar = similar.items()
    similar.sort(reverse=True)

    target = open(sys.argv[3], 'w')
    for similarUser in similar:
      target.write(similarUser[0])
      target.write("\n")

    spark.stop()
