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

def findGivenUser(userMovies, foundUserMovie, givenUser):
    if foundUserMovie[0] == givenUser:
      userMovies.append(foundUserMovie[1]+"-"+foundUserMovie[2])
    return (foundUserMovie[1]+"-"+foundUserMovie[2], foundUserMovie[0])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: netflix <file> <userID> <output>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("NetflixRecommendation")\
        .getOrCreate()

    target = open(sys.argv[3], 'w')
    target.truncate()
    userMovies = []
    givenUser = sys.argv[2]
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0].split())
    movieRatingsToUsers = lines.map(lambda x: findGivenUser(userMovies, x, givenUser)) \
                  .groupByKey().mapValues(list)

    similar = defaultdict(int)
    for movie in userMovies:
      usersThatRatedSame = movieRatingsToUsers.lookup(movie)[0]
      for user in usersThatRatedSame:
        similar[user] += 1

    similarItems = similar.items()
    similarItems.sort(key=lambda x: -x[1])

    target.write('\n')
    for m in userMovies:
      target.write(str(m))
      target.write('\n')

    target.write('\n')
    for similarUser in similarItems:
      target.write(str(similarUser))
      target.write("\n")

    spark.stop()
    
