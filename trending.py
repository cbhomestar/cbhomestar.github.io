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

import  os
#check if pyspark env vars are set and then reset to required or delete.   
del os.environ['PYSPARK_SUBMIT_ARGS']

import sys
from operator import add
from collections import defaultdict

from pyspark.sql import SparkSession

def findUsageTrend(counts):
  trend = 0
  if len(counts) == 1:
    return 0
  for i in xrange(1, len(counts)):
    trend += int(counts[i]) - int(counts[i-1])

  return trend

if __name__ == "__main__":
    if len(sys.argv) != 3:
      print("Usage: trending <file> <output>", file=sys.stderr)
      exit(-1)

    spark = SparkSession\
        .builder\
        .appName("DisappearingWords")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    wordCount = lines.map(lambda x: (x.split()[0], x.split()[2])) \
      .groupByKey().mapValues(list).map(lambda x: (x[0], findUsageTrend(x[1]))) \
      .sortBy(lambda x: x[1])

    wordCount.saveAsTextFile(sys.argv[2])
    '''similar = defaultdict(int)
    for movie in userMovies:
      usersThatRatedSame = movieRatingsToUsers.lookup(movie)[0]
      for user in usersThatRatedSame:
        similar[user] += 1

    similarItems = similar.items()
    similarItems.sort(key=lambda x: -x[1])
    
    target.write('check 1 2 3')
    '''

    spark.stop()                                             
