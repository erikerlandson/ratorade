#!/bin/env python

# ratorade: Everyone's a critic
#
# Copyright (c) 2012 Erik Erlandson
#
# Author:  Erik Erlandson <erikerlandson@yahoo.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import argparse
import random
from math import sqrt, floor

import pymongo

def require_connection(dbserver):
    try:
        mongo = pymongo.Connection(dbserver)
    except:
        sys.stderr.write("failed to connect to db server %s\n" % (dbserver))
        exit(1)
    return mongo

def require_db(mongo, dbname):
    try:
        if dbname not in mongo.database_names():
            raise Exception("db does not exist")
        mongo_db = mongo[dbname]
    except:
        sys.stderr.write("failed to open db %s on server %s:%s\n" % (dbname, mongo.host, mongo.port))
        exit(1)
    return mongo_db

def require_collection(mongo_db, collection):
    try:
        mongo_db.validate_collection(collection)
        collection = mongo_db[collection]
    except:
        sys.stderr.write("failed to open collection %s on db %s\n" % (collection, mongo_db.name))
        exit(1)
    return collection

def query_attributes(query):
    attr = set()
    if type(query) == dict:
        for k in query.keys():
            attr |= set(query_attributes(query[k]))
            if k[0] == '$': continue
            attr.add(k)
        return list(attr)
    elif type(query) == list:
        for e in query:
            attr |= set(query_attributes(e))
    return attr


def random_sampling_query(p, rk0="rk0", rk1="rk1", pad = 0):
    d = (1.0 - sqrt(1.0-p)) * (1.0 + pad)
    if d > 1.0: d = 1.0
    if d < 0.0: d = 0.0
    s0 = random.random()*(1.0 - d)
    s1 = random.random()*(1.0 - d)
    return {"$or":[{rk0:{"$gte":s0, "$lt":s0+d}}, {rk1:{"$gte":s1, "$lt":s1+d}}]}


parser = argparse.ArgumentParser(add_help=False)
grp = parser.add_argument_group(title="MongoDB Authentication")
grp.add_argument('-dbname', default='ratorade', metavar='<name>', help='rating db name: def="ratorade"')
grp.add_argument('-dbserver', default='127.0.0.1', metavar='<db-server-ip>', help='def=127.0.0.1')
