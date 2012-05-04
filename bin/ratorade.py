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
import pymongo
import bson
from math import sqrt

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

def histogram(collection, keylist, histname, kdelim=":"):
    fmap_code = "function() { emit(" + (' + "' + kdelim + '" + ').join(["this."+k for k in keylist]) + ", 1) }"
    fmap = bson.code.Code(fmap_code)
    fred = bson.code.Code("function (key, values) {"
                          "  var total = 0;"
                          "  for (var i = 0; i < values.length; i++) {"
                          "    total += values[i];"
                          "  }"
                          "  return total;"
                          "}")
    hist = collection.map_reduce(fmap, fred, histname)
    return hist

def update_model_linear(models, tnew, tref, id_attr="beer", rating_attr="rating", prev=None):
    # canonically, the lesser id is associated with "0", the other one is "1"
    if (tnew[id_attr] <= tref[id_attr]):
        newk = "0"
        refk = "1"
    else:
        newk = "1"
        refk = "0"
    # retrieve the current model, or create a new one
    # these are unique
    model = models.find_one({"pair":{newk:tnew[id_attr], refk:tref[id_attr]}, "type":"linear", "id_attr":id_attr, "rating_attr":rating_attr})
    if model is None:
        mid = models.insert({"pair":{newk:tnew[id_attr], refk:tref[id_attr]}, "type":"linear", "id_attr":id_attr, "rating_attr":rating_attr, "n":0.0, "0":0.0, "1":0.0, "00":0.0, "11":0.0, "01":0.0})
        model = models.find_one({"_id":mid})
    if prev is not None:
        # altering an existing pairwise stat
        rnew = tnew[rating_attr]
        rprv = prev[rating_attr]
        rref = tref[rating_attr]
        model[newk] += rnew - rprv
        model[newk+newk] += (rnew*rnew) - (rprv*rprv)
        model["01"] += (rnew - rprv)*rref
    else:
        # adding a new pairwise stat
        rnew = tnew[rating_attr]
        rref = tref[rating_attr]
        model["n"] += 1
        model[newk] += rnew
        model[refk] += rref
        model[newk+newk] += rnew*rnew
        model[refk+refk] += rref*rref
        model["01"] += rnew*rref
    # cache latest linear params
    n = model["n"]
    s0 = model["0"]
    s1 = model["1"]
    s00 = model["00"]
    s11 = model["11"]
    s01 = model["01"]
    nn = n*s01 - s0*s1
    d0 = n*s00 - s0*s0
    d1 = n*s11 - s1*s1
    if ((d0 != 0) and (d1 != 0)):
        # correlation coefficient:
        model["r"] = nn / (sqrt(d0) * sqrt(d1))
        # linear model x1 = b01*x0 + a01
        model["b01"] = nn / d0
        model["a01"] = (s1 - model["b01"]*s0)/n
        # linear model x0 = b10*x1 + a10
        model["b10"] = nn / d1
        model["a10"] = (s0 - model["b10"]*s1)/n
    else:
        model["r"] = 0
        model["b01"] = 0
        model["a01"] = 0
        model["b10"] = 0
        model["a10"] = 0
    # store the updated model back into the db collection
    models.update({"_id":model["_id"]}, {"$set":model})
