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
import random
from math import sqrt

import pymongo
import bson

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

def histogram(collection, keylist, histname, kdelim=":", numeric=False, bins=0, kmin=None, kmax=None):
    if bins>0: numeric=True
    if numeric and (len(keylist) > 1):
        raise Exception("only one numeric histogram key is allowed")
    if bins > 0:
        if kmin is None:
            kkmin = minimum_value(collection, keylist[0], "_work"+histname).find_one()['value']
        else:
            kkmin = kmin
        if kmax is None:
            kkmax = maximum_value(collection, keylist[0], "_work"+histname).find_one()['value']
        else:
            kkmax = kmax
    if numeric and (bins > 0):
        binw = (kkmax-kkmin)/float(bins)
        fmap_code = "function() { emit(Math.floor((this." + keylist[0] + " - " + str(kkmin) + ")/"+str(binw)+")*" + str(binw) + " + " + str(kkmin) + ", 1) }"
    else:
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

def minimum_value(collection, key, resname):
    fmap = bson.code.Code("function() { emit(\"min\", this." + key + ") }")
    fred = bson.code.Code("function (key, values) {"
                          "  var vmin = values[0];"
                          "  for (var i = 1; i < values.length; i++) {"
                          "    vmin = Math.min(vmin,values[i]);"
                          "  }"
                          "  return vmin;"
                          "}")
    return collection.map_reduce(fmap, fred, resname)

def maximum_value(collection, key, resname):
    fmap = bson.code.Code("function() { emit(\"max\", this." + key + ") }")
    fred = bson.code.Code("function (key, values) {"
                          "  var vmax = values[0];"
                          "  for (var i = 1; i < values.length; i++) {"
                          "    vmax = Math.max(vmax,values[i]);"
                          "  }"
                          "  return vmax;"
                          "}")
    return collection.map_reduce(fmap, fred, resname)

def random_sampling_query(p, rk0="rk0", rk1="rk1", pad = 0):
    d = (1.0 - sqrt(1.0-p)) * (1.0 + pad)
    if d > 1.0: d = 1.0
    if d < 0.0: d = 0.0
    s0 = random.random()*(1.0 - d)
    s1 = random.random()*(1.0 - d)
    return {"$or":[{rk0:{"$gte":s0, "$lt":s0+d}}, {rk1:{"$gte":s1, "$lt":s1+d}}]}


def update_model_linear(models, tnew, tref, id_attr="***undef***", rating_attr="***undef***", prev=None):
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
        # linear model x1 = a0*x0 + b0
        model["a0"] = nn / d0
        model["b0"] = (s1 - model["a0"]*s0)/n
        # linear model x0 = a1*x1 + b1
        model["a1"] = nn / d1
        model["b1"] = (s0 - model["a1"]*s1)/n
    else:
        model["r"] = 0
        model["a0"] = 0
        model["b0"] = 0
        model["a1"] = 0
        model["b1"] = 0
    # store the updated model back into the db collection
    models.save(model)
