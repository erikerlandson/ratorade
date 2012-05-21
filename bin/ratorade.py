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
import time
import random
from math import sqrt, floor, fabs

import pymongo
import bson

import dbutils

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


def histogram_to_collection(collection, keylist, histname, bins={}, where={}, sortkey='freq', prob=False, cumulative=False, counts=False, sortdir=pymongo.DESCENDING, sample=0):
    # start with fresh collection:
    collection.database.drop_collection(histname)
    hist = collection.database[histname]
    min_scan = []
    max_scan = []
    # identify any binning keys that need min/max values:
    for a in bins.keys():
        if a not in keylist:
            em = "binning key %s not in %s" % (a, keylist)
            raise Exception(em)
        if (not bins[a].has_key('bins')) or (type(bins[a]['bins']) != int):
            em = "binning key %s requires integer 'bins' entry" % (a)
            raise Exception(em)
        if not bins[a].has_key('min'): min_scan += [a]
        else: bins[a]['min'] = float(bins[a]['min'])
        if not bins[a].has_key('max'): max_scan += [a]
        else: bins[a]['max'] = float(bins[a]['max'])
    # include random sampling if requested:
    sq = None
    use_limit = 0
    if sample >= 1:
        sq = dbutils.random_sampling_query(float(sample) / float(collection.count()), pad=0.1)
        use_limit = int(sample)
    elif sample > 0:
        sq = dbutils.random_sampling_query(sample)
    if sq is not None:
        where = dict(list(where.items())+list(sq.items()))
    # if we need to, scan the data to determin min/max values:
    if (len(min_scan)+len(max_scan)) > 0:
        efirst = True
        if use_limit > 0:  curs = collection.find(where,fields=keylist).limit(use_limit)
        else:              curs = collection.find(where,fields=keylist)
        for e in curs:
            if efirst:
                for a in min_scan: bins[a]['min'] = e[a]
                for a in max_scan: bins[a]['max'] = e[a]
                efirst = False
            for a in min_scan: bins[a]['min'] = min(e[a], bins[a]['min'])
            for a in max_scan: bins[a]['max'] = max(e[a], bins[a]['max'])
    # determine bin widths:
    for a in bins.keys():
        d = bins[a]['max'] - bins[a]['min']
        if d <= 0:
            em = "max <= min for %s" % (a)
            raise Exception(em)
        bins[a]['w'] = d/float(bins[a]['bins'])
    # iterate over the data and do the histogramming:
    if use_limit > 0: qres = collection.find(where, fields=keylist).limit(use_limit)
    else:             qres = collection.find(where, fields=keylist)
    ftot = qres.count(True)
    for e in qres:
        hk = {}
        for k in keylist:
            if bins.has_key(k):
                hk[k] = floor((float(e[k])-bins[k]['min'])/bins[k]['w'])*bins[k]['w'] + bins[k]['min']
            else:
                hk[k] = e[k]
        # either increments freq, or creates new entry w/ freq = 1:
        hist.update({'_id':hk}, {'$inc':{'freq':1}}, True)
    # fill in optional fields like prob, cfreq, cprob, etc:
    if prob or cumulative or counts:
        if sortkey in keylist:
            hres = hist.find().sort([("_id."+sortkey, sortdir)])
        else:
            hres = hist.find().sort([(sortkey, sortdir)])
        htot = hres.count()
        count = 0
        cfreq = 0
        for h in hres:
            freq = h['freq']
            up = {}
            if prob:
                up['prob'] = float(freq)/float(ftot)
            if cumulative:
                cfreq += freq
                up['cfreq'] = cfreq
                up['cprob'] = float(cfreq)/float(ftot)
            if counts:
                count += 1
                up['count'] = count
                up['cfrac'] = float(count)/float(htot)
            hist.update({'_id':h['_id']}, {'$set':up})
    # return the collection we built the histogram in:
    return hist


def inverse_quantile(hist, q, cprob='cprob'):
    return list(hist.find({cprob:{"$gte": q}}).sort([(cprob, pymongo.ASCENDING)]).limit(1))[0]


def update_stats_linear(stats, tnew, tref, id_attr="***undef***", rating_attr="***undef***", prev=None):
    # canonically, the lesser id is associated with "0", the other one is "1"
    if (tnew[id_attr] <= tref[id_attr]):
        newk = "0"
        refk = "1"
    else:
        newk = "1"
        refk = "0"
    if prev is not None:
        # altering an existing pairwise stat
        rnew = tnew[rating_attr]
        rprv = prev[rating_attr]
        rref = tref[rating_attr]
        stats.update({'_id':{"k"+newk:tnew[id_attr], "k"+refk:tref[id_attr]}}, {'$inc':{"s"+newk:rnew-rprv, "s"+newk+newk:(rnew*rnew) - (rprv*rprv), "s01":(rnew - rprv)*rref}, '$set':{"upd":time.time()}})
    else:
        # adding a new pairwise stat
        rnew = tnew[rating_attr]
        rref = tref[rating_attr]
        stats.update({'_id':{"k"+newk:tnew[id_attr], "k"+refk:tref[id_attr]}}, {'$inc':{"n":1, "s"+newk:rnew, "s"+refk:rref, "s"+newk+newk:rnew*rnew, "s"+refk+refk:rref*rref, "s01":rnew*rref}, '$set':{"upd":time.time()}}, True)


def update_model_linear(models, stats, ssmin=0, rrmin=0.0, absamax=None, absbmax=None):
    # cache latest linear params
    n = stats["n"]
    if n < ssmin: return

    s0 = stats["s0"]
    s1 = stats["s1"]
    s00 = stats["s00"]
    s11 = stats["s11"]
    s01 = stats["s01"]

    nn = n*s01 - s0*s1
    d0 = n*s00 - s0*s0
    d1 = n*s11 - s1*s1

    if ((d0 != 0) and (d1 != 0)):
        # correlation coefficient:
        r = nn / (sqrt(d0) * sqrt(d1))
    else:
        r = 0
    rr = r*r
    if rr < rrmin: return

    if ((d0 != 0) and (d1 != 0)):
        # linear model x1 = a0*x0 + b0
        a0 = nn / d0
        b0 = (s1 - a0*s0)/n
        # linear model x0 = a1*x1 + b1
        a1 = nn / d1
        b1 = (s0 - a1*s1)/n
    else:
        a0 = 0
        b0 = 0
        a1 = 0
        b1 = 0

    # I think if any of these are out of bounds I'll ignore both models
    if (absamax is not None) and (absamax < fabs(a0)): return
    if (absamax is not None) and (absamax < fabs(a1)): return
    if (absbmax is not None) and (absbmax < fabs(b0)): return
    if (absbmax is not None) and (absbmax < fabs(b1)): return

    # store the updated models
    u = time.time()
    models.update({'_id':{"x":stats['_id']["k0"],"y":stats['_id']["k1"]}}, {'$set':{"n":n, "rr":rr, "a":a0, "b":b0, "upd":u}}, True)
    models.update({'_id':{"x":stats['_id']["k1"],"y":stats['_id']["k0"]}}, {'$set':{"n":n, "rr":rr, "a":a1, "b":b1, "upd":u}}, True)
