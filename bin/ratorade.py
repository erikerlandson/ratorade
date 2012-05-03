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

import pymongo

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
    model = models.find_one({"type":"linear", "id_attr":id_attr, "rating_attr":rating_attr, "pair":{newk:tnew[id_attr], refk:tref[id_attr]}})
    if model is None:
        mid = models.insert({"type":"linear", "id_attr":id_attr, "rating_attr":rating_attr, "pair":{newk:tnew[id_attr], refk:tref[id_attr]}, "n":0.0, "0":0.0, "1":0.0, "00":0.0, "11":0.0, "01":0.0})
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
    # store the updated model back into the db collection
    models.update({"_id":model["_id"]}, {"$set":model})
