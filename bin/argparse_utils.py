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

def trynum(s):
    try: v = int(s)
    except: pass
    else: return v

    try: v = float(s)
    except: pass
    else: return v

    return s


def dict_expr(arg):
    emsg = "bad dict expr '%s'" % (arg)
    try:
        d = eval(arg)
    except Exception:
        raise argparse.ArgumentTypeError(emsg)
    if type(d) != dict:
        raise argparse.ArgumentTypeError(emsg)
    return d


def avpair(arg):
    p = arg.split(':')
    if len(p) != 2:
        emsg = "bad avpair '%s'" % (arg)
        raise argparse.ArgumentTypeError(emsg)
    return (p[0],trynum(p[1]))


class store_avpair_dict(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if type(values) != list: vals = [values]
        else:                    vals = values
        setattr(namespace, self.dest, dict(vals))


class append_avpair_dict(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if type(values) != list: vals = [values]
        else:                    vals = values
        if hasattr(namespace, self.dest): d = getattr(namespace, self.dest)
        else: d = []
        setattr(namespace, self.dest, d + [dict(vals)])
