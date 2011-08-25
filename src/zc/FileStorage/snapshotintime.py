##############################################################################
#
# Copyright (c) 2005-2011 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import os
import re
import sys
import zc.FileStorage
import ZODB.TimeStamp

usage = """Usage: %s [input-path utc-snapshot-time output-path]

Make a point-in time snapshot of a file-storage data file containing
just the current records as of the given time.  The resulting file can
be used as a basis of a demo storage.

If the output file isn't given, then a file name will be generated
based on the input file name and the utc-snapshot-time.

If the utc-snapshot-time is ommitted, then the current time will be used.

Note: blobs (if any) aren't copied.

The UTC time is a string of the form: YYYY-MM-DDTHH:MM:SS.  The time
conponents are optional.  The time defaults to midnight, UTC.
"""



def main(args=None):
    if args is None:
        args = sys.argv[1:]

    if len(args) < 2 or len(args) > 3:
        print >>sys.stderr, usage % sys.argv[0]
        sys.exit(1)


    try:
        if len(args) > 2:
            inpath, stop, outpath = args
        else:
            inpath, stop = args
            if inpath.endswith('.fs'):
                outpath = inpath[:-3]+stop+'.fs'
            else:
                outpath = inpath+stop
    except ValueError:
        print >>sys.stderr, usage % sys.argv[0]
        sys.exit(1)

    if not os.path.exists(inpath):
        print >>sys.stderr, inpath, 'Does not exist.'
        sys.exit(1)

    try:
        date, time = (stop.split('T')+[''])[:2]
        year, month, day = map(int, date.split('-'))
        if time:
            hour, minute, second = (map(int, time.split(':'))+[0,0])[:3]
        else:
            hour = minute = second = 0
        stop = repr(
            ZODB.TimeStamp.TimeStamp(year, month, day, hour, minute, second)
            )
    except Exception:
        print >>sys.stderr, 'Bad date-time:', stop
        sys.exit(1)

    zc.FileStorage.PackProcess(inpath, stop, os.stat(inpath).st_size
                               ).pack(snapshot_in_time_path=outpath)

