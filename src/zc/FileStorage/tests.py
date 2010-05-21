##############################################################################
#
# Copyright (c) 2006 Zope Corporation and Contributors.
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

##############################################################################
# Test FileStorage packing sans GC
#
# This module is a bit of a hack.  It simply copies and modifies the
# tests affected by the lack of gc in pack.
##############################################################################

import pickle
import unittest
import zc.FileStorage
import ZODB.blob
import ZODB.tests.testblob

from ZODB.tests.testFileStorage import * # :-P
from ZODB.tests.PackableStorage import * # :-P
from ZODB.tests.TransactionalUndoStorage import * # :-P

from zope.testing import doctest, setupstack

class ZCFileStorageTests(FileStorageTests):

    blob_dir = None

    def setUp(self):
        self.open(create=1, packer=zc.FileStorage.packer,
                  blob_dir=self.blob_dir)

    def tearDown(self):
        self._storage.close()
        self._storage.cleanup()
        if self.blob_dir:
            ZODB.blob.remove_committed_dir(self.blob_dir)

    def checkPackAllRevisions(self):
        self._initroot()
        eq = self.assertEqual
        raises = self.assertRaises
        # Create a `persistent' object
        obj = self._newobj()
        oid = obj.getoid()
        obj.value = 1
        # Commit three different revisions
        revid1 = self._dostoreNP(oid, data=pdumps(obj))
        obj.value = 2
        revid2 = self._dostoreNP(oid, revid=revid1, data=pdumps(obj))
        obj.value = 3
        revid3 = self._dostoreNP(oid, revid=revid2, data=pdumps(obj))
        # Now make sure all three revisions can be extracted
        data = self._storage.loadSerial(oid, revid1)
        pobj = pickle.loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 1)
        data = self._storage.loadSerial(oid, revid2)
        pobj = pickle.loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 2)
        data = self._storage.loadSerial(oid, revid3)
        pobj = pickle.loads(data)
        eq(pobj.getoid(), oid)
        eq(pobj.value, 3)
        # Now pack all transactions; need to sleep a second to make
        # sure that the pack time is greater than the last commit time.
        now = packtime = time.time()
        while packtime <= now:
            packtime = time.time()
        self._storage.pack(packtime, referencesf)
        # Only old revisions of the object should be gone. We don't gc
        raises(KeyError, self._storage.loadSerial, oid, revid1)
        raises(KeyError, self._storage.loadSerial, oid, revid2)
        self._storage.loadSerial(oid, revid3)


    def checkPackUndoLog(self):
        self._initroot()
        # Create a `persistent' object
        obj = self._newobj()
        oid = obj.getoid()
        obj.value = 1
        # Commit two different revisions
        revid1 = self._dostoreNP(oid, data=pdumps(obj))
        obj.value = 2
        snooze()
        packtime = time.time()
        snooze()
        self._dostoreNP(oid, revid=revid1, data=pdumps(obj))
        # Now pack the first transaction
        self.assertEqual(3, len(self._storage.undoLog()))
        self._storage.pack(packtime, referencesf)
        # The undo log contains only the most resent transaction
        self.assertEqual(3, len(self._storage.undoLog()))

    def checkPackWithGCOnDestinationAfterRestore(self):
        pass

    def checkPackWithMultiDatabaseReferences(self):
        pass

class ZCFileStorageTestsWithBlobs(ZCFileStorageTests):

    blob_dir = 'blobs'


time_hack_template = """
now = 1268166473.0
import time

time_time, time_sleep = time.time, time.sleep

time.sleep(1) # Slow things down a bit to give the test time to commit

def faux_time():
    global now
    now += 1
    return now

def faux_sleep(x):
    logging.info('sleep '+`x`)

time.time, time.sleep = faux_time, faux_sleep
"""

GIG_hack_template = """

import sys

sys.path[:] = %(syspath)r

import zc.FileStorage
zc.FileStorage.GIG = 100

"""

def test_pack_sleep():
    """
Make sure that sleep is being called. :)

Mess with time -- there should be infrastructure for this!

    >>> exec(time_hack_template)
    >>> time.sleep = time_sleep

    >>> import threading, transaction, shutil, ZODB.FileStorage, zc.FileStorage
    >>> fs = ZODB.FileStorage.FileStorage('data.fs',
    ...                                   packer=zc.FileStorage.packer1)
    >>> db = ZODB.DB(fs)
    >>> conn = db.open()
    >>> for i in range(5):
    ...     conn.root()[i] = conn.root().__class__()
    ...     transaction.commit()
    >>> pack_time = time.time()
    >>> for i in range(5):
    ...     conn.root()[i].x = 1
    ...     transaction.commit()

    >>> pack_script_template = zc.FileStorage.pack_script_template
    >>> zc.FileStorage.pack_script_template = (
    ...     time_hack_template + GIG_hack_template + pack_script_template)
    >>> thread = threading.Thread(target=fs.pack, args=(pack_time, now))
    >>> thread.start()
    >>> for i in range(100):
    ...     if os.path.exists('data.fs.packscript'):
    ...        break
    ...     time.sleep(0.01)
    >>> def faux_sleep(x):
    ...     print 'sleep '+`x`
    >>> time.sleep = faux_sleep
    >>> conn.root().x = 1
    >>> transaction.commit()
    >>> thread.join()
    sleep 1.0

    >>> fs.close()
    >>> print open('data.fs.packlog').read(), # doctest: +NORMALIZE_WHITESPACE
    2010-03-09 15:27:55,000 root INFO packing to 2010-03-09 20:28:06.000000,
       sleep 1
    2010-03-09 15:27:57,000 root INFO read 162
    2010-03-09 15:27:59,000 root INFO sleep 2.0
    2010-03-09 15:28:01,000 root INFO read 411
    2010-03-09 15:28:03,000 root INFO sleep 2.0
    2010-03-09 15:28:05,000 root INFO read 680
    2010-03-09 15:28:07,000 root INFO sleep 2.0
    2010-03-09 15:28:09,000 root INFO read 968
    2010-03-09 15:28:11,000 root INFO sleep 2.0
    2010-03-09 15:28:13,000 root INFO read 1275
    2010-03-09 15:28:15,000 root INFO sleep 2.0
    2010-03-09 15:28:17,000 root INFO read 1601
    2010-03-09 15:28:19,000 root INFO sleep 2.0
    2010-03-09 15:28:21,000 root INFO initial scan 6 objects at 1601
    2010-03-09 15:28:22,000 root INFO copy to pack time
    2010-03-09 15:28:24,000 root INFO read 162
    2010-03-09 15:28:26,000 root INFO sleep 2.0
    2010-03-09 15:28:28,000 root INFO read 411
    2010-03-09 15:28:30,000 root INFO sleep 2.0
    2010-03-09 15:28:32,000 root INFO read 680
    2010-03-09 15:28:34,000 root INFO sleep 2.0
    2010-03-09 15:28:36,000 root INFO read 968
    2010-03-09 15:28:38,000 root INFO sleep 2.0
    2010-03-09 15:28:40,000 root INFO read 1275
    2010-03-09 15:28:42,000 root INFO sleep 2.0
    2010-03-09 15:28:44,000 root INFO read 1601
    2010-03-09 15:28:46,000 root INFO sleep 2.0
    2010-03-09 15:28:47,000 root INFO copy from pack time
    2010-03-09 15:28:51,000 root INFO sleep 1.0
    2010-03-09 15:28:52,000 root INFO read 1737
    2010-03-09 15:28:54,000 root INFO sleep 5.0
    2010-03-09 15:28:58,000 root INFO sleep 1.0
    2010-03-09 15:28:59,000 root INFO read 1873
    2010-03-09 15:29:01,000 root INFO sleep 5.0
    2010-03-09 15:29:05,000 root INFO sleep 1.0
    2010-03-09 15:29:06,000 root INFO read 2009
    2010-03-09 15:29:08,000 root INFO sleep 5.0
    2010-03-09 15:29:12,000 root INFO sleep 1.0
    2010-03-09 15:29:13,000 root INFO read 2145
    2010-03-09 15:29:15,000 root INFO sleep 5.0
    2010-03-09 15:29:19,000 root INFO sleep 1.0
    2010-03-09 15:29:20,000 root INFO read 2281
    2010-03-09 15:29:22,000 root INFO sleep 5.0
    2010-03-09 15:29:23,000 root INFO packscript done

    >>> time.sleep = time_sleep
    >>> time.time = time_time

Now do it all again with a longer sleep:

    >>> shutil.copyfile('data.fs.old', 'data.fs')
    >>> fs = ZODB.FileStorage.FileStorage('data.fs',
    ...                                   packer=zc.FileStorage.packer2)
    >>> fs.pack(pack_time, now)
    >>> print open('data.fs.packlog').read(), # doctest: +NORMALIZE_WHITESPACE
    2010-03-09 15:27:55,000 root INFO packing to 2010-03-09 20:28:06.000000,
      sleep 2
    2010-03-09 15:27:57,000 root INFO read 162
    2010-03-09 15:27:59,000 root INFO sleep 4.0
    2010-03-09 15:28:01,000 root INFO read 411
    2010-03-09 15:28:03,000 root INFO sleep 4.0
    2010-03-09 15:28:05,000 root INFO read 680
    2010-03-09 15:28:07,000 root INFO sleep 4.0
    2010-03-09 15:28:09,000 root INFO read 968
    2010-03-09 15:28:11,000 root INFO sleep 4.0
    2010-03-09 15:28:13,000 root INFO read 1275
    2010-03-09 15:28:15,000 root INFO sleep 4.0
    2010-03-09 15:28:17,000 root INFO read 1601
    2010-03-09 15:28:19,000 root INFO sleep 4.0
    2010-03-09 15:28:21,000 root INFO initial scan 6 objects at 1601
    2010-03-09 15:28:22,000 root INFO copy to pack time
    2010-03-09 15:28:24,000 root INFO read 162
    2010-03-09 15:28:26,000 root INFO sleep 4.0
    2010-03-09 15:28:28,000 root INFO read 411
    2010-03-09 15:28:30,000 root INFO sleep 4.0
    2010-03-09 15:28:32,000 root INFO read 680
    2010-03-09 15:28:34,000 root INFO sleep 4.0
    2010-03-09 15:28:36,000 root INFO read 968
    2010-03-09 15:28:38,000 root INFO sleep 4.0
    2010-03-09 15:28:40,000 root INFO read 1275
    2010-03-09 15:28:42,000 root INFO sleep 4.0
    2010-03-09 15:28:44,000 root INFO read 1601
    2010-03-09 15:28:46,000 root INFO sleep 4.0
    2010-03-09 15:28:47,000 root INFO copy from pack time
    2010-03-09 15:28:51,000 root INFO sleep 2.0
    2010-03-09 15:28:52,000 root INFO read 1737
    2010-03-09 15:28:54,000 root INFO sleep 10.0
    2010-03-09 15:28:58,000 root INFO sleep 2.0
    2010-03-09 15:28:59,000 root INFO read 1873
    2010-03-09 15:29:01,000 root INFO sleep 10.0
    2010-03-09 15:29:05,000 root INFO sleep 2.0
    2010-03-09 15:29:06,000 root INFO read 2009
    2010-03-09 15:29:08,000 root INFO sleep 10.0
    2010-03-09 15:29:12,000 root INFO sleep 2.0
    2010-03-09 15:29:13,000 root INFO read 2145
    2010-03-09 15:29:15,000 root INFO sleep 10.0
    2010-03-09 15:29:19,000 root INFO sleep 2.0
    2010-03-09 15:29:20,000 root INFO read 2281
    2010-03-09 15:29:22,000 root INFO sleep 10.0
    2010-03-09 15:29:26,000 root INFO sleep 2.0
    2010-03-09 15:29:27,000 root INFO read 2514
    2010-03-09 15:29:29,000 root INFO sleep 10.0
    2010-03-09 15:29:30,000 root INFO packscript done

    >>> zc.FileStorage.pack_script_template = pack_script_template

    """

def data_transform_and_untransform_hooks():
    r"""The Packer factory takes uptions to transform and untransform data

This is helpful when data records aren't raw pickles or when you want
to transform them so that they aren't raw pickles.  To test this,
we'll take a file storage database and convert it to use the
ZODB.tests.hexstorage trandormation.

    >>> import ZODB.FileStorage
    >>> db = ZODB.DB(ZODB.FileStorage.FileStorage(
    ...     'data.fs', blob_dir='blobs',
    ...     packer=zc.FileStorage.Packer(
    ...            transform='zc.FileStorage.tests:hexer',
    ...            untransform='zc.FileStorage.tests:unhexer',
    ...            )))
    >>> conn = db.open()
    >>> conn.root.b = ZODB.blob.Blob('test')
    >>> conn.transaction_manager.commit()

    >>> _ = conn.root.b.open().read()

So, here we have some untransformed data. Now, we'll pack it:

    >>> db.pack()

Now, the database records are hex:

    >>> db.storage.load('\0'*8)[0][:50]
    '.h6370657273697374656e742e6d617070696e670a50657273'

    >>> db.storage.load('\0'*7+'\1')[0][:50]
    '.h635a4f44422e626c6f620a426c6f620a71012e4e2e'

Let's add an object. (WE get away with this because the object's we
use are in the cache. :)

    >>> conn.root.a = conn.root().__class__()
    >>> conn.transaction_manager.commit()

Now the root and the new object are not hex:

    >>> db.storage.load('\0'*8)[0][:50]
    'cpersistent.mapping\nPersistentMapping\nq\x01.}q\x02U\x04data'

    >>> db.storage.load('\0'*7+'\2')[0][:50]
    'cpersistent.mapping\nPersistentMapping\nq\x01.}q\x02U\x04data'

We capture the current time as the pack time:

    >>> import time
    >>> pack_time = time.time()
    >>> time.sleep(.1)

We'll throw in a blob modification:

    >>> conn.root.b.open('w').write('test 2')
    >>> conn.transaction_manager.commit()

Now pack and make sure all the records have been transformed:


    >>> db.pack()
    >>> from ZODB.utils import p64
    >>> for i in range(len(db.storage)):
    ...     if db.storage.load(p64(i))[0][:2] != '.h':
    ...         print i

We should have only one blob file:

    >>> nblobs = 0
    >>> for _, _, files in os.walk('blobs'):
    ...     for file in files:
    ...         if file.endswith('.blob'):
    ...             nblobs += 1
    >>> nblobs
    1

    """

def hexer(data):
    return (data[:2] == '.h') and data or ('.h'+data.encode('hex'))
def unhexer(data):
    return data and (data[:2] == '.h' and data[2:].decode('hex') or data)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZCFileStorageTests, "check"))
    suite.addTest(unittest.makeSuite(ZCFileStorageTestsWithBlobs, "check"))
    suite.addTest(doctest.DocFileSuite(
        'blob_packing.txt',
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown,
        ))
    suite.addTest(doctest.DocTestSuite(
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown,
        ))
    return suite
