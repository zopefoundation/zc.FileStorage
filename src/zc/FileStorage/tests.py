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

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ZCFileStorageTests, "check"))
    suite.addTest(unittest.makeSuite(ZCFileStorageTestsWithBlobs, "check"))
    suite.addTest(doctest.DocFileSuite(
        'blob_packing.txt',
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown,
        ))
    return suite
