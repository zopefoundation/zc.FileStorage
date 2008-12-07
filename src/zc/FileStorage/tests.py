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


import os
import unittest
from zope.testing import doctest

from ZODB.tests.testFileStorage import * # :-P
from ZODB.tests.PackableStorage import * # :-P
from ZODB.tests.TransactionalUndoStorage import * # :-P

class NoGCFileStorageTests(FileStorageTests):

    def setUp(self):
        self.open(create=1)
        self.__gcpath = os.path.abspath('FileStorageTests.fs.packnogc')
        open(self.__gcpath, 'w')

    def tearDown(self):
        self._storage.close()
        self._storage.cleanup()
        os.remove(self.__gcpath)


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


    def checkTransactionalUndoAfterPack(self):
        eq = self.assertEqual
        # Add a few object revisions
        oid = self._storage.new_oid()
        revid1 = self._dostore(oid, data=MinPO(51))
        snooze()
        packtime = time.time()
        snooze()                # time.time() now distinct from packtime
        revid2 = self._dostore(oid, revid=revid1, data=MinPO(52))
        self._dostore(oid, revid=revid2, data=MinPO(53))
        # Now get the undo log
        info = self._storage.undoInfo()
        eq(len(info), 3)
        tid = info[0]['id']
        # Now pack just the initial revision of the object.  We need the
        # second revision otherwise we won't be able to undo the third
        # revision!
        self._storage.pack(packtime, referencesf)
        # Make some basic assertions about the undo information now
        info2 = self._storage.undoInfo()
        eq(len(info2), 3)
        # And now attempt to undo the last transaction
        t = Transaction()
        self._storage.tpc_begin(t)
        tid, oids = self._storage.undo(tid, t)
        self._storage.tpc_vote(t)
        self._storage.tpc_finish(t)
        eq(len(oids), 1)
        eq(oids[0], oid)
        data, revid = self._storage.load(oid, '')
        # The object must now be at the second state
        eq(zodb_unpickle(data), MinPO(52))
        self._iterate()

    
    def checkPackWithGCOnDestinationAfterRestore(self):
        pass

    def checkPackWithMultiDatabaseReferences(self):
        pass

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(NoGCFileStorageTests, "check"))
    suite.addTest(doctest.DocFileSuite('mru.txt'))
    return suite
