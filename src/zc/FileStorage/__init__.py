##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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

import ZODB.FileStorage
import ZODB.FileStorage.fspack

class FileStorage(ZODB.FileStorage.FileStorage):

    def pack(self, t, referencesf):
        """Copy data from the current database file to a packed file

        Non-current records from transactions with time-stamp strings less
        than packtss are ommitted. As are all undone records.

        Also, data back pointers that point before packtss are resolved and
        the associated data are copied, since the old records are not copied.
        """
        if self._is_read_only:
            raise POSException.ReadOnlyError()

        stop=`TimeStamp(*time.gmtime(t)[:5]+(t%60,))`
        if stop==z64: raise FileStorageError('Invalid pack time')

        # If the storage is empty, there's nothing to do.
        if not self._index:
            return

        self._lock_acquire()
        try:
            if self._pack_is_in_progress:
                raise FileStorageError('Already packing')
            self._pack_is_in_progress = True
            current_size = self.getSize()
        finally:
            self._lock_release()

        p = FileStoragePacker(self._file_name, stop,
                              self._lock_acquire, self._lock_release,
                              self._commit_lock_acquire,
                              self._commit_lock_release,
                              current_size)
        try:
            opos = None
            try:
                opos = p.pack()
            except RedundantPackWarning, detail:
                logger.info(str(detail))
            if opos is None:
                return
            oldpath = self._file_name + ".old"
            self._lock_acquire()
            try:
                self._file.close()
                try:
                    if os.path.exists(oldpath):
                        os.remove(oldpath)
                    os.rename(self._file_name, oldpath)
                except Exception:
                    self._file = open(self._file_name, 'r+b')
                    raise

                # OK, we're beyond the point of no return
                os.rename(self._file_name + '.pack', self._file_name)
                self._file = open(self._file_name, 'r+b')
                self._initIndex(p.index, p.vindex, p.tindex, p.tvindex,
                                p.oid2tid, p.toid2tid,
                                p.toid2tid_delete)
                self._pos = opos
                self._save_index()
            finally:
                self._lock_release()
        finally:
            if p.locked:
                self._commit_lock_release()
            self._lock_acquire()
            self._pack_is_in_progress = False
            self._lock_release()

class FileStoragePacker(ZODB.FileStorage.fspack.FileStoragePacker):

    def __init__(self, path, stop, la, lr, cla, clr, current_size):
        self._name = path
        # We open our own handle on the storage so that much of pack can
        # proceed in parallel.  It's important to close this file at every
        # return point, else on Windows the caller won't be able to rename
        # or remove the storage file.
        if hasattr(os, 'O_DIRECT'):
            fd = os.open(path, os.O_DIRECT)
            self._file = os.fdopen(fd, 'rb', 1<<20)
        else:
            self._file = open(path, "rb")

        self._path = path
        self._stop = stop
        self.locked = 0
        self.file_end = current_size

        self.gc = GC(self._file, self.file_end, self._stop)

        # The packer needs to acquire the parent's commit lock
        # during the copying stage, so the two sets of lock acquire
        # and release methods are passed to the constructor.
        self._lock_acquire = la
        self._lock_release = lr
        self._commit_lock_acquire = cla
        self._commit_lock_release = clr

        # The packer will use several indexes.
        # index: oid -> pos
        # vindex: version -> pos
        # tindex: oid -> pos, for current txn
        # tvindex: version -> pos, for current txn
        # oid2tid: not used by the packer

        self.index = fsIndex()
        self.vindex = {}
        self.tindex = {}
        self.tvindex = {}
        self.oid2tid = {}
        self.toid2tid = {}
        self.toid2tid_delete = {}

        # Index for non-version data.  This is a temporary structure
        # to reduce I/O during packing
        self.nvindex = fsIndex()
