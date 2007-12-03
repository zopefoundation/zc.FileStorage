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
import BTrees.LLBTree

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
                              current_size, referencesf)
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

    def __init__(self, path, stop, la, lr, cla, clr, current_size, referencesf):
        self._name = path
        # We open our own handle on the storage so that much of pack can
        # proceed in parallel.  It's important to close this file at every
        # return point, else on Windows the caller won't be able to rename
        # or remove the storage file.

        # We set the buffer quite high (32MB) to try to reduce seeks
        # when the storage is disk is doing other io
        self._file = open(path, "rb", 1<<25)



        self._path = path
        self._stop = stop
        self.locked = 0
        self.file_end = current_size

        # The packer needs to acquire the parent's commit lock
        # during the copying stage, so the two sets of lock acquire
        # and release methods are passed to the constructor.
        self._lock_acquire = la
        self._lock_release = lr
        self._commit_lock_acquire = cla
        self._commit_lock_release = clr

        # The packer will use several indexes.
        # index: oid -> pos
        # tindex: oid -> pos, for current txn
        # oid2tid: not used by the packer

        self.index = BTrees.fsBTree.fsIndex()
        self.tindex = {}
        self.oid2tid = {}
        self.toid2tid = {}
        self.toid2tid_delete = {}

        self.referencesf = referencesf

    def pack(self):
        packed, index, references, packpos = self.buildPackIndex(
            self._stop, self.file_end)
        is packed:
            # nothing to do
            self._file.close()
            return None
        
        self.updateReferences(references, packpos, self.file_end)
        index = self.gc(index, references)

        output = open(self._name + ".pack", "w+b", 1<<25)
        index, new_pos = self.copyToPacktime(packpos, index, output)
        if new_pos == packpos:
            # pack didn't free any data.  there's no point in continuing.
            self._file.close()
            output.close()
            os.remove(self._name + ".pack")
            return None

        new_pos = self.copyFromPacktime(packpos, self.file_end, output, index)
        
        self._commit_lock_acquire()
        self.locked = 1
        self._lock_acquire()
        try:
            # Re-open the file in unbuffered mode.

            # The main thread may write new transactions to the file,
            # which creates the possibility that we will read a status
            # 'c' transaction into the pack thread's stdio buffer even
            # though we're acquiring the commit lock.  Transactions
            # can still be in progress throughout much of packing, and
            # are written to the same physical file but via a distinct
            # Python file object.  The code used to leave off the
            # trailing 0 argument, and then on every platform except
            # native Windows it was observed that we could read stale
            # data from the tail end of the file.
            self._file.close()  # else self.gc keeps the original alive & open
            self._file = open(self._path, "rb", 0)
            self._file.seek(0, 2)
            self.file_end = self._file.tell()
        finally:
            self._lock_release()
        if ipos < self.file_end:
            self.copyRest(ipos)

        # OK, we've copied everything. Now we need to wrap things up.
        pos = self._tfile.tell()
        self._tfile.flush()
        self._tfile.close()
        self._file.close()

        return pos


    def buildPackIndex(self, stop, file_end):
        index = BTrees.fsBTree.fsIndex()
        references = BTrees.LOBTree.LOBTree()
        pos = 4L
        packed = True
        while pos < file_end:
            th = self._read_txn_header(pos)
            if th.tid > stop:
                break
            self.checkTxn(th, pos)
            if th.status != "p":
                packed = False

            tpos = pos
            end = pos + th.tlen
            pos += th.headerlen()

            while pos < end:
                dh = self._read_data_header(pos)
                self.checkData(th, tpos, dh, pos)
                if dh.version:
                    self.fail(pos, "Versions are not supported")
                index[dh.oid] = pos
                refs = self._refs(dh)
                if refs:
                    references[oid] = refs
                
                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          tlen, th.tlen)
            pos += 8

        return packed, index, references, pos

    def updateReferences(self, references, pos, file_end):
        while pos < file_end:
            th = self._read_txn_header(pos)
            self.checkTxn(th, pos)

            tpos = pos
            end = pos + th.tlen
            pos += th.headerlen()

            while pos < end:
                dh = self._read_data_header(pos)
                self.checkData(th, tpos, dh, pos)
                if dh.version:
                    self.fail(pos, "Versions are not supported")
                refs = self._refs(dh, self.references.get(oid))
                if refs:
                    references[oid] = refs
                
                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          tlen, th.tlen)
            pos += 8

    def _refs(self, dh, initial=None):
        # Chase backpointers until we get to the record with the refs
        while dh.back:
            dh = self._read_data_header(dh.back)

        if not dh.plen:
            return initial
        
        refs = self.referencesf(self._file.read(dh.plen))
        if not refs:
            return initial
            
        if not initial:
            initial = BTrees.LLBTree.LLSet()
        initial.update(u64(oid) for oid in refs)
        result = BTrees.LLBTree.LLSet()
        result.__setstate___((tuple(initial),))
        return result

    def gc(self, index, references):
        to_do = [0]
        reachable = BTrees.fsBTree.fsIndex()
        while to_do:
            ioid = to_do.pop()
            oid = p64(ioid)
            if oid in reachable:
                continue
            reachable[oid] = index.pop(oid)
            to_do.extend(references.pop(ioid, ()))
        references.clear()
        return reachable

    def copyToPacktime(self, packpos, index, output):
        pos = new_pos = self._metadata_size
        output.write(self._file.read(self._metadata_size))
        new_index = BTrees.fsBTree.fsIndex()

        while pos < packpos:
            th = self._read_txn_header(pos)
            new_tpos = 0L
            tend = pos + th.tlen
            pos += th.headerlen()
            while pos < tend:
                h = self._read_data_header(pos)
                if index.get(h.oid) != pos:
                    pos += h.recordlen()
                    continue

                pos += h.recordlen()

                # If we are going to copy any data, we need to copy
                # the transaction header.  Note that we will need to
                # patch up the transaction length when we are done.
                if not new_tpos:
                    th.status = "p"
                    new_tpos = output.tell()
                    output.write(th.asString())

                if h.plen:
                    data = self._file.read(h.plen)
                else:
                    # If a current record has a backpointer, fetch
                    # refs and data from the backpointer.  We need
                    # to write the data in the new record.
                    data = self.fetchBackpointer(h.oid, h.back) or ''

                h.prev = 0
                h.back = 0
                h.plen = len(data)
                h.tloc = new_tpos
                new_index[h.oid] = output.tell()
                output.write(h.asString())
                output.write(data)
                if not data:
                    # Packed records never have backpointers (?).
                    # If there is no data, write a z64 backpointer.
                    # This is a George Bailey event.
                    output.write(z64)

            if new_tpos:
                new_pos = output.tell()
                tlen = p64(new_pos - new_tpos)
                output.write(tlen)
                new_pos += 8

                # Update the transaction length
                output.seek(new_tpos + 8)
                output.write(tlen)
                output.seek(new_pos)

            pos += 8

        return new_index, new_pos

    def copyFromPacktime(self, pos, file_end, output, index):
        while pos < file_end:
            th = self._read_txn_header(pos)
            new_tpos = output.tell()
            output.write(th.asString())
            tend = pos + th.tlen
            pos += th.headerlen()
            while pos < tend:
                h = self._read_data_header(pos)

                prev_txn = None
                if h.plen:
                    data = self._file.read(h.plen)
                else:
                    # If a current record has a backpointer, fetch
                    # refs and data from the backpointer.  We need
                    # to write the data in the new record.
                    data = self.fetchBackpointer(h.oid, h.back)
                    if h.back:
                        prev_txn = self.getTxnFromData(h.oid, h.back)

                if h.version:
                    self.fail(pos, "Versions are not supported.")

                self._copier.copy(h.oid, h.tid, data, h.version, prev_txn,
                                  new_tpos, output.tell())


                pos += h.recordlen()

            new_pos = output.tell()
            tlen = p64(new_pos - new_tpos)
            output.write(tlen)
            new_pos += 8

            if tlen != h.tlen:
                # Update the transaction length
                output.seek(new_tpos + 8)
                output.write(tlen)
                output.seek(new_pos)

            pos += 8

        return new_index, new_pos



    def copyOne(self, ipos):
        # The call below will raise CorruptedDataError at EOF.
        th = self._read_txn_header(ipos)
        self._lock_counter += 1
        if self._lock_counter % 20 == 0:
            self._commit_lock_release()
        pos = self._tfile.tell()
        self._copier.setTxnPos(pos)
        self._tfile.write(th.asString())
        tend = ipos + th.tlen
        ipos += th.headerlen()

        while ipos < tend:
            h = self._read_data_header(ipos)
            ipos += h.recordlen()
            prev_txn = None
            if h.plen:
                data = self._file.read(h.plen)
            else:
                data = self.fetchBackpointer(h.oid, h.back)
                if h.back:
                    prev_txn = self.getTxnFromData(h.oid, h.back)

            if h.version:
                self.fail(ipos, "Versions are not supported.")

            self._copier.copy(h.oid, h.tid, data, prev_txn, pos,
                              self._tfile.tell())

        tlen = self._tfile.tell() - pos
        assert tlen == th.tlen
        self._tfile.write(p64(tlen))
        ipos += 8

        self.index.update(self.tindex)
        self.tindex.clear()
        if self._lock_counter % 20 == 0:
            self._commit_lock_acquire()
        return ipos


