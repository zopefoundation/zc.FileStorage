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

import cPickle
import logging
import os
import subprocess
import sys
import time

from ZODB.FileStorage.format import FileStorageFormatter, CorruptedDataError
from ZODB.utils import p64, u64, z64
from ZODB.FileStorage.format import TRANS_HDR_LEN

import ZODB.FileStorage
import ZODB.FileStorage.fspack
import ZODB.fsIndex
import ZODB.TimeStamp

GIG = 1<<30

def Packer(sleep=0, transform=None, untransform=None):
    def packer(storage, referencesf, stop, gc):
        return FileStoragePacker(storage, stop, sleep, transform, untransform
                                 ).pack()
    return packer

packer  = Packer(0)
packer1 = Packer(1)
packer2 = Packer(2)
packer4 = Packer(3)
packer8 = Packer(4)

class FileStoragePacker(FileStorageFormatter):

    def __init__(self, storage, stop,
                 sleep=0, transform=None, untransform=None):
        self.storage = storage
        self._name = path = storage._file.name
        self.sleep = sleep
        self.transform_option = transform
        self.untransform_option = untransform

        # We open our own handle on the storage so that much of pack can
        # proceed in parallel.  It's important to close this file at every
        # return point, else on Windows the caller won't be able to rename
        # or remove the storage file.
        self._file = open(path, "rb")

        self._stop = stop
        self.locked = 0

        # The packer needs to acquire the parent's commit lock
        # during the copying stage, so the two sets of lock acquire
        # and release methods are passed to the constructor.
        self._lock_acquire = storage._lock_acquire
        self._lock_release = storage._lock_release
        self._commit_lock_acquire = storage._commit_lock_acquire
        self._commit_lock_release = storage._commit_lock_release

        self._lock_acquire()
        try:
            storage._file.seek(0, 2)
            self.file_end = storage._file.tell()
        finally:
            self._lock_release()

        self.ltid = z64

    def pack(self):

        script = self._name+'.packscript'
        open(script, 'w').write(pack_script_template % dict(
            path = self._name,
            stop = self._stop,
            size = self.file_end,
            syspath = sys.path,
            blob_dir = self.storage.blob_dir,
            sleep = self.sleep,
            transform = self.transform_option,
            untransform = self.untransform_option,
            ))
        for name in 'error', 'log':
            name = self._name+'.pack'+name
            if os.path.exists(name):
                os.remove(name)
        proc = subprocess.Popen(
            (sys.executable, script),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            close_fds=True,
            )

        proc.stdin.close()
        out = proc.stdout.read()
        if proc.wait():
            if os.path.exists(self._name+'.packerror'):
                v = cPickle.Unpickler(open(self._name+'.packerror', 'rb')
                                      ).load()
                os.remove(self._name+'.packerror')
                raise v
            raise RuntimeError('The Pack subprocess failed\n'
                               +'-'*60+out+'-'*60+'\n')

        packindex_path = self._name+".packindex"
        if not os.path.exists(packindex_path):
            return # already packed or pack didn't benefit

        index, opos = cPickle.Unpickler(open(packindex_path, 'rb')).load()
        os.remove(packindex_path)
        os.remove(self._name+".packscript")

        output = open(self._name + ".pack", "r+b")
        output.seek(0, 2)
        assert output.tell() == opos
        self.copyRest(self.file_end, output, index)

        # OK, we've copied everything. Now we need to wrap things up.
        pos = output.tell()
        output.close()

        return pos, index

    def copyRest(self, input_pos, output, index):
        # Copy data records written since packing started.

        self._commit_lock_acquire()
        self.locked = 1
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
        self._file = open(self._name, "rb", 0)
        try:
            try:
                while 1:
                    # The call below will raise CorruptedDataError at EOF.
                    input_pos = self._copyNewTrans(
                        input_pos, output, index,
                        self._commit_lock_acquire, self._commit_lock_release)
            except CorruptedDataError, err:
                # The last call to copyOne() will raise
                # CorruptedDataError, because it will attempt to read past
                # the end of the file.  Double-check that the exception
                # occurred for this reason.
                self._file.seek(0, 2)
                endpos = self._file.tell()
                if endpos != err.pos:
                    raise
        finally:
            self._file.close()

    transform = None
    def _copyNewTrans(self, input_pos, output, index,
                      acquire=None, release=None):
        tindex = {}
        copier = PackCopier(output, index, tindex)
        th = self._read_txn_header(input_pos)
        if release is not None:
            release()

        transform = self.transform
        start_time = time.time()
        output_tpos = output.tell()
        copier.setTxnPos(output_tpos)
        output.write(th.asString())
        tend = input_pos + th.tlen
        input_pos += th.headerlen()
        while input_pos < tend:
            h = self._read_data_header(input_pos)
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

            if data and (transform is not None):
                data = transform(data)
            copier.copy(h.oid, h.tid, data, prev_txn,
                        output_tpos, output.tell())

            input_pos += h.recordlen()

        output_pos = output.tell()
        tlen = p64(output_pos - output_tpos)
        output.write(tlen)
        output_pos += 8

        if tlen != th.tlen:
            # Update the transaction length
            output.seek(output_tpos + 8)
            output.write(tlen)
            output.seek(output_pos)

        index.update(tindex)
        tindex.clear()
        time.sleep((time.time()-start_time)*self.sleep)

        if acquire is not None:
            acquire()

        return input_pos + 8

    def fetchBackpointer(self, oid, back):
        if back == 0:
            return None
        data, tid = self._loadBackTxn(oid, back, 0)
        return data

class PackCopier(ZODB.FileStorage.fspack.PackCopier):

    def _txn_find(self, tid, stop_at_pack):
        # _pos always points just past the last transaction
        pos = self._pos
        while pos > 4:
            self._file.seek(pos - 8)
            pos = pos - u64(self._file.read(8)) - 8
            self._file.seek(pos)
            h = self._file.read(TRANS_HDR_LEN)
            _tid = h[:8]
            if _tid == tid:
                return pos
            if stop_at_pack:
                if h[16] == 'p':
                    break

        return None


pack_script_template = """

import sys, logging

sys.path[:] = %(syspath)r

import cPickle
import zc.FileStorage

logging.getLogger().setLevel(logging.INFO)
handler = logging.FileHandler(%(path)r+'.packlog')
handler.setFormatter(logging.Formatter(
   '%%(asctime)s %%(name)s %%(levelname)s %%(message)s'))
logging.getLogger().addHandler(handler)

try:
    packer = zc.FileStorage.PackProcess(%(path)r, %(stop)r, %(size)r,
                                        %(blob_dir)r, %(sleep)s,
                                        %(transform)r, %(untransform)r)
    packer.pack()
except Exception, v:
    logging.exception('packing')
    try:
        v = cPickle.dumps(v)
    except Exception:
        pass
    else:
        open(%(path)r+'.packerror', 'w').write(v)
    raise
"""

class PackProcess(FileStoragePacker):

    def __init__(self, path, stop, current_size, blob_dir,
                 sleep, transform, untransform):
        self._name = path
        # We open our own handle on the storage so that much of pack can
        # proceed in parallel.  It's important to close this file at every
        # return point, else on Windows the caller won't be able to rename
        # or remove the storage file.

        if blob_dir:
            self.pack_blobs = True
            self.blob_removed = open(os.path.join(blob_dir, '.removed'), 'w')
        else:
            self.pack_blobs = False

        self._file = open(path, "rb")

        self._name = path
        self._stop = stop
        self.locked = 0
        self.file_end = current_size

        self.ltid = z64

        self._freecache = _freefunc(self._file)
        self.sleep = sleep
        if isinstance(transform, str):
            transform = getglobal(transform)
        self.transform = transform
        if isinstance(untransform, str):
            untransform = getglobal(untransform)
        self.untransform = untransform
        logging.info('packing to %s, sleep %s',
                     ZODB.TimeStamp.TimeStamp(self._stop),
                     self.sleep)


    def _read_txn_header(self, pos, tid=None):
        self._freecache(pos)
        return FileStoragePacker._read_txn_header(self, pos, tid)

    def pack(self):
        packed, index, packpos = self.buildPackIndex(self._stop, self.file_end)
        logging.info('initial scan %s objects at %s', len(index), packpos)
        if packed:
            # nothing to do
            logging.info('done, nothing to do')
            self._file.close()
            return

        logging.info('copy to pack time')
        output = open(self._name + ".pack", "w+b")
        self._freeoutputcache = _freefunc(output)
        index, new_pos = self.copyToPacktime(packpos, index, output)
        if new_pos == packpos:
            # pack didn't free any data.  there's no point in continuing.
            self._file.close()
            output.close()
            os.remove(self._name + ".pack")
            logging.info('done, no decrease')
            return

        logging.info('copy from pack time')
        self._freecache = self._freeoutputcache = lambda pos: None
        self.copyFromPacktime(packpos, self.file_end, output, index)

        # Save the index so the parent process can use it as a starting point.
        f = open(self._name + ".packindex", 'wb')
        cPickle.Pickler(f, 1).dump((index, output.tell()))
        f.close()
        output.flush()
        os.fsync(output.fileno())
        output.close()
        self._file.close()
        logging.info('packscript done')


    def buildPackIndex(self, stop, file_end):
        index = ZODB.fsIndex.fsIndex()
        pos = 4L
        packed = True
        log_pos = pos

        while pos < file_end:
            start_time = time.time()
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
                if dh.plen or dh.back:
                    index[dh.oid] = pos
                else:
                    # deleted
                    if dh.oid in index:
                        del index[dh.oid]
                pos += dh.recordlen()

            tlen = self._read_num(pos)
            if tlen != th.tlen:
                self.fail(pos, "redundant transaction length does not "
                          "match initial transaction length: %d != %d",
                          tlen, th.tlen)
            pos += 8

            if pos - log_pos > GIG:
                logging.info("read %s" % pos)
                log_pos = pos

            time.sleep((time.time()-start_time)*self.sleep)

        return packed, index, pos

    def copyToPacktime(self, packpos, index, output):
        pos = new_pos = self._metadata_size
        self._file.seek(0)
        output.write(self._file.read(self._metadata_size))
        new_index = ZODB.fsIndex.fsIndex()
        pack_blobs = self.pack_blobs
        transform = self.transform
        untransform = self.untransform
        if untransform is None:
            is_blob_record = ZODB.blob.is_blob_record
        else:
            _is_blob_record = ZODB.blob.is_blob_record
            def is_blob_record(data):
                return _is_blob_record(untransform(data))

        log_pos = pos

        while pos < packpos:
            start_time = time.time()
            th = self._read_txn_header(pos)
            new_tpos = 0L
            tend = pos + th.tlen
            pos += th.headerlen()
            while pos < tend:
                h = self._read_data_header(pos)
                if index.get(h.oid) != pos:
                    pos += h.recordlen()
                    if pack_blobs:
                        if h.plen:
                            data = self._file.read(h.plen)
                        else:
                            data = self.fetchDataViaBackpointer(h.oid, h.back)
                        if data and is_blob_record(data):
                            # We need to remove the blob record. Maybe we
                            # need to remove oid.

                            # But first, we need to make sure the
                            # record we're looking at isn't a dup of
                            # the current record. There's a bug in ZEO
                            # blob support that causes duplicate data
                            # records.
                            rpos = index.get(h.oid)
                            is_dup = (rpos and
                                      self._read_data_header(rpos).tid == h.tid)
                            if not is_dup:
                                # Note that we delete the revision.
                                # If rpos was None, then we could
                                # remove the oid.  What if somehow,
                                # another blob update happened after
                                # the deletion. This shouldn't happen,
                                # but we can leave it to the cleanup
                                # code to take care of removing the
                                # directory for us.
                                self.blob_removed.write(
                                    (h.oid+h.tid).encode('hex')+'\n')

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

                if transform is not None:
                    data = self.transform(data)

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

                if tlen != th.tlen:
                    # Update the transaction length
                    output.seek(new_tpos + 8)
                    output.write(tlen)
                    output.seek(new_pos)

                self._freeoutputcache(new_pos)


            pos += 8

            if pos - log_pos > GIG:
                logging.info("read %s" % pos)
                log_pos = pos

            time.sleep((time.time()-start_time)*self.sleep)

        return new_index, new_pos

    def fetchDataViaBackpointer(self, oid, back):
        """Return the data for oid via backpointer back

        If `back` is 0 or ultimately resolves to 0, return None.
        In this case, the transaction undoes the object
        creation.
        """
        if back == 0:
            return None
        data, tid = self._loadBackTxn(oid, back, 0)
        return data

    def copyFromPacktime(self, pos, file_end, output, index):

        log_pos = pos
        while pos < file_end:
            start_time = time.time()
            pos = self._copyNewTrans(pos, output, index)
            self._freeoutputcache(output.tell())

            if pos - log_pos > GIG:
                logging.info("read %s" % pos)
                log_pos = pos

            time.sleep((time.time()-start_time)*self.sleep)
        return pos

def getglobal(s):
    module, expr = s.split(':', 1)
    return eval(expr, __import__(module, {}, {}, ['*']).__dict__)


def _freefunc(f):
    # Return an posix_fadvise-based cache freeer.

    try:
        import _zc_FileStorage_posix_fadvise
    except ImportError:
        return lambda pos: None

    fd = f.fileno()
    last = [0]
    def _free(pos):
        if pos == 4:
            last[0] = 0
        elif (pos - last[0]) < 50000000:
            return

        last[0] = pos
        _zc_FileStorage_posix_fadvise.advise(
            fd, 0, last[0]-10000,
            _zc_FileStorage_posix_fadvise.POSIX_FADV_DONTNEED)

    return _free
