##############################################################################
#
# Copyright (c) Zope Corporation and Contributors.
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

class MRU:

    def __init__(self, size, evicted=lambda k, v: None):
        assert size > 0
        self.size = size
        self.evicted = evicted
        self.data = {}
        self.top = _node()

    def clear(self):
        while self.data:
            self.pop()
    
    def __len__(self):
        return len(self.data)

    def __iter__(self):
        # We van't do a generator. We have to take a snapshot, otherwise
        # the client might do operations that would change the order!
        result = []
        top = node = self.top
        while 1:
            node = node.previous
            if node is top:
                break
            result.append(node.key)
        return iter(result)

    def get(self, key, default=None):
        node = self.data.get(key)
        if node is None:
            return default
        if node.next != self.top:
            node.unlink()
            node.link(self.top)
        return node.value

    def __getitem__(self, key):
        result = self.get(key, self)
        if result is not self:
            return result
        raise KeyError(key)

    def __setitem__(self, key, value):
        assert value is not self
        data = self.data
        node = data.get(key)
        if node is None:
            node = _node(self.top)
            data[key] = node
            node.key = key
            if len(data) > self.size:
                self.pop()
        node.value = value

    def pop(self):
        doomed = self.top.next
        self.evicted(doomed.key, doomed.value)
        del self.data[doomed.key]
        doomed.unlink()
        

class _node:

    next = previous = key = value = None

    def __init__(self, next=None):
        if next is None:
            next = self
        self.link(next)

    def link(self, next):
        self.next = next
        self.previous = next.previous
        next.previous = self
        self.previous.next = self

    def unlink(self):
        self.next.previous = self.previous
        self.previous.next = self.next
        
