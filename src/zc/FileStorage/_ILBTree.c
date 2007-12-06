/*############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################*/

#define MASTER_ID "$Id: _IIBTree.c 25186 2004-06-02 15:07:33Z jim $\n"

/* IIBTree - int key, int value BTree

   Implements a collection using int type keys
   and int type values
*/

/* Setup template macros */

#define PERSISTENT

#define MOD_NAME_PREFIX "IL"
#define INITMODULE init_ILBTree
#define DEFAULT_MAX_BUCKET_SIZE 120
#define DEFAULT_MAX_BTREE_SIZE 500

#include "BTrees/intkeymacros.h"


#define VALUEMACROS_H "$Id:$\n"

#define NEED_LONG_LONG_SUPPORT
#define VALUE_TYPE PY_LONG_LONG
#define VALUE_PARSE "L"
#define COPY_VALUE_TO_OBJECT(O, K) O=longlong_as_object(K)
#define COPY_VALUE_FROM_ARG(TARGET, ARG, STATUS) \
    if (PyInt_Check(ARG)) TARGET=PyInt_AS_LONG(ARG); else \
        if (longlong_check(ARG)) TARGET=PyLong_AsLongLong(ARG); else \
            if (PyLong_Check(ARG)) { \
                PyErr_SetString(PyExc_ValueError, "long integer out of range"); \
                (STATUS)=0; (TARGET)=0; } \
            else { \
            PyErr_SetString(PyExc_TypeError, "expected integer value");   \
            (STATUS)=0; (TARGET)=0; }


#undef VALUE_TYPE_IS_PYOBJECT
#define TEST_VALUE(K, T) (((K) < (T)) ? -1 : (((K) > (T)) ? 1: 0)) 
#define VALUE_SAME(VALUE, TARGET) ( (VALUE) == (TARGET) )
#define DECLARE_VALUE(NAME) VALUE_TYPE NAME
#define DECREF_VALUE(k)
#define INCREF_VALUE(k)
#define COPY_VALUE(V, E) (V=(E))

#define NORMALIZE_VALUE(V, MIN) ((MIN) > 0) ? ((V)/=(MIN)) : 0

#define MERGE_DEFAULT 1
#define MERGE(O1, w1, O2, w2) ((O1)*(w1)+(O2)*(w2))
#define MERGE_WEIGHT(O, w) ((O)*(w))


#include "BTrees/BTreeModuleTemplate.c"
