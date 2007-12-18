/*###########################################################################
 #
 # Copyright (c) 2003 Zope Corporation and Contributors.
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

#include "Python.h"

#include <fcntl.h>

#ifdef POSIX_FADV_DONTNEED

#define OBJECT(O) ((PyObject*)(O))

static PyObject *
py_posix_fadvise(PyObject *self, PyObject *args)
{  
  int fd, advice;
  long long offset, len;
  
  if (! PyArg_ParseTuple(args, "iLLi", &fd, &offset, &len, &advice))
    return NULL; 
  return PyInt_FromLong(posix_fadvise(fd, offset, len, advice));
}

static struct PyMethodDef m_methods[] = {
  {"advise", (PyCFunction)py_posix_fadvise, METH_VARARGS, ""},
  
  {NULL,	 (PyCFunction)NULL, 0, NULL}		/* sentinel */
};


#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
init_zc_FileStorage_posix_fadvise(void)
{
  PyObject *m;
  
  /* Create the module and add the functions */
  m = Py_InitModule3("_zc_FileStorage_posix_fadvise", m_methods, "");
  if (m == NULL)
    return;

  if (PyModule_AddObject(m, "POSIX_FADV_NORMAL",
                         OBJECT(PyInt_FromLong(POSIX_FADV_NORMAL))
                         ) < 0)
    return;
  if (PyModule_AddObject(m, "POSIX_FADV_SEQUENTIAL",
                         OBJECT(PyInt_FromLong(POSIX_FADV_SEQUENTIAL))
                         ) < 0)
    return;

  if (PyModule_AddObject(m, "POSIX_FADV_RANDOM",
                         OBJECT(PyInt_FromLong(POSIX_FADV_RANDOM))
                         ) < 0)
    return;

  if (PyModule_AddObject(m, "POSIX_FADV_WILLNEED",
                         OBJECT(PyInt_FromLong(POSIX_FADV_WILLNEED))
                         ) < 0)
    return;

  if (PyModule_AddObject(m, "POSIX_FADV_DONTNEED",
                         OBJECT(PyInt_FromLong(POSIX_FADV_DONTNEED))
                         ) < 0)
    return;

  if (PyModule_AddObject(m, "POSIX_FADV_NOREUSE",
                         OBJECT(PyInt_FromLong(POSIX_FADV_NOREUSE))
                         ) < 0)
    return;
}

#endif
