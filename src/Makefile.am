# automake Makefile for aft lib component

lib_LTLIBRARIES = libaft.la

libaft_la_LDFLAGS=-version-info $(DSO_VERSION) \
                     $(CRAY_UGNI_LIBS)  \
                     $(CRAY_PMI_LIBS)

libdaft_la_SOURCES = aft_internal.h  \
                     aft_init.c \
                     aft_put.c

if USE_LOCAL_GNI_HEADERS
AM_CFLAGS = -I$(top_srcdir)//include \
            $(CRAY_PMI_CFLAGS) \
            $(CRAY_UGNI_CFLAGS)

