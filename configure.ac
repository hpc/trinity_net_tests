dnl Process this file with autoconf to produce a configure script.

AC_PREREQ([2.60]))
AC_INIT([aft],[0.9.0])
AC_CONFIG_SRCDIR([src/aft_init.c])
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR(config)
AC_CONFIG_HEADERS(config.h)
AM_INIT_AUTOMAKE([0.9.0 dist-bzip2 foreign -Wall -Werror])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CANONICAL_HOST

dnl Checks for programs
AC_PROG_CC_C99
AM_PROG_LIBTOOL

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h malloc.h stdint.h stdlib.h string.h sys/ioctl.h unistd.h values.h asm/page.h sys/time.h])

AC_ARG_ENABLE([debug],
¬             [AS_HELP_STRING([--enable-debug],
¬       ¬       ¬             [Enable debugging @<:@default=no@:>@])
¬             ],
¬             [CFLAGS="$CFLAGS -g -O0 -Wall"
¬              dbg=1],
¬             [enable_debug=no
               dbg=0])

AC_DEFINE_UNQUOTED([ENABLE_DEBUG],[$dbg],
                   [defined to 1 if aft is configured with --enable-debug, 0 otherwise])

dnl Fix autoconf's habit of adding -g -O2 by default
AS_IF([test -z "$CFLAGS"],
      [CFLAGS='-fvisibility=hidden -O2 -DNDEBUG -Wall'])

# AM PROG_AR did not exist pre AM 1.11.x (where x is somewhere >0 and
# <3), but it is necessary in AM 1.12.x.
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T
AC_C_VOLATILE

PKG_CHECK_MODULES([CRAY_UGNI], [cray-ugni])
PKG_CHECK_MODULES([CRAY_PMI], [cray-pmi])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 include/Makefile
                 tests/Makefile])
AC_OUTPUT
