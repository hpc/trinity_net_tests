#
# Copyright 2011 Cray Inc.  All Rights Reserved.
#

SHELL   = /bin/sh
CC = gcc

SRCS = cq_write_pmi_example.c \
	ce_pmi_example.c \
	datagram_post_pmi_example.c \
	fma_amo_pmi_example.c \
	fma_amo_fp_pmi_example.c \
	fma_amo_g2_b32_fp_pmi_example.c \
	fma_amo_g2_b32_pmi_example.c \
	fma_amo_g2_b64_pmi_example.c \
	fma_get_pmi_example.c \
	fma_put_pmi_example.c \
	memory_registration_pmi_example.c \
	msgq_send_pmi_example.c \
	rdma_get_pmi_example.c \
	rdma_put_pmi_example.c \
	smsg_send_pmi_example.c \
        rdma_put_simple.c

PGMS	= $(SRCS:.c=)
OBJS	= $(SRCS:.c=.o)

PMI_CFLAGS = $(shell pkg-config --cflags cray-pmi)
PMI_LIBS = $(shell pkg-config --libs cray-pmi)
UGNI_CFLAGS = $(shell pkg-config --cflags cray-ugni)
UGNI_LIBS = $(shell pkg-config --libs cray-ugni)

all: $(PGMS)

$(PGMS): $(SRCS)
	$(CC) $(CFLAGS) $(PMI_CFLAGS) $(UGNI_CFLAGS) $(PMI_LIBS) $(UGNI_LIBS) -o $@ $@.c 

clean:
	rm -f core $(PGMS) *.o
