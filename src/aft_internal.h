/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * This header file contains the common utility functions.
 */

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <malloc.h>
#include <sched.h>

/*
 * aft typedefs
 */

typedef struct {
	gni_mem_handle_t mdh;
	void		 *target_addr;
	gni_ep_handle_t  ep;
} aft_mdh_addr_t;

typedef struct {
	gni_nic_handle_t nic;
	gni_cq_handle_t  tx_cq;
	gni_cq_handle_t  rx_cq;
} aft_nic_t;

/*
 * globals
 */

extern aft_nic_t aft_nic;

/*
 * prototypes for aft internal functions
 */

int aft_cqe_error(gni_cq_entry_t cqe, int peer_rank);

