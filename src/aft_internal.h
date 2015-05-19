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
#include "gni_pub.h"

/*
 * aft typedefs
 */

typedef struct {
	int my_rank;
	uint32_t addr;
	gni_smsg_attr_t  smsg_attr;
} aft_smsg_w_addr_t;

typedef struct {
	gnic_nic_handle_t cdm_hndl;
	gni_nic_handle_t nic;
	gni_cq_handle_t  tx_cq;
	gni_cq_handle_t  rx_cq;
} aft_nic_t;

/*
 * globals
 */

extern aft_nic_t aft_nic;
extern aft_smsg_w_addr_t my_smsg_attr;
extern gni_ep_handle_t *aft_ep_hndls;

/*
 * prototypes for aft internal functions
 */

int aft_cqe_error(gni_cq_entry_t cqe, int peer_rank);
int aft_init(void);


