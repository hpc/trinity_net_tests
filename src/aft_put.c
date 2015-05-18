/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * RDMA Put test example - this test only uses PMI
 * 
 * Note: this test should not be run oversubscribed on nodes, i.e. more instances
 * on a given node than cpus, owing to the busy wait for incoming data.
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
#include <sys/utsname.h>
#include <errno.h>
#include "gni_pub.h"
#include "pmi.h"
#include "aft_internal.h"

/*
 * simple uni-directional ping test using the
 * Aries BTE to do an RDMA write of a given
 * transferi size for a number of iterations.
 */

int
aft_ping(int niters, int peer_rank, size_t tlen, uint16_t dlvr_mode)
{
	int i, my_rank, rc;
	gni_post_descriptor_t put_desc;
	gni_post_descriptor_t cq_desc;
	gni_cq_entry_t cq_entry;
	aft_mdh_addr_t peer_mdh_addr;
	char *send_buffer;
	uint64_t the_cq_data;

	rc = PMI_Get_rank(&my_rank);
	if (rc != PMI_SUCCESS) {
		ret = aft_pmi_err_to_aft_err(rc);
		goto err;
	}

	rc = aft_get_peer_mdh_addr(peer_rank, &peer_mdh_addr);
	if (rc != AFT_SUCCESS)
		goto err;

	rc = posix_memalign((void **)&send_buffer, 64,
			    (tlen * niters));
	if (rc != 0)
		goto err;

	/*
	 * Initialize the buffer to a value
	 */

	memset(send_buffer, 8, (tlen * niters));

	status = GNI_MemRegister(nic_handle,
				 (uint64_t) send_buffer,
				 (tlen * niters),
				 NULL,
				 GNI_MEM_READWRITE,
				 -1,
				 &source_memory_handle);
	if (status != GNI_RC_SUCCESS) {
		rc = gni_err_to_aft_err(status);
		goto err;
	}

	/*
	 * sync with my partner using CQ's
	 */

	cq_desc.type = GNI_POST_CQWRITE;
	cq_desc.cqwrite_value = my_rank;
	cq_desc.remote_mem_hndl  = peer_mdh_addr.mdh;
	cq_desc.cq_mode = GNI_CQMODE_GLOBAL_EVENT;
	cq_desc.dlvr_mode = GNI_DLVMODE_IN_ORDER;
	cq_desc.src_cq_hndl = aft_nic.tx_cq;
	cq_desc.post_id  = (uint64_t)&pdesc;

	for (i  0; i < 64; i++) {

		cq_desc.cqwrite_value = (my_rank | (i << 32));
		status = GNI_CqWrite(peer_mdh_addr.ep,
				     &cq_desc);
		if (status != GNI_RC_SUCCESS) {
			rc = aft_gni_err_to_aft_err(status);
			goto err;
		}

		status = GNI_RC_NOT_DONE;
		while (1) {
			GNI_CqGetEvent(aft_nic.tx_cq,
					&cqe_entry);
			if (status == GNI_RC_SUCCESS)
				break;
			if (status == GNI_RC_TRANSACTION_ERROR) {
				aft_cqe_error(cqe_entry, peer_rank);
				goto err;
			}
		}
		while (1) {
			GNI_CqGetEvent(aft_nic.rx_cq,
					&cqe_entry);
			if (status == GNI_RC_SUCCESS) {
				the data = GNI_CQ_GET_DATA(cqe_entry);
				assert (the_cq_data ==
					(peer_rank | ( i << 32)));
				break;
			}
			if (status == GNI_RC_TRANSACTION_ERROR) {
				aft_cqe_error(cqe_entry, peer_rank);
				goto err;
			}
		}
	}

	put_desc.type = GNI_POST_RDMA_PUT;
	put_desc.cq_mode = GNI_CQMODE_GLOBAL_EVENT |
				GNI_CQMODE_REMOTE_EVENT;
	put_desc.dlvr_mode = dlvr_mode;
	put_desc.local_addr = (uint64_t) send_buffer;
	put_desc.local_mem_hndl = source_memory_handle;
	put_desc.remote_mem_hndl = peer_mdh_addr.mdh;
	put_desc.length = tlen;
	put_desc.src_cq_hndl = aft_nic.tx_cq;
	put_desc.rdma_mode = 0;
	put_desc.post_id = (uint64_t) &put_desc;

	for (i = 0; i < niters; i++) {

		put_desc.local_addr += i * tlen;
		put_desc.remote_addr = peer_mdh_addr.addr;
		put_desc.remote_addr += i * tlen;

		if (my_rank < send_to) {

			start_timer;
			/*
			 * Send the data.
			 */

			status = GNI_PostRdma(peer_mdh_addr.ep,
					      &put_desc);
			if (status != GNI_RC_SUCCESS)
				goto err;

			status = GNI_RC_NOT_DONE;
			while (status != GNI_RC_SUCCESS)
				status = GNI_CqGetEvent(tx_cq, &current_event);
			check_tx_cqe(current_event);

			/*
			 * wait for rx CQE from peer
			 */

			status = GNI_RC_NOT_DONE;
			while (status != GNI_RC_SUCCESS)
				status = GNI_CqGetEvent(rx_cq, &current_event);
			check_rx_cqe(current_event);

			stop_timer;

		} else {
			/*
			 * wait for RX CQE
			 */

			/*
			 * send the data
			 */

			/*
			 * wait for TX CQE
			 */
		}
	}			/* end of for loop for niters */

	return rc;
}
