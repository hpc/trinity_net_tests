
#include "aft_internal.h"

aft_nic_t aft_nic;
aft_smsg_w_addr_t my_smsg_attr;
gni_ep_handle_t *aft_ep_hndls;

int aft_init(int cdm_modes)
{
	int spawned;
	int i, rc, my_rank, nranks;
	int device_id = 0; /* only 1 aries nic/node */
	uint32_t bytes_per_smsg;
	gni_smsg_attr_t smsg_attr;
	gni_smsg_attr_t *all_smsg_attrs = NULL;
#if 0
	uint32_t modes = GNI_CDM_MODE_USE_PCI_IOMMU;
#define GNI_CDM_MODE_FLBTE_DISABLE          0x00100000
#endif
	gni_smsg_attr_t my_smsg_attr = {0};

	/*
	 * Fire up PMI
	 */

	rc = PMI_Init(&first_spawned);
	if (rc != PMI_SUCCESS) {
		AFT_WARN("PMI_Init returned %d\n",rc);
		goto err;
	}

	rc = PMI_Get_size(&nranks);
	if (rc != PMI_SUCCESS) {
		AFT_WARN("PMI_Get_size returned %d\n",rc);
		goto err;
	}

	rc = PMI_Get_rank(&my_rank);
	if (rc != PMI_SUCCESS) {
		AFT_WARN("PMI_Get_size returned %d\n",rc);
		goto err;
	}

	/*
	 * Get the GNI RDMA credentials from PMI
	 */

	ptag = __get_ptag();
	cookie = __get_cookie();

	status = GNI_CdmCreate(my_rank,
			       ptag,
			       cookie,
			       cdm_modes,
			       &aft_nic.cdm_hndl);
	if (status != GNI_RC_SUCCESS) {
		AFT_WARN("GNI_CdmCreate returned %s\n",
			gni_err_str[status]);
		goto err;
	}

	status = GNI_CdmAttach(aft_nic.cdm_hndl,
			       device_id,
			       &local_address,
			       &aft_nic.nic);
	if (status != GNI_RC_SUCCESS) {
		AFT_WARN("GNI_CdmAttach returned %s\n",
			gni_err_str[status]);
		goto err1;
	}

	my_smsg_attr.rank = my_rank;
	my_smsg_attr.addr = local_address;

	/*
	 * create a TX CQ
	 */

	status = GNI_CqCreate(aft_nic.nic,
			      number_of_cq_entries,
			      0,
			      GNI_CQ_NOBLOCK | GNI_CQ_PHYS_PAGES,
			      NULL,
			      NULL,
			      &aft_nic.tx_cq);
	if (status != GNI_RC_SUCCESS) {
		AFT_WARN("GNI_CqCreate returned %s\n",
			gni_err_str[status]);
		goto err1;
	}

	/*
	 * create a RX CQ
	 */
        status = GNI_CqCreate(aft_nic.nic,
			      number_of_rx_cq_entries,
			      GNI_CQ_NOBLOCK | GNI_CQ_PHYS_PAGES,
			      NULL,
			      NULL,
			      &aft_nic.rx_cq);
	if (status != GNI_RC_SUCCESS) {
		AFT_WARN("GNI_CqCreate returned %s\n",
			gni_err_str[status]);
		goto err2;
	}

	/*
	 * set up space for mailboxes - these don't need to
	 * be high performance, just enough to exchange some
	 * memhndl/vaddr info
	 */

	smsg_attr.msg_type = GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT;
	smsg_attr.mbox_maxcredit = 16;
	smsg_attr.msg_maxsize = 512;

	status = GNI_SmsgBufferSizeNeeded(&my_smsg_attr,
					  &bytes_per_smsg);

	smsg_attr.msg_buffer = malloc(bytes_per_smsg * nranks);
	if (smsg_buffer == NULL) {
		AFT_WARN("malloc of smsg space failed\n");
		goto err3;
	}

	/*
	 * now register the smsg buffer
	 */

	status = GNI_MemRegister(aft_nic.nic,
				 my_smsg_attr.msg_buffer,
				 bytes_per_smsg * nranks,
				 NULL,
				 GNI_MEM_READWRITE,
				 -1,
				 &my_smsg_attr.mem_hndl);
	if (status != GNI_RC_SUCCESS) {
		AFT_WARN("GNI_MemRegister returned %s\n",
			gni_err_str[status]);
		goto err3;
	}

	/*
	 * now we can gather addresses and smsg_attr's
	 */

	memcpy(&my_smsg_attr.smsg_attr,
		&smsg_attr, sizeof(aft_smsg_w_addr_t));

	all_smsg_attrs = malloc(nranks * sizeof(my_smsg_attr));
	if (all_smsg_attrs == NULL) {
		AFT_WARN("malloc of %lu failed\n",
			 nranks * sizeof(my_smsg_attr));
		goto err3;
	}

	rc = PMI_Allgather(&my_smsg_attr,
			   all_smsg_attrs,
                           sizeof(my_smsg_attr));
	if (rc != PMI_SUCCESS) {
		AFT_WARN("PMI_Allgather returned %d\n", rc);
		goto err3;
	}

	/*
	 * Set up the endpoints
	 */

	aft_ep_hndls  = malloc(nranks * sizeof(gni_ep_handle_t));
	if (ep_hndls == NULL) {
		AFT_WARN("calloc of ep_hndls failed\n");
		goto err3;
	}

	i = 0;
	while(i < nranks) {
		the_rank = all_smsg_attrs[i].my_rank;
        	status = GNI_EpCreate(aft_nic.nic,
					aft_nic.tx_cq,
					&aft_ep_hndls[the_rank]);
		if (status != GNI_RC_SUCCESS) {
			AFT_WARN("GNI_MemRegister returned %s\n",
				gni_err_str[status]);
			goto err3;
		}
		all_smsg_attrs[i].smsg_attr.mbox_offset = bytes_per_smsg * the_rank;

		smsg_attr.msg_type = GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT;
		smsg_attr.mbox_maxcredit = 16;
		smsg_attr.msg_maxsize = 512;
		smsg_attr.mbox_offset = bytes_per_smsg * the_rank;

		status = GNI_SmsgInit(aft_ep_hndls[the_rank],
					&smsg_attr,
					&all_smsg_attrs[i]);
		if (status != GNI_RC_SUCCESS) {
			AFT_WARN("GNI_MemRegister returned %s\n",
				gni_err_str[status]);
			goto err3;
		}
		++i;;
	}

	/*
	 * need to barrier here to make sure all ranks have
	 * initialized their endpoints for messaging
	 */

	rc = PMI_Barrier();
	if (rc != PMI_SUCCESS) {
		AFT_WARN("GNI_MemRegister returned %s\n",
			gni_err_str[status]);
		goto err;
	}

	return 0;

err3:
	GNI_CqDestroy(aft_nic.rx_cq);
err2:
	GNI_CqDestroy(aft_nic.tx_cq);
err1:
	GNI_CdmDestroy(aft_nic.cdm);
err:
	if (all_smsg_attrs != NULL)
		free(all_smsg_attrs);
	if (aft_ep_hndls != NULL)
		free(aft_ep_hndls);

	PMI_Finalize();
	return -1;

}
