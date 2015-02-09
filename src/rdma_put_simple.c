/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * RDMA Put test example - this test only uses PMI
 *
 * Note: this test should not be run oversubscribed on nodes, i.e. more
 * instances on a given node than cpus, owing to the busy wait for
 * incoming data.
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

#define BIND_ID_MULTIPLIER       100
#define CACHELINE_MASK           0x3F   /* 64 byte cacheline */
#define CDM_ID_MULTIPLIER        1000
#define FLAG_DATA                0xffff000000000000
#define LOCAL_EVENT_ID_BASE      10000000
#define NUMBER_OF_TRANSFERS      10
#define POST_ID_MULTIPLIER       1000
#define REMOTE_EVENT_ID_BASE     11000000
#define SEND_DATA                0xdddd000000000000
#define TRANSFER_LENGTH          1024
#define TRANSFER_LENGTH_IN_BYTES ((TRANSFER_LENGTH)*sizeof(uint64_t))

typedef struct {
    gni_mem_handle_t mdh;
    uint64_t        addr;
} mdh_addr_t;

int             compare_data_failed = 0;
int             rank_id;
struct utsname  uts_info;
int             v_option = 0;

#include "utility_functions.h"

void print_help(void)
{
    fprintf(stdout,
"\n"
"RDMA_PUT_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the sending of a data\n"
"    transaction to a remote communication endpoint using a RDMA Put request.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_PostRdma() is used to with the 'PUT' type to send a data\n"
"        transaction to a remote location.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-D' specifies that the destination completion queue will not be\n"
"          created.\n"
"          The default value is that the destination completion queue will\n"
"          be created.\n"
"      2.  '-e' specifies that the GNI_EpSetEventId API will be used.\n"
"      3.  '-h' prints the help information for this example.\n"
"      4.  '-n' specifies the number of data transactions that will be sent.\n"
"          The default value is 10 data transactions to be sent.\n"
"      5.  '-O' specifies that the destination completion queue will\n"
"          be created with a very small number of entries.  This will\n"
"      6   cause an overrun condition on the destination complete queue.\n"
"          The default value is that the destination completion queue will\n"
"          be created with a sufficient number of entries to not cause\n"
"          the overrun condition to occur.  This implies that '-D' is ignored.\n"
"      7.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - rdma_put_pmi_example\n"
"      - rdma_put_pmi_example -e\n"
"      - rdma_put_pmi_example -D\n"
"      - rdma_put_pmi_example -D -e\n"
"      - rdma_put_pmi_example -O\n"
"\n"
    );
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    uint32_t        bind_id;
    gni_cdm_handle_t cdm_handle;
    uint32_t        cdm_id;
    int             cookie;
    gni_cq_handle_t cq_handle;
    int             create_destination_cq = 1;
    int             create_destination_overrun = 0;
    gni_cq_entry_t  current_event;
    uint64_t        data = SEND_DATA;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    gni_post_descriptor_t *event_post_desc_ptr;
    int             first_spawned;
    int             i;
    int             j;
    unsigned int    local_address;
    int             modes = GNI_CDM_MODE_BTE_SINGLE_CHANNEL;
    int             my_id;
    mdh_addr_t      my_memory_handle;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries;
    int             number_of_dest_cq_entries;
    int             number_of_ranks;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    uint8_t         ptag;
    int             rc;
    gni_post_descriptor_t *rdma_data_desc;
    uint64_t       *receive_buffer;
    uint64_t        receive_data = SEND_DATA;
    uint64_t        receive_flag = FLAG_DATA;
    int             receive_from;
    gni_mem_handle_t receive_memory_handle;
    unsigned int    remote_address;
    uint32_t        remote_event_id;
    mdh_addr_t     *remote_memory_handle_array;
    uint64_t       *send_buffer;
    int             send_to;
    gni_mem_handle_t source_memory_handle;
    gni_return_t    status = GNI_RC_SUCCESS;
    gni_return_t    save_status;
    char           *text_pointer;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;
    uint32_t        outstanding_puts = 0;
    int             recoverable;
    char buffer[1024];

    if ((i = uname(&uts_info)) != 0) {
        fprintf(stderr, "uname(2) failed, errno=%d\n", errno);
        exit(1);
    }

    /*
     * Get job attributes from PMI.
     */

    rc = PMI_Init(&first_spawned);
    assert(rc == PMI_SUCCESS);

    rc = PMI_Get_size(&number_of_ranks);
    assert(rc == PMI_SUCCESS);

    rc = PMI_Get_rank(&rank_id);
    assert(rc == PMI_SUCCESS);

    while ((opt = getopt(argc, argv, "hn:v")) != -1) {
        switch (opt) {
        case 'h':
            if (rank_id == 0) {
                print_help();
             }

            /*
             * Clean up the PMI information.
             */

            PMI_Finalize();

            exit(0);

        case 'n':

            /*
             * Set the number of messages that will be sent to the
             * shared message queue.
             */

            transfers = atoi(optarg);
            if (transfers < 1) {
                transfers = NUMBER_OF_TRANSFERS;
            }

            break;

        case 'v':
            v_option++;
            break;

        case '?':
            break;
        }
    }

    /*
     * Get job attributes from PMI.
     */

    ptag = get_ptag();
    cookie = get_cookie();

    /*
     * Allocate the rdma_data_desc array.
     */

    rdma_data_desc = (gni_post_descriptor_t *) calloc(transfers,
                                                sizeof(gni_post_descriptor_t));
    assert(rdma_data_desc != NULL);

    cdm_id = rank_id;

    /*
     * Create a handle to the communication domain.
     *    cdm_id is the rank of this instance of the job.
     *    ptag is the protection tab for the job.
     *    cookie is a unique identifier created by the system.
     *    modes is a bit mask used to enable various flags.
     *            GNI_CDM_MODE_BTE_SINGLE_CHANNEL states to do RDMA posts
     *            using only one BTE channel.
     *    cdm_handle is the handle that is returned pointing to the
     *        communication domain.
     */

    status = GNI_CdmCreate(cdm_id, ptag, cookie, modes, &cdm_handle);

    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate     ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        goto EXIT_TEST;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate     inst_id: %i ptag: %u cookie: 0x%x\n",
                uts_info.nodename, rank_id, cdm_id, ptag, cookie);
    }

    /*
     * Attach the communication domain handle to the NIC.
     *    cdm_handle is the handle pointing to the communication domain.
     *    device_id is the device identifier of the NIC that be attached to.
     *    local_address is the PE address that is returned for the
     *        communication domain that this NIC is attached to.
     *    nic_handle is the handle that is returned pointing to the NIC.
     */

    status =
        GNI_CdmAttach(cdm_handle, device_id, &local_address, &nic_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmAttach     ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmAttach     to NIC\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Determine the minimum number of completion queue entries, which
     * is the number of outstanding transactions at one time.  For this
     * test, it will be up to transfers transactions outstanding
     * at one time.
     */

    number_of_cq_entries = transfers;

    /*
     * Create the completion queue.
     *     nic_handle is the NIC handle that this completion queue will be
     *          associated with.
     *     number_of_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before an
     *          interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the callback
     *          function.
     *     cq_handle is the handle that is returned pointing to this newly
     *          created completion queue.
     */

    status =
        GNI_CqCreate(nic_handle, number_of_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      source with %i entries\n",
                uts_info.nodename, rank_id, number_of_cq_entries);
    }

    /*
     * Allocate the endpoint handles array.
     */

    endpoint_handles_array = (gni_ep_handle_t *) calloc(number_of_ranks,
                                                        sizeof
                                                        (gni_ep_handle_t));
    assert(endpoint_handles_array != NULL);

    /*
     * Get all of the NIC address for all of the ranks.
     */

    all_nic_addresses = (unsigned int *) gather_nic_addresses();

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        /*
         * You must do an EpCreate for each endpoint pair.
         * That is for each remote node that you will want to communicate with.
         * The EpBind request updates some fields in the endpoint_handle so
         * this is the reason that all pairs of endpoints need to be created.
         *
         * Create the logical endpoint for each rank.
         *     nic_handle is our NIC handle.
         *     cq_handle is our completion queue handle.
         *     endpoint_handles_array will contain the handle that is returned
         *         for this endpoint instance.
         */

        status =
            GNI_EpCreate(nic_handle, cq_handle,
                         &endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate      ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate      remote rank: %4i NIC: %p, CQ: %p, EP: %p\n",
                    uts_info.nodename, rank_id, i, nic_handle, cq_handle,
                    endpoint_handles_array[i]);
        }

        /*
         * Get the remote address to bind to.
         */

        remote_address = all_nic_addresses[i];
        bind_id = (rank_id * BIND_ID_MULTIPLIER) + i;

        /*
         * Bind the remote address to the endpoint handler.
         *     endpoint_handles_array is the endpoint handle that is being bound
         *     remote_address is the address that is being bound to this
         *         endpoint handler
         *     bind_id is an unique user specified identifier for this bind.
         *         In this test bind_id refers to the instance id of the remote
         *         communication domain that we are binding to.
         */

        status = GNI_EpBind(endpoint_handles_array[i], remote_address, bind_id);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        remote rank: %4i EP:  %p remote_address: %u, remote_id: %u\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i], remote_address, bind_id);
        }
    }

    /*
     * Allocate the buffer that will contain the data to be sent.  This
     * allocation is creating a buffer large enough to hold all of the
     * sending data for all of the transfers.
     */

    rc = posix_memalign((void **) &send_buffer, 64,
                        (TRANSFER_LENGTH_IN_BYTES * transfers));
    assert(rc == 0);

    /*
     * Initialize the buffer to all zeros.
     */

    memset(send_buffer, 0, (TRANSFER_LENGTH_IN_BYTES * transfers));

    /*
     * Register the memory associated for the send buffer with the NIC.
     * We are sending the data from this buffer not receiving into it.
     *     nic_handle is our NIC handle.
     *     send_buffer is the memory location of the send buffer.
     *     TRANSFER_LENGTH_IN_BYTES is the size of the memory allocated to the
     *         send buffer.
     *     NULL means that no completion queue handle is specified.
     *     GNI_MEM_READWRITE is the read/write attribute for the flag's
     *         memory region.
     *     -1 specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     source_memory_handle is the handle for this memory region.
     */

    status = GNI_MemRegister(nic_handle, (uint64_t) send_buffer,
                             (TRANSFER_LENGTH_IN_BYTES *
                              transfers), NULL,
                             GNI_MEM_READWRITE, -1,
                             &source_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister  send_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        goto EXIT_ENDPOINT;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   send_buffer  size: %u address: %p\n",
                uts_info.nodename, rank_id,
                (unsigned int) (TRANSFER_LENGTH_IN_BYTES *
                                transfers), send_buffer);
    }

    /*
     * Allocate the buffer that will receive the data.  This allocation is
     * creating a buffer large enough to hold all of the received data for
     * all of the transfers.
     */

    rc = posix_memalign((void **) &receive_buffer, 64,
                        (TRANSFER_LENGTH_IN_BYTES * transfers));
    assert(rc == 0);

    /*
     * Initialize the buffer to all zeros.
     */

    memset(receive_buffer, 0, (TRANSFER_LENGTH_IN_BYTES * transfers));

    /*
     * Register the memory associated for the receive buffer with the NIC.
     * We are receiving the data into this buffer.
     *     nic_handle is our NIC handle.
     *     receive_buffer is the memory location of the receive buffer.
     *     (TRANSFER_LENGTH_IN_BYTES * transfers) is the size of the
     *         memory allocated to the receive buffer.
     *     destination_cq_handle is the destination completion queue handle.
     *     GNI_MEM_READWRITE is the read/write attribute for the receive buffer's
     *         memory region.
     *     -1 specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     receive_memory_handle is the handle for this memory region.
     */

    status = GNI_MemRegister(nic_handle, (uint64_t) receive_buffer,
                             TRANSFER_LENGTH_IN_BYTES *
                             transfers, NULL,
                             GNI_MEM_READWRITE,
                             -1, &receive_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   receive_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        goto EXIT_MEMORY_SOURCE;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   receive_buffer  size: %u address: %p\n",
                uts_info.nodename, rank_id,
                (unsigned int) (((TRANSFER_LENGTH * transfers)
                                 + CACHELINE_MASK + 1) * sizeof(uint64_t)),
                receive_buffer);
    }

    /*
     * Allocate a buffer to contain all of the remote memory handle's.
     */

    remote_memory_handle_array =
        (mdh_addr_t *) calloc(number_of_ranks, sizeof(mdh_addr_t));
    assert(remote_memory_handle_array);

    my_memory_handle.addr = (uint64_t) receive_buffer;
    my_memory_handle.mdh = receive_memory_handle;

    /*
     * Gather up all of the remote memory handle's.
     * This also acts as a barrier to get all of the ranks to sync up.
     */

    allgather(&my_memory_handle, remote_memory_handle_array,
              sizeof(mdh_addr_t));

    if ((v_option > 1) && (rank_id == 0)) {
        fprintf(stdout,
                "[%s] rank address     mdh.qword1            mdn.qword2\n",
                uts_info.nodename);
        for (i = 0; i < number_of_ranks; i++) {
            fprintf(stdout, "[%s] %4i 0x%lx    0x%016lx    0x%016lx\n",
                    uts_info.nodename, i,
                    remote_memory_handle_array[i].addr,
                    remote_memory_handle_array[i].mdh.qword1,
                    remote_memory_handle_array[i].mdh.qword2);
        }
    }

    if (v_option > 1) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    /*
     * Determine who we are going to send our data to and
     * who we are going to receive data from.
     */

    send_to = (rank_id + 1) % number_of_ranks;
    my_id = (rank_id & 0xffffff) << 24;

    for (i = 0; i < transfers; i++) {

        /*
         * Initialize the data to be sent.
         * The source data will look like: 0xddddlllllltttttt
         *     where: dddd is the actual value
         *            llllll is the rank for this process
         *            tttttt is the transfer number
         */

        data = SEND_DATA + my_id + i + 1;

        for (j = 0; j < TRANSFER_LENGTH; j++) {
            send_buffer[j + (i * TRANSFER_LENGTH)] = data;
        }

        /*
         * Setup the data request.
         *    type is RDMA_PUT.
         *    cq_mode states what type of events should be sent.
         *         GNI_CQMODE_GLOBAL_EVENT allows for the sending of an event
         *             to the local node after the receipt of the data.
         *         GNI_CQMODE_REMOTE_EVENT allows for the sending of an event
         *             to the remote node after the receipt of the data.
         *    dlvr_mode states the delivery mode.
         *    local_addr is the address of the sending buffer.
         *    local_mem_hndl is the memory handle of the sending buffer.
         *    remote_addr is the the address of the receiving buffer.
         *    remote_mem_hndl is the memory handle of the receiving buffer.
         *    length is the amount of data to transfer.
         *    rdma_mode states how the request will be handled.
         *    src_cq_hndl is the source complete queue handle.
         */

        rdma_data_desc[i].type = GNI_POST_RDMA_PUT;
        rdma_data_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT;
#if 1
        rdma_data_desc[i].dlvr_mode = GNI_DLVMODE_PERFORMANCE;
#else
        rdma_data_desc[i].dlvr_mode = GNI_DLVMODE_IN_ORDER;
#endif
        rdma_data_desc[i].local_addr = (uint64_t) send_buffer;
        rdma_data_desc[i].local_addr += i * TRANSFER_LENGTH_IN_BYTES;
        rdma_data_desc[i].local_mem_hndl = source_memory_handle;
        rdma_data_desc[i].remote_addr = remote_memory_handle_array[send_to].addr;
        rdma_data_desc[i].remote_addr += i * TRANSFER_LENGTH_IN_BYTES;
        rdma_data_desc[i].remote_mem_hndl = remote_memory_handle_array[send_to].mdh;
        rdma_data_desc[i].length = TRANSFER_LENGTH_IN_BYTES;
        rdma_data_desc[i].rdma_mode = 0;
        rdma_data_desc[i].src_cq_hndl = cq_handle;
        rdma_data_desc[i].post_id = (uint64_t)&rdma_data_desc[i];

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostRdma      data transfer: %4i send to:   %4i local addr:  0x%lx remote addr: 0x%lx data: 0x%16lx data length: %4i post_id: %lu\n",
                    uts_info.nodename, rank_id, (i + 1), send_to,
                    rdma_data_desc[i].local_addr,
                    rdma_data_desc[i].remote_addr, data,
                    (int) (TRANSFER_LENGTH_IN_BYTES - sizeof(uint64_t)),
                    rdma_data_desc[i].post_id);
        }

        /*
         * Send the data.
         */

        status =
            GNI_PostRdma(endpoint_handles_array[send_to],
                         &rdma_data_desc[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostRdma      data ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            continue;
        }

        outstanding_puts++;

        if (v_option > 2) {
            fprintf(stdout, "[%s] Rank: %4i GNI_PostRdma      data successful\n",
                    uts_info.nodename, rank_id);
        }

    }   /* end of for loop for transfers */

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    /*
     * Get all of the data completion queue events.
     */

    if (v_option) {
        fprintf(stdout,
                "[%s] Rank: %4i data transfers complete, checking CQ events\n",
                uts_info.nodename, rank_id);
    }

    while (outstanding_puts > 0) {

        /*
         * Check the completion queue to verify that the message request has
         * been sent.  The source completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostRdma to fail.
         */

        status = GNI_RC_NOT_DONE;
        status = GNI_CqGetEvent(cq_handle, &current_event);
        if (status == GNI_RC_NOT_DONE) continue;

        if (v_option) {
            fprintf(stdout, "[%s] Rank: %4i GNI_CqGetEvent returned %s outstanding_puts %d\n",
                    uts_info.nodename, rank_id, gni_err_str[status], outstanding_puts);
        }

        save_status = status; 
        
        assert(GNI_CQ_GET_TYPE(current_event) == GNI_CQ_EVENT_TYPE_POST);

        switch (save_status) {

        case GNI_RC_SUCCESS:

            /* Happy case */

            assert(GNI_CQ_STATUS_OK(current_event));  /* sanity check */
            status = GNI_GetCompleted(cq_handle,current_event, &event_post_desc_ptr);
            assert ((status == GNI_RC_SUCCESS) || (status == GNI_RC_TRANSACTION_ERROR)); 
            outstanding_puts--;
            break;

        case GNI_RC_TRANSACTION_ERROR:

            /* Uh oh - lets see if we can retry */

            status = GNI_CqErrorRecoverable(current_event,(unsigned int *)&recoverable);
            assert (status == GNI_RC_SUCCESS); 
            GNI_CqErrorStr(current_event,buffer,sizeof(buffer));

            if (recoverable != 1) {
                fprintf(stderr,"Non recoverable network error - (%s) \n",buffer);
            } else  {
                fprintf(stderr,"Recoverable network error - (%s) \n",buffer);
            }

            status = GNI_GetCompleted(cq_handle, current_event, &event_post_desc_ptr);
            assert ((status == GNI_RC_SUCCESS) || (status == GNI_RC_TRANSACTION_ERROR)); 
            outstanding_puts--;
            break;

        case GNI_RC_ERROR_RESOURCE:
            assert(0);    /* we've somehow overrun the CQ */
            break;

        default:
            assert(0);    /* we've somehow overrun the CQ */
            break;
        }
    }

  EXIT_WAIT_BARRIER:

    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Free allocated memory.
     */

    free(remote_memory_handle_array);

    /*
     * Deregister the memory associated for the receive buffer with the NIC.
     *     nic_handle is our NIC handle.
     *     receive_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &receive_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister receive_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
    } else {
        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister receive_buffer    NIC: %p\n",
                    uts_info.nodename, rank_id, nic_handle);
        }

        /*
         * Free allocated memory.
         */

        free(receive_buffer);
    }

  EXIT_MEMORY_SOURCE:

    /*
     * Deregister the memory associated for the send buffer with the NIC.
     *     nic_handle is our NIC handle.
     *     source_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &source_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister send_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
    } else {
        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister send_buffer       NIC: %p\n",
                    uts_info.nodename, rank_id, nic_handle);
        }

        /*
         * Free allocated memory.
         */

        free(send_buffer);
    }


  EXIT_ENDPOINT:

    /*
     * Remove the endpoints to all of the ranks.
     *
     * Note: if there are outstanding events in the completion queue,
     *       the endpoint can not be unbound.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        if (endpoint_handles_array[i] == 0) {

            /*
             * This endpoint does not exist.
             */

            continue;
        }

        /*
         * Unbind the remote address from the endpoint handler.
         *     endpoint_handles_array is the endpoint handle that is being unbound
         */

        status = GNI_EpUnbind(endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpUnbind      ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpUnbind      remote rank: %4i EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }

        /*
         * You must do an EpDestroy for each endpoint pair.
         *
         * Destroy the logical endpoint for each rank.
         *     endpoint_handles_array is the endpoint handle that is being
         *         destroyed.
         */

        status = GNI_EpDestroy(endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy     ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy     remote rank: %4i EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }
    }

    /*
     * Free allocated memory.
     */

    free (endpoint_handles_array);

  EXIT_CQ:

    /*
     * Destroy the completion queue.
     *     cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy     source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy     source\n",
                uts_info.nodename, rank_id);
    }

  EXIT_DOMAIN:

    /*
     * Clean up the communication domain handle.
     */

    status = GNI_CdmDestroy(cdm_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmDestroy    ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmDestroy\n",
                uts_info.nodename, rank_id);
    }

  EXIT_TEST:

    /*
     * Free allocated memory.
     */

    free(rdma_data_desc);

    /*
     * Display the results from this test.
     */

#if 0
    rc = print_results();
#endif

    /*
     * Clean up the PMI information.
     */

    PMI_Finalize();

    return rc;
}
