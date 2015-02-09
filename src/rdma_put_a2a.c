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
    int             data_transfers_sent = 0;
    gni_cq_handle_t destination_cq_handle = NULL;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    uint32_t        event_inst_id;
    gni_post_descriptor_t *event_post_desc_ptr;
    uint32_t        expected_local_event_id;
    uint32_t        expected_remote_event_id;
    int             first_spawned;
    uint64_t        *flag;
    volatile uint64_t *flag_ptr;
    int             flag_transfers_sent = 0;
    int             i;
    int             j;
    unsigned int    local_address;
    uint32_t        local_event_id;
    int             modes = GNI_CDM_MODE_BTE_SINGLE_CHANNEL;
    gni_mem_handle_t my_flag_memory_handle;
    int             my_id;
    mdh_addr_t      my_memory_handle;
    int             my_receive_from;
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
    gni_post_descriptor_t *rdma_flag_desc;
    uint64_t       *receive_buffer;
    uint64_t        receive_data = SEND_DATA;
    uint64_t        receive_flag = FLAG_DATA;
    int             receive_from;
    gni_mem_handle_t receive_memory_handle;
    unsigned int    remote_address;
    uint32_t        remote_event_id;
    mdh_addr_t     *remote_memory_handle_array;
    uint64_t       *send_buffer;
    uint64_t        send_post_id;
    int             send_to;
    gni_mem_handle_t source_memory_handle;
    gni_return_t    status = GNI_RC_SUCCESS;
    char           *text_pointer;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;
    int             use_event_id = 0;

    command_name = ((text_pointer = rindex(argv[0], '/')) != NULL) ?
        strdup(++text_pointer) : strdup(argv[0]);

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

    local_event_id = rank_id;

    while ((opt = getopt(argc, argv, "hn:l:")) != -1) {
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
     * Determine the number of passes required for this test to be successful.
     */

    if (create_destination_cq != 0) {
        expected_passed = transfers * 8;
    } else {
        expected_passed = transfers * 6;
    }

    /*
     * Allocate the flag array.
     */

    flag = (uint64_t *) calloc(transfers, sizeof(uint64_t));
    assert(flag != NULL);

    /*
     * Allocate the rdma_data_desc array.
     */

    rdma_data_desc = (gni_post_descriptor_t *) calloc(transfers,
                                                sizeof(gni_post_descriptor_t));
    assert(rdma_data_desc != NULL);

    /*
     * Allocate the rdma_flag_desc array.
     */

    rdma_flag_desc = (gni_post_descriptor_t *) calloc(transfers,
                                                sizeof(gni_post_descriptor_t));
    assert(rdma_flag_desc != NULL);

    cdm_id = rank_id * CDM_ID_MULTIPLIER;

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
        INCREMENT_ABORTED;
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
        INCREMENT_ABORTED;
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
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      source with %i entries\n",
                uts_info.nodename, rank_id, number_of_cq_entries);
    }

    if (create_destination_cq != 0) {
        if (create_destination_overrun == 0) {
            /*
             * Determine the minimum number of completion queue entries, which
             * is the number of transactions outstanding at one time.  For this
             * test, since there are no barriers between transfers, the number
             * of outstanding transfers could be up to transfers.
             * Also, we need to multiple the number of outstanding transfers by
             * 2 because the data and flag transfers are done on separate
             * requests and each request will create an event.
             */

            number_of_dest_cq_entries = transfers * 2;
        } else {

            /*
             * Set the number of completion queue entries to 1 to create a
             * completion queue overrun condition.  There should be more than
             * 1 transfer in progress at any given time.
             */

            number_of_dest_cq_entries = 1;
        }

        /*
         * Create the destination_completion queue.
         *     nic_handle is the NIC handle that this completion queue will be
         *          associated with.
         *     number_of_dest_cq_entries is the size of the completion queue.
         *     zero is the delay count is the number of allowed events before
         *          an interrupt is generated.
         *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
         *     NULL states that no user supplied callback function is defined.
         *     NULL states that no user supplied pointer is passed to the
         *          callback function.
         *     destination_cq_handle is the handle that is returned pointing to
         *          this newly created completion queue.
         */

        status =
            GNI_CqCreate(nic_handle, number_of_dest_cq_entries, 0,
                         GNI_CQ_NOBLOCK, NULL, NULL,
                         &destination_cq_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqCreate      destination ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CQ;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqCreate      destination with %i entries\n",
                    uts_info.nodename, rank_id, number_of_dest_cq_entries);
        }
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
            INCREMENT_ABORTED;
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
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        remote rank: %4i EP:  %p remote_address: %u, remote_id: %u\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i], remote_address, bind_id);
        }

        if (use_event_id == 1) {
            local_event_id = LOCAL_EVENT_ID_BASE + cdm_id + bind_id;
            remote_event_id = REMOTE_EVENT_ID_BASE + cdm_id + bind_id;

            status = GNI_EpSetEventData(endpoint_handles_array[i], local_event_id, remote_event_id);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpSetEventData ERROR remote rank: %4i status: %d\n",
                        uts_info.nodename, rank_id, i, status);
                INCREMENT_ABORTED;
                goto EXIT_ENDPOINT;
            }

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpSetEventData remote rank: %4i EP:  %p local_event_id: %u, remote_event_id: %u\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i], local_event_id, remote_event_id);
            }
        }
    }

    /*
     * Register the memory associated for the flag with the NIC.
     *     nic_handle is our NIC handle.
     *     flag is the memory location of the flag.
     *     (transfers * sizeof(uint64_t)) is the size of the memory
     *         allocated to the flag.
     *     NULL means that no completion queue handle is specified.
     *     GNI_MEM_READWRITE is the read/write attribute for the flag's
     *         memory region.
     *     -1 specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     my_flag_memory_handle is the handle for this memory region.
     */

    status = GNI_MemRegister(nic_handle, (uint64_t) flag,
                             (transfers * sizeof(uint64_t)),
                             NULL, GNI_MEM_READWRITE, -1,
                             &my_flag_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   flag ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_ENDPOINT;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   flag  size: %lu address: %p\n",
                uts_info.nodename, rank_id,
                (transfers * sizeof(uint64_t)), flag);
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
        INCREMENT_ABORTED;
        goto EXIT_MEMORY_FLAG;
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
                             transfers, destination_cq_handle,
                             GNI_MEM_READWRITE,
                             -1, &receive_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   receive_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
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
    receive_from = (number_of_ranks + rank_id - 1) % number_of_ranks;
    my_receive_from = (receive_from & 0xffffff) << 24;
    my_id = (rank_id & 0xffffff) << 24;

    if (use_event_id == 1) {
        expected_local_event_id = LOCAL_EVENT_ID_BASE + cdm_id
                                  + (BIND_ID_MULTIPLIER * rank_id)
                                  + send_to;
        expected_remote_event_id = REMOTE_EVENT_ID_BASE
                                  + (CDM_ID_MULTIPLIER * receive_from)
                                  + (BIND_ID_MULTIPLIER * receive_from)
                                  + rank_id;
    } else {
        expected_local_event_id = (rank_id * BIND_ID_MULTIPLIER) + send_to;
        expected_remote_event_id = CDM_ID_MULTIPLIER * receive_from;
    }

    for (i = 0; i < transfers; i++) {
        send_post_id = ((uint64_t) expected_local_event_id * POST_ID_MULTIPLIER) + i + 1;

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
        if (create_destination_cq != 0) {
            rdma_data_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT |
                GNI_CQMODE_REMOTE_EVENT;
        } else {
            rdma_data_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT;
        }
        rdma_data_desc[i].dlvr_mode = GNI_DLVMODE_PERFORMANCE;
        rdma_data_desc[i].local_addr = (uint64_t) send_buffer;
        rdma_data_desc[i].local_addr += i * TRANSFER_LENGTH_IN_BYTES;
        rdma_data_desc[i].local_mem_hndl = source_memory_handle;
        rdma_data_desc[i].remote_addr =
            remote_memory_handle_array[send_to].addr + sizeof(uint64_t);
        rdma_data_desc[i].remote_addr += i * TRANSFER_LENGTH_IN_BYTES;
        rdma_data_desc[i].remote_mem_hndl =
            remote_memory_handle_array[send_to].mdh;
        rdma_data_desc[i].length =
            TRANSFER_LENGTH_IN_BYTES - sizeof(uint64_t);
        rdma_data_desc[i].rdma_mode = GNI_RDMAMODE_FENCE;
        rdma_data_desc[i].src_cq_hndl = cq_handle;
        rdma_data_desc[i].post_id = send_post_id;

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
            INCREMENT_FAILED;
            continue;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout, "[%s] Rank: %4i GNI_PostRdma      data successful\n",
                    uts_info.nodename, rank_id);
        }

        data_transfers_sent++;
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

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i data transfers complete, checking CQ events\n",
                uts_info.nodename, rank_id);
    }

    for (i = 0; i < data_transfers_sent; i++) {
        send_post_id = ((uint64_t) expected_local_event_id * POST_ID_MULTIPLIER) + i + 1;

        /*
         * Check the completion queue to verify that the message request has
         * been sent.  The source completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostRdma to fail.
         */

        rc = get_cq_event(cq_handle, uts_info, rank_id, 1, 1, &current_event);
        if (rc == 0) {

            /*
             * An event was received.
             *
             * Complete the event, which removes the current event's post
             * descriptor from the event queue.
             */

            status = GNI_GetCompleted(cq_handle, current_event, &event_post_desc_ptr);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_GetCompleted  data ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);

                INCREMENT_FAILED;
            } else {

                /*
                 * Validate the completed request's post id with the expected id.
                 */

                if (send_post_id != event_post_desc_ptr->post_id) {

                    /*
                     * The event's inst_id was not the expected inst_id
                     * value.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i Completed data ERROR received post_id: %lu, expected post_id: %lu\n",
                            uts_info.nodename, rank_id, event_post_desc_ptr->post_id,
                            send_post_id);

                    INCREMENT_FAILED;
                } else {

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_GetCompleted  data transfer: %4i send to:   %4i remote addr: 0x%lx post_id: %lu\n",
                                uts_info.nodename, rank_id, (i + 1), send_to,
                                event_post_desc_ptr->remote_addr,
                                event_post_desc_ptr->post_id);
                    }

                    INCREMENT_PASSED;
                }

                /*
                 * Validate the current event's instance id with the expected id.
                 */

                event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                if (event_inst_id != expected_local_event_id) {

                    /*
                     * The event's inst_id was not the expected inst_id
                     * value.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i CQ Event data ERROR received inst_id: %u, expected inst_id: %u in event_data\n",
                            uts_info.nodename, rank_id, event_inst_id, expected_local_event_id);

                    INCREMENT_FAILED;
                } else {

                    INCREMENT_PASSED;
                }
            }
        } else if (rc == 2) {

            /*
             * An overrun error occurred while receiving the event.
             */

            if (create_destination_overrun == 1) {
                expected_passed = 1;
                passed = 1;
                failed = 0;
            } else {
                INCREMENT_FAILED;

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i get_cq_event        data ERROR status: OVERRUN\n",
                            uts_info.nodename, rank_id);
                }
            }
                        
            continue;
        } else {

            /*
             * An error occurred while receiving the event.
             */

            INCREMENT_FAILED;
            continue;
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    for (i = 0; i < transfers; i++) {

        /*
         * Initialize the flag to be sent.
         * The source flag will look like: 0xfffflllllltttttt
         *     where: ffff is the actual value
         *            llllll is the rank for this process
         *            tttttt is the transfer number
         */

        flag[i] = FLAG_DATA + my_id + i + 1;

        /*
         * Setup the flag request.
         *    type is RDMA_PUT.
         *    cq_mode states what type of events should be sent.
         *         GNI_CQMODE_GLOBAL_EVENT allows for the sending of an event
         *             to the local node after the receipt of the data.
         *         GNI_CQMODE_REMOTE_EVENT allows for the sending of an event
         *             to the remote node after the receipt of the data.
         *    dlvr_mode states the delivery mode.
         *    local_addr is the address of the sending flag.
         *    local_mem_hndl is the memory handle of the sending flag.
         *    remote_addr is the the address of the receiving flag.
         *    remote_mem_hndl is the memory handle of the receiving flag.
         *    length is the amount of data to transfer.
         *    rdma_mode states how the request will be handled.
         *    src_cq_hndl is the source complete queue handle.
         */

        rdma_flag_desc[i].type = GNI_POST_RDMA_PUT;
        if (create_destination_cq != 0) {
            rdma_flag_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT |
                GNI_CQMODE_REMOTE_EVENT;
        } else {
            rdma_flag_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT;
        }
        rdma_flag_desc[i].dlvr_mode = GNI_DLVMODE_PERFORMANCE;
        rdma_flag_desc[i].local_addr = (uint64_t) & flag[i];
        rdma_flag_desc[i].local_mem_hndl = my_flag_memory_handle;
        rdma_flag_desc[i].remote_addr =
            remote_memory_handle_array[send_to].addr;
        rdma_flag_desc[i].remote_addr += i * TRANSFER_LENGTH_IN_BYTES;
        rdma_flag_desc[i].remote_mem_hndl =
            remote_memory_handle_array[send_to].mdh;
        rdma_flag_desc[i].length = sizeof(uint64_t);
        rdma_flag_desc[i].rdma_mode = 0;
        rdma_flag_desc[i].src_cq_hndl = cq_handle;

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostRdma      flag transfer: %4i send to:   %4i local addr:  0x%lx remote addr: 0x%lx flag: 0x%16lx data length: %4i\n",
                    uts_info.nodename, rank_id, (i + 1), send_to,
                    rdma_flag_desc[i].local_addr,
                    rdma_flag_desc[i].remote_addr, flag[i],
                    (int) (sizeof(uint64_t)));
        }

        /*
         * Send the flag.
         */

        status =
            GNI_PostRdma(endpoint_handles_array[send_to],
                         &rdma_flag_desc[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostRdma      flag ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_FAILED;
            continue;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout, "[%s] Rank: %4i GNI_PostRdma      flag successful\n",
                    uts_info.nodename, rank_id);
        }

        flag_transfers_sent++;
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    /*
     * Get all of the flag completion queue events.
     */

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i flag transfers complete, checking CQ events\n",
                uts_info.nodename, rank_id);
    }

    for (i = 0; i < flag_transfers_sent; i++) {

        /*
         * Check the completion queue to verify that the message request has
         * been sent.  The source completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostRdma to fail.
         */

        rc = get_cq_event(cq_handle, uts_info, rank_id, 1, 1, &current_event);
        if (rc == 0) {

            /*
             * An event was received.
             *
             * Complete the event, which removes the current event's post
             * descriptor from the event queue.
             */

            status = GNI_GetCompleted(cq_handle, current_event, &event_post_desc_ptr);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_GetCompleted  flag ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);

                INCREMENT_FAILED;
            } else {

                /*
                 * Validate the current event's instance id with the expected id.
                 */

                event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                if (event_inst_id != expected_local_event_id) {

                    /*
                     * The event's inst_id was not the expected inst_id
                     * value.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i CQ Event flag ERROR received inst_id: %u, expected inst_id: %u in event_data\n",
                            uts_info.nodename, rank_id, event_inst_id, expected_local_event_id);

                    INCREMENT_FAILED;
                } else {

                    INCREMENT_PASSED;
                }
            }
        } else if (rc == 2) {

            /*
             * An overrun error occurred while receiving the event.
             */

            if (create_destination_overrun == 1) {
                expected_passed = 1;
                passed = 1;
                failed = 0;
            } else {
                INCREMENT_FAILED;

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i get_cq_event        flag ERROR status: OVERRUN\n",
                            uts_info.nodename, rank_id);
                }
            }
                        
            continue;
        } else {
            /*
             * An error occurred while receiving the event.
             */

            INCREMENT_FAILED;
            continue;
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    if (create_destination_cq != 0) {

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i Wait for destination completion queue events recv from: %4i\n",
                    uts_info.nodename, rank_id, receive_from);
        }

        /*
         * Check the completion queue to verify that the data and flag has
         * been received.  The destination completion queue needs to be
         * checked and events to be removed so that it does not become full
         * and cause succeeding events to be lost.
         */

        for (i = 0; i < transfers * 2; i++) {
            rc = get_cq_event(destination_cq_handle, uts_info,
                                         rank_id, 0, 1, &current_event);
            if (rc == 0) {

                /*
                 * An event was received.
                 *
                 * Validate the current event's instance id with the expected id.
                 */

                event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                if (event_inst_id != expected_remote_event_id) {

                    /*
                     * The event's inst_id was not the expected inst_id
                     * value.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i CQ Event destination ERROR received inst_id: %u, expected inst_id: %u in event_data\n",
                            uts_info.nodename, rank_id, event_inst_id, expected_remote_event_id);

                    INCREMENT_FAILED;
                } else {

                    INCREMENT_PASSED;
                }
            } else if (rc == 2) {

                /*
                 * An overrun error occurred while receiving the event.
                 */

                if (create_destination_overrun == 1) {
                    expected_passed = 1;
                    passed = 1;
                    failed = 0;
                } else {
                    INCREMENT_FAILED;

                    if (v_option > 2) {
                        fprintf(stdout,
                                "[%s] Rank: %4i get_cq_event         destination CQ ERROR status: OVERRUN\n",
                                uts_info.nodename, rank_id);
                    }
                }

                goto EXIT_WAIT_BARRIER;
            } else {

                /*
                 * An error occurred while receiving the event.
                 */

                fprintf(stdout,
                        "[%s] Rank: %4i CQ Event ERROR destination queue did not receieve"
                        " flag or data event\n",
                        uts_info.nodename, rank_id);

                INCREMENT_FAILED;
                goto EXIT_WAIT_BARRIER;
            }
        }

        if (v_option) {

            /*
             * Write out all of the output messages.
             */

            fflush(stdout);
        }
    }

    for (i = 0; i < transfers; i++) {

        /*
         * Detemine what the received flag will look like.
         * The received flag will look like: 0xffffrrrrrrtttttt
         *     where: ffff is the actual value
         *            rrrrrr is the rank of the remote process,
         *                   that is sending to this process
         *            tttttt is the transfer number
         */

        receive_flag = FLAG_DATA + my_receive_from + i + 1;

        /*
         * Wait for arrival of the flag from the remote node
         */

        flag_ptr = (uint64_t *) & receive_buffer[TRANSFER_LENGTH * i];

        while (*flag_ptr != receive_flag) {
            sched_yield();
        };

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i Received          flag transfer: %4i recv from: %4i remote addr: %p flag: 0x%16lx\n",
                    uts_info.nodename, rank_id, (i + 1),
                    (int) ((*flag_ptr >> 24) & 0xffffff), flag_ptr,
                    *flag_ptr);
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    for (i = 0; i < transfers; i++) {

        /*
         * Detemine what the received data will look like.
         * The received data will look like: 0xddddrrrrrrtttttt
         *     where: dddd is the actual value
         *            rrrrrr is the rank of the remote process,
         *                   that is sending to this process
         *            tttttt is the transfer number
         */

        receive_data = SEND_DATA + my_receive_from + i + 1;

        /*
         * Verify the received data.
         * The first element in the buffer is the flag.
         */

        compare_data_failed = 0;

        for (j = 1; j < TRANSFER_LENGTH; j++) {
            if (receive_buffer[j + (TRANSFER_LENGTH * i)] != receive_data) {

                /*
                 * The data was not what was expected.
                 */

                compare_data_failed++;
                fprintf(stdout,
                        "[%s] Rank: %4i Received data ERROR in transfer: %4i element: %4i (address %p)"
                        " received data: 0x%016lx expected data: 0x%016lx\n",
                        uts_info.nodename, rank_id, (i + 1),
                        j + (TRANSFER_LENGTH * i),
                        &(receive_buffer[j + (TRANSFER_LENGTH * i)]),
                        receive_buffer[j + (TRANSFER_LENGTH * i)],
                        receive_data);
            } else if (j == 1) {
                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Received          data transfer: %4i recv from: %4i remote addr: %p data: 0x%016lx\n",
                            uts_info.nodename, rank_id, (i + 1),
                            (int) ((receive_buffer
                                    [j + (TRANSFER_LENGTH * i)] >> 24)
                                   & 0xffffff),
                            &receive_buffer[j + (TRANSFER_LENGTH * i)],
                            receive_buffer[j + (TRANSFER_LENGTH * i)]);
                }
            }

            /*
             * Only print the first 10 data compare errors.
             */

            if (compare_data_failed > 9) {
                break;
            }
        }

        if (compare_data_failed != 0) {

            /*
             * The data did not compare correctly.
             * Increment the failed test count.
             */

            INCREMENT_FAILED;
        } else {

            /*
             * The data compared correctly.
             * Increment the passed test count.
             */

            INCREMENT_PASSED;
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
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
        INCREMENT_ABORTED;
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
        INCREMENT_ABORTED;
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

  EXIT_MEMORY_FLAG:

    /*
     * Deregister the memory associated for the flag with the NIC.
     *     nic_handle is our NIC handle.
     *     my_flag_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &my_flag_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister flag ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister flag              NIC: %p\n",
                uts_info.nodename, rank_id, nic_handle);
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

    if (create_destination_cq != 0) {
        /*
         * Destroy the destination completion queue.
         *     cq_handle is the handle that is being destroyed.
         */

        status = GNI_CqDestroy(destination_cq_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqDestroy     destination ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
        } else if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqDestroy     destination\n",
                    uts_info.nodename, rank_id);
        }
    }

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

    free(rdma_flag_desc);

    /*
     * Free allocated memory.
     */

    free(rdma_data_desc);

    /*
     * Free allocated memory.
     */

    free(flag);

    /*
     * Display the results from this test.
     */

    rc = print_results();

    /*
     * Clean up the PMI information.
     */

    PMI_Finalize();

    return rc;
}
