/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * Test basic FMA short message functionality - this test only uses PMI
 * GNI_SmsgInit, GNI_SmsgSend,GNI_SmsgGetNext,GNI_SmsgRelease
 */

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#include <errno.h>
#include "gni_pub.h"
#include "pmi.h"

#define CACHELINE_SIZE      64
#define MB_MAXCREDIT        128
#define MSG_MAXSIZE         1024
#define NUMBER_OF_TRANSFERS 500

typedef struct {
    int             rank;
    int             transmission;
    char            buffer[16];
} msg_header_t;

int             rank_id;
struct utsname  uts_info;
int             v_option = 0;

#include "utility_functions.h"

void print_help(void)
{
    fprintf(stdout,
"\n"
"SMSG_SEND_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the sending and receiving\n"
"    of short messages between two communication endpoints.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_SmsgBufferSizeNeeded() is used to determine the amount\n"
"        of memory needed to be allocated for the short messaging resources.\n"
"      - GNI_EpPostData() is used to post a datagram to a remote bounded\n"
"        communication endpoint.\n"
"      - GNI_EpPostDataTest() is used to get the state of a previously\n"
"        posted datagram.\n"
"      - GNI_SmsgInit() is used to configure the short messaging protocol\n"
"        on the communication endpoint.\n"
"      - GNI_SmsgSend() is used to send a message to the remote\n"
"        communication endpoint.\n"
"      - GNI_SmsgGetNext() is used to receive a message from the remote\n"
"        communication endpoint.\n"
"      - GNI_SmsgRelease() is used to release the current message buffer\n"
"        from the last SmsgGetNext API call.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-h' prints the help information for this example.\n"
"      2.  '-n' specifies the number of messages that will be sent.\n"
"          The default value is 500 data transactions to be sent.\n"
"      3.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - smsg_send_pmi_example\n"
"\n"
    );
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    int             bytes_needed;
    unsigned int    bytes_per_mbox;
    gni_cdm_handle_t cdm_handle;
    int             cookie;
    gni_cq_entry_t  current_event;
    gni_cq_handle_t destination_cq_handle;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    uint32_t        event_inst_id;
    int             file_descriptor;
    int             first_spawned;
    register int    i;
    unsigned int    local_address;
    gni_mem_handle_t local_memory_handle;
    gni_smsg_attr_t local_smsg_attributes;
    int             message_count = 0;
    void           *mmap_ptr;
    int             modes = 0;
    register int    n;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries = 0;
    int             number_of_dest_cq_entries = 0;
    int             number_of_ranks;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    gni_post_state_t post_state;
    uint8_t         ptag;
    int             rc;
    char            receive_buffer[16];
    int             recv_from;
    msg_header_t   *receive_header;
    unsigned int    remote_address;
    gni_smsg_attr_t *remote_smsg_attributes_array;
    unsigned int    responding_remote_addr;
    int             responding_remote_id;
    msg_header_t    send_header;
    int             send_to;
    gni_cq_handle_t source_cq_handle;
    gni_return_t    status;
    char           *text_pointer;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;

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
     * Determine the number of passes required for this test to be successful.
     */

    expected_passed = (transfers * 6) + ((number_of_ranks - 1) * 3);

    /*
     * Create a handle to the communication domain.
     *    rank_id is the rank of the instance of the job.
     *    ptag is the protection tab for the job.
     *    cookie is a unique identifier created by the system.
     *    modes is a bit mask used to enable various flags.
     *    cdm_handle is the handle that is returned pointing to the
     *        communication domain.
     */

    status = GNI_CdmCreate(rank_id, ptag, cookie, modes, &cdm_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate     ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_TEST;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate     with ptag %u cookie 0x%x\n",
                uts_info.nodename, rank_id, ptag, cookie);
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
     * wait for all of the ranks to attach to the nic.
     */
    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Determine the minimum number of completion queue entries, which is
     * the number of transactions outstanding at one time.  For this test,
     * since there are no barriers between transfers, the number of
     * outstanding transfers could be up to 2.
     * since all messages are sent before the complete queue
     * is checked, we need transfers entries.  But since a message
     * is received from all of the other ranks, we need to multiple
     * transfers by the number of ranks for this application
     * minus one.  We do not send messages to ourselves.
     */

    number_of_cq_entries = (number_of_ranks - 1) * 2;

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
     *     source_cq_handle is the handle that is returned pointing to this
     *          newly created completion queue.
     */

    status =
        GNI_CqCreate(nic_handle, number_of_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &source_cq_handle);
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

    /*
     * Determine the minimum number of completion queue entries, which is
     * the number of transactions outstanding at one time.  For this test,
     * since all messages are sent before the destination complete queue
     * is checked, we need transfers entries.  But since a message
     * is received from all of the other ranks, we need to multiple
     * transfers by the number of ranks for this application
     * minus one.  We do not send messages to ourselves.
     */

    number_of_dest_cq_entries = (number_of_ranks - 1) * 2;

    /*
     * Create the destination completion queue, which is used to receive.
     * message notifications from the other ranks.
     *     nic_handle is the NIC handle that this completion queue will be
     *          associated with.
     *     number_of_dest_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before an
     *          interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the callback
     *          function.
     *     destination_cq_handle is the handle that is returned pointing to this
     *          newly created completion queue.
     */

    status =
        GNI_CqCreate(nic_handle, number_of_dest_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &destination_cq_handle);
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

    /*
     * Create the endpoints to all of the ranks.
     */

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
         *     source_cq_handle is our completion queue handle.
         *     endpoint_handles_array will contain the handle that is returned
         *         for this endpoint instance.
         */

        status = GNI_EpCreate(nic_handle, source_cq_handle,
                              &endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate      ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate      remote rank: %4i NIC: %p CQ: %p EP: %p\n",
                    uts_info.nodename, rank_id, i, nic_handle,
                    source_cq_handle, endpoint_handles_array[i]);
        }

        /*
         * Get the remote address to bind to.
         */

        remote_address = all_nic_addresses[i];

        /*
         * Bind the remote address to the endpoint handler.
         *     endpoint_handles_array is the endpoint handle that is being bound
         *     remote_address is the address that is being bound to this endpoint
         *         handler
         *     i is an unique user specified identifier for this bind.  In this
         *         test i refers to the instance id of the remote communication
         *         domain that we are binding to.
         */

        status = GNI_EpBind(endpoint_handles_array[i], remote_address, i);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }


        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        remote rank: %4i EP:  %p remote_address: %u remote_id: %u\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i], remote_address, i);
        }
    }

    /*
     * Setup the short message attributes.
     *    msg_type determines the message type.
     *        GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT allows the GNI library
     *        to retransmit the message on network errors.
     *    mbox_maxcredit determines the maximum number of messages that
     *        can be in the message buffer at one time.
     *    msq_maxsize determines the maximum size of the message that can
     *        be received.
     */

    local_smsg_attributes.msg_type = GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT;
    local_smsg_attributes.mbox_maxcredit = MB_MAXCREDIT;
    local_smsg_attributes.msg_maxsize = MSG_MAXSIZE;

    /*
     * The GNI_SmsgBufferSizeNeeded determines the amount of memory that is
     * needed for each mailbox.
     *    local_smsg_attributes contains the attributes for the short messages.
     *    bytes_per_mbox contains the returned number of bytes needed.
     */

    status =
        GNI_SmsgBufferSizeNeeded(&local_smsg_attributes, &bytes_per_mbox);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_GetSmsgBufferSize ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_ENDPOINT;
    }

    /*
     * Make sure that the bytes_per_mbox will be on a cache line.
     */

    bytes_per_mbox =
        ((bytes_per_mbox + CACHELINE_SIZE -
          1) / CACHELINE_SIZE) * CACHELINE_SIZE;
    bytes_needed = number_of_ranks * bytes_per_mbox;

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_SmsgBufferSizeNeeded allocated bytes_per_mbox: %d\n",
                uts_info.nodename, rank_id, bytes_per_mbox);
    }

    /*
     * Open the device, zero, for the memory map.
     */

    file_descriptor = open("/dev/zero", O_RDWR, 0);
    if (file_descriptor == -1) {
        fprintf(stdout,
                "[%s] Rank: %4i open /dev/zero ERROR status:\n",
                uts_info.nodename, rank_id);
        perror("FAIL: open of /dev/zero failed: ");
        INCREMENT_ABORTED;
        goto EXIT_ENDPOINT;
    }

    if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i Opened /dev/zero\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Set up a mmap region to contain all of my mailboxes.
     */

    mmap_ptr =
        mmap(NULL, bytes_needed, PROT_READ | PROT_WRITE, MAP_PRIVATE,
             file_descriptor, 0);
    if (mmap_ptr == (char *) MAP_FAILED) {
        perror("FAIL: mmap of /dev/zero failed: ");
        INCREMENT_ABORTED;
        goto EXIT_CLOSE_FILE;
    }

    /*
     * Register the memory associated for the mailboxes with the NIC.
     *     nic_handle is our NIC handle.
     *     mmap_ptr is the memory location of the mailboxes.
     *     bytes_needed is the size of the memory allocated to the mailboxes.
     *     destination_cq_handle is the destination completion queue handle.
     *     GNI_MEM_READWRITE is the read/write attribute for the receive buffer's
     *         memory region.
     *     -1 specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     local_memory_handle is the handle for this memory region.
     */

    status =
        GNI_MemRegister(nic_handle, (unsigned long) mmap_ptr, bytes_needed,
                        destination_cq_handle, GNI_MEM_READWRITE, -1,
                        &local_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister mmap ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_MMAP;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   mmap: size: %u addr: %p\n",
                uts_info.nodename, rank_id, bytes_needed, mmap_ptr);
        fflush(stdout);
    }

    /*
     * Setup the short message attributes.
     *    msg_type determines the message type.
     *        GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT allows the GNI library
     *        to retransmit the message on network errors.
     *    msg_buffer is a pointer to the memory region that contains the
     *        mailboxes.
     *    buff_size contains the size of the memory region, i.e. mailboxes.
     *    mem_hndl is the handle for the memory region.
     *    mbox_maxcredit determines the maximum number of messages that
     *        can be in the message buffer at one time.
     *    msq_maxsize determines the maximum size of the message that can
     *        be received.
     */

    local_smsg_attributes.msg_type = GNI_SMSG_TYPE_MBOX_AUTO_RETRANSMIT;
    local_smsg_attributes.msg_buffer = mmap_ptr;
    local_smsg_attributes.buff_size = bytes_per_mbox;
    local_smsg_attributes.mem_hndl = local_memory_handle;
    local_smsg_attributes.mbox_maxcredit = MB_MAXCREDIT;
    local_smsg_attributes.msg_maxsize = MSG_MAXSIZE;

    /*
     * Allocate memory to hold the small message attributes from the
     * remote nodes.
     */

    remote_smsg_attributes_array =
        (gni_smsg_attr_t *) malloc(number_of_ranks *
                                   sizeof(gni_smsg_attr_t));
    assert(remote_smsg_attributes_array != NULL);

    /*
     * Broadcast my short message attributes to the remote nodes
     * and receive a message from the remote nodes.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpPostData    message send to:   %4i local addr: %p remote addr: %p\n",
                    uts_info.nodename, rank_id, i, &local_smsg_attributes,
                    &remote_smsg_attributes_array[i]);
        }

        /*
         * Determine the mailbox offset for the remote node.
         */

        local_smsg_attributes.mbox_offset = bytes_per_mbox * i;

        /*
         * Send the message attributes to the remote node.
         *     endpoint_handles_array is the endpoint handle for the remote node.
         *     local_smsg_attributes is the message to be sent to the remote node.
         *     sizeof(gni_smsg_attr_t) is the length of the outgoing message.
         *     remote_smsg_attributes_array is pointer to the message buffer
         *         of the received message.
         *     sizeof(gni_smsg_attr_t) is the length of the incoming message.
         */

        status = GNI_EpPostData(endpoint_handles_array[i],
                                &local_smsg_attributes,
                                sizeof(gni_smsg_attr_t),
                                &remote_smsg_attributes_array[i],
                                sizeof(gni_smsg_attr_t));

        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpPostData    ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_MEMORY_LOCAL;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpPostData    message successful\n",
                    uts_info.nodename, rank_id);
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    if (v_option > 2) {
        fprintf(stdout, "[%s] Rank: %4i testing for messages ....\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Determine if the sending of the attributes to all of the remote
     * nodes were successful.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        post_state = GNI_POST_PENDING;

        do {

            /*
             * Get the state of the transfer to each remote node.
             */

            status = GNI_EpPostDataTest(endpoint_handles_array[i],
                                        &post_state,
                                        &responding_remote_addr,
                                        (unsigned int *)
                                        &responding_remote_id);

            if (status == GNI_RC_TIMEOUT) {

                /*
                 * The transfer to this remote node timed out.
                 */

                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpPostDataTest ERROR "
                        "for inst_id %4i returned with TIMEOUT\n",
                        uts_info.nodename, rank_id, i);
                INCREMENT_FAILED;
                break;
            }

            if (status != GNI_RC_SUCCESS) {

                /*
                 * The transfer to this remote node was not successful.
                 */

                fprintf(stdout, "[%s] Rank: %4i GNI_EpPostDataTest ERROR "
                        "status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
                INCREMENT_FAILED;
                break;
            }
        } while (post_state != GNI_POST_COMPLETED);

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpPostDataTest message recv from: %4i state: %i remote addr: 0x%8.8x remote id: %i\n",
                    uts_info.nodename, rank_id, i, post_state,
                    responding_remote_addr, responding_remote_id);
        }

        if (post_state == GNI_POST_COMPLETED) {
            /*
             * Do a basic sanity check on the received remote small
             * message attribute data.
             */
            if (local_smsg_attributes.msg_maxsize !=
                remote_smsg_attributes_array[i].msg_maxsize) {
                INCREMENT_FAILED;

                fprintf(stdout,
                        "[%s] Rank: %4i ERROR invalid smsg attribute from inst_id %d\n",
                        uts_info.nodename, rank_id, responding_remote_id);
            } else {
                INCREMENT_PASSED;
            }
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i initializing short message structures \n",
                uts_info.nodename, rank_id);
    }

    /*
     * Configure the endpoints for short messaging.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgInit      message send to:   %4i msg_type: %i msg_maxsize: %i mbox_maxcredit: %i buff_size: %i\n",
                    uts_info.nodename, rank_id, i,
                    local_smsg_attributes.msg_type,
                    local_smsg_attributes.msg_maxsize,
                    local_smsg_attributes.mbox_maxcredit,
                    local_smsg_attributes.buff_size);
        }

        /*
         * Determine the mailbox offset for the remote node.
         */

        local_smsg_attributes.mbox_offset = bytes_per_mbox * i;

        /*
         * Initialize the endpoints for short messaging.
         *     endpoint_handles_array is the handle of this endpoint.
         *     local_smsg_attributes contains the local attributes.
         *     remote_smsg_attributes_array contains the remote attributes.
         */
        status = GNI_SmsgInit(endpoint_handles_array[i],
                              &local_smsg_attributes,
                              &remote_smsg_attributes_array[i]);

        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgInit      message ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_MEMORY_LOCAL;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgInit      message successful\n",
                    uts_info.nodename, rank_id);
        }
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    /*
     * Determine who we are going to send our messages to and
     * who we are going to receive messages from.
     */

    send_to = (rank_id + 1) % number_of_ranks;
    recv_from = (number_of_ranks + rank_id - 1) % number_of_ranks;

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i sending short messages to rank: %4i \n",
                uts_info.nodename, rank_id, send_to);
    }

    for (n = 0; n < transfers; n++) {

        /*
         * Initialize the message to be sent.
         * The source message will look like: ' msglllllltttttt'
         *     where: msg is the actual value
         *            llllll is the rank for this process
         *            tttttt is the transfer number
         */

        send_header.rank = rank_id;
        send_header.transmission = n + 1;
        sprintf(send_header.buffer, "msg%6.6x%6.6x", rank_id, (n + 1));

        /*
         * Detemine what the received message will look like.
         * The received message will look like: ' msgrrrrrrtttttt'
         *     where: msg is the actual value
         *            rrrrrr is the rank of the remote process,
         *                   that is sending to this process
         *            tttttt is the transfer number
         */

        sprintf(receive_buffer, "msg%6.6x%6.6x", recv_from, (n + 1));

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgSend      message send to:   %4i transfer: %4i buffer: '%16.16s' size: %i\n",
                    uts_info.nodename, rank_id, send_to, (n + 1),
                    send_header.buffer, sizeof(msg_header_t));
        }

        /*
         * Send the message.
         */

        status =
            GNI_SmsgSend(endpoint_handles_array[send_to], &send_header,
                         sizeof(msg_header_t), NULL, 0, 0);
        if ((status != GNI_RC_SUCCESS) && (status != GNI_RC_NOT_DONE)) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgSend      ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_MEMORY_LOCAL;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_SmsgSend      message successful\n",
                    uts_info.nodename, rank_id);
        }

        /*
         * Check the completion queue to verify that the message request has
         * been sent.  The source completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to SmsgSend to fail.
         */

        rc = get_cq_event(source_cq_handle, uts_info, rank_id, 1, 1, &current_event);
        if (rc != 0) {
            /*
             * An error occurred while receiving the event.
             */

            INCREMENT_ABORTED;
            goto EXIT_MEMORY_LOCAL;
        }

        message_count = 0;

        do {
            /*
             * Check the completion queue to verify that the data has
             * been received.  The destination completion queue needs to be
             * checked and events to be removed so that it does not become full
             * and cause succeeding events to be lost.  There are a couple of
             * CQ events that can be in the destination CQ.  The first CQ event
             * states that a message has arrived.  The second CQ event is for
             * accounting purposes, i.e. free up back credits.  The problem
             * is that the user can not distiguish between these two events.
             * So the user must process both of them blindly.
             */

            rc = get_cq_event(destination_cq_handle, uts_info, rank_id, 0, 0, &current_event);
            if (rc == 0) {

                /*
                 * Validate the current event's instance id with the expected id.
                 */

                event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                if (event_inst_id != recv_from) {
                    if (event_inst_id != send_to) {
                        /*
                         * Since this test only sends messages in one direction,
                         * the driver needs to send information back to the
                         * originating node once in a while to clear the back credits.
                         */

                        /*
                         * The event's inst_id was not the expected inst_id
                         * value.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i CQ Event destination ERROR received inst_id: %d, expected inst_id: %d in event_data\n",
                                uts_info.nodename, rank_id, event_inst_id, recv_from);

                        INCREMENT_FAILED;
                    }
                } else {
                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i Verify destination CQ     recv from: %4i transfer: %4i event id: %4i\n",
                                uts_info.nodename, rank_id, recv_from,
                                (n + 1), event_inst_id);
                    }


                    INCREMENT_PASSED;
                }

                status = GNI_SmsgGetNext(endpoint_handles_array[event_inst_id],
                                             (void **) &receive_header);
                if (status == GNI_RC_SUCCESS) {
                    message_count++;

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_SmsgGetNext   message from id:   %4i transfer: %4i buffer: '%16.16s'\n",
                                uts_info.nodename, rank_id, receive_header->rank,
                                receive_header->transmission, receive_header->buffer);
                    }

                    /*
                     * Validate the rank_id in the received header.
                     */

                    if (receive_header->rank != event_inst_id) {
                        fprintf(stdout,
                                "[%s] Rank: %4i Message ERROR rank field, received: %d expected: %d\n",
                                uts_info.nodename, rank_id, receive_header->rank,
                                event_inst_id);
                        INCREMENT_ABORTED;
                        goto EXIT_MEMORY_LOCAL;
                    }

                    INCREMENT_PASSED;

                    /*
                     * Validate the transmission number in the received header.
                     */

                    if (receive_header->transmission != (n + 1)) {
                        fprintf(stdout,
                                "[%s] Rank: %4i Message ERROR transmission field, received: %d expected: %d\n",
                                uts_info.nodename, rank_id,
                                receive_header->transmission, (n + 1));
                        INCREMENT_ABORTED;
                        goto EXIT_MEMORY_LOCAL;
                    }

                    INCREMENT_PASSED;

                    /*
                     * Validate the message in the received header.
                     */

                    if (strcmp(receive_header->buffer, receive_buffer)) {
                        fprintf(stdout,
                                "[%s] Rank: %4i Message ERROR buffer field, received: '%s' expected: '%s'\n",
                                uts_info.nodename, rank_id, receive_header->buffer,
                                receive_buffer);
                        INCREMENT_ABORTED;
                        goto EXIT_MEMORY_LOCAL;
                    }

                    INCREMENT_PASSED;

                    if (v_option > 1) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_SmsgRelease   message transfer:   %4i\n",
                                uts_info.nodename, rank_id, (n + 1));
                    }

                    /*
                     * Release the current message.
                     */

                    status = GNI_SmsgRelease(endpoint_handles_array[event_inst_id]);
                    if ((status != GNI_RC_SUCCESS) && (status != GNI_RC_NOT_DONE)) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_SmsgRelease   ERROR status: %s (%d)\n",
                                uts_info.nodename, rank_id, gni_err_str[status], status);
                        INCREMENT_ABORTED;
                        goto EXIT_MEMORY_LOCAL;
                    }

                    INCREMENT_PASSED;
                }
            } else if (rc == 3) {
                if (message_count > 0) {
                    break;
                }
            }
        } while(1 == 1);

        /*
         * Wait for all the processes to finish receiving
         * a message before sending the next one.
         */

        rc = PMI_Barrier();
        assert(rc == PMI_SUCCESS);

        if (v_option) {

            /*
             * Write out all of the output messages.
             */

            fflush(stdout);
        }
    }

    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Free allocated memory.
     */

    free (remote_smsg_attributes_array);

  EXIT_MEMORY_LOCAL:

    /*
     * Deregister the memory associated for the mailboxes with the NIC.
     *     nic_handle is our NIC handle.
     *     local_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &local_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister mmap ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister mmap              NIC: %p\n",
                uts_info.nodename, rank_id, nic_handle);
    }

  EXIT_MMAP:

    /*
     * Unmap the mmap region that contains all of my mailboxes.
     */

    (void) munmap(mmap_ptr, bytes_needed);

  EXIT_CLOSE_FILE:

    /*
     * Close the device, zero, that contains the memory map.
     */

    (void) close(file_descriptor);

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
            INCREMENT_ABORTED;
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
            INCREMENT_ABORTED;
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

    /*
     * Destroy the destination completion queue.
     *     destination_cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(destination_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy     destination ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy     destination\n",
                uts_info.nodename, rank_id);
    }

  EXIT_CQ:

    /*
     * Destroy the completion queue.
     *     source_cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(source_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy     source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
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
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmDestroy\n",
                uts_info.nodename, rank_id);
    }

  EXIT_TEST:

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
