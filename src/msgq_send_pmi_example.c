/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * Test basic FMA short message functionality - this test only uses PMI
 * GNI_MsgqInit, GNI_MsgqConnect, GNI_MsgqSend, GNI_MsgqProgress
 *
 * NOTE: This test requires a minimum of 2 nodes.
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

#define MESSAGE_TAG_FACTOR    5
#define MESSAGE_TAG_SIZE      255
#define NUMBER_OF_TRANSFERS   200
#define DEFAULT_TIMEOUT_VALUE 1

typedef struct {
    uint32_t        sender_id;
    uint32_t        receiver_id;
    uint32_t        transmission;
} msg_header_t;

typedef struct {
    char            buffer[16];
} msg_data_t;

typedef struct {
    uint32_t        rank;
    uint32_t        send_to_id;
    uint32_t        receive_from_id;
    uint32_t        transmission;
    uint32_t        received_count;
    uint32_t        last_received_transmission;
    uint8_t         sender_tag;
    uint8_t         receiver_tag;
    char            buffer[16];
} callback_info_t;

int             rank_id;
struct utsname  uts_info;
int             v_option = 0;

#include "utility_functions.h"

void print_help(void)
{
    fprintf(stdout,
"\n"
"MSGQ_SEND_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the sending and receiving\n"
"    of messages using a shared message queue.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_MsgqInit() is used to create and initialize the resources for\n"
"        a shared message queue.\n"
"      - GNI_MsgqGetConnAttrs() is used to assign connection attributes to\n"
"        a remote endpoint address.\n"
"      - GNI_EpPostData() is used to post a datagram to a remote bounded\n"
"        communication endpoint.\n"
"      - GNI_EpPostDataWait() is used to block until the previously posted\n"
"        datagram completes or times out.\n"
"      - GNI_MsgqConnect() is used to connect to a shared message queue.\n"
"      - GNI_MsgqSend() is used to send a message to the shared message\n"
"        queue.\n"
"      - GNI_MsgqProgress() is used to poll the shared message queue until\n"
"        an event is received or times out.\n"
"      - GNI_MsgqConnRelease() is used to release the connection resources\n"
"        associated with a remote endpoint.\n"
"      - GNI_MsgqRelease() is used to release the resources associated\n"
"        with the shared message queue.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-a' send message from all ranks to all of the other ranks.\n"
"      2.  '-B' specifies that the shared message queue will set to\n"
"          non-blocking with a non-blocking timeout value.\n"
"          The default value is that the shared message queue will set\n"
"          up in blocking mode.\n"
"      3.  '-h' prints the help information for this example.\n"
"      4.  '-m' specifies the number of mailbox credits that will be\n"
"          allocated for the shared message queue.\n"
"          The default value is 2 for the number of mailbox credits.\n"
"      5.  '-n' specifies the number of messages that will be sent.\n"
"          The default value is 200 data transactions to be sent.\n"
"      6.  '-t' specifies the timeout value that will be used with the\n"
"          'MsgqProgress' API.\n"
"          The default value is 1 milliseconds.\n"
"      7.  '-T' specifies the timeout value will be set to non-blocking\n"
"          for the 'MsgqProgress' API.\n"
"          The default value is 20 milliseconds.\n"
"      8.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - msgq_send_pmi_example\n"
"      - msgq_send_pmi_example -a\n"
"      - msgq_send_pmi_example -B\n"
"\n"
    );
}

/*
 *
 * msgq_callback_handler
 *
 */

int
msgq_callback_handler(uint32_t sender_id, uint32_t sender_pe,
                      void *message, uint8_t message_tag,
                      void *sender_info)
{
    callback_info_t *callback_info = (callback_info_t *)sender_info;
    void            *data = message + sizeof(msg_header_t);
    msg_header_t    *head = message;
    int              mismatches = 0;
    int              rc = 0;

    if (v_option) {
        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler in  parameters  sender_id:  %4i sender_pe:     %4i message_tag:     %4i\n",
                uts_info.nodename, rank_id, sender_id, sender_pe,
                message_tag);

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler in  message     sender_id:  %4i receiver_id:   %4i transmission:    %4i data:           '%16.16s'\n",
                uts_info.nodename, rank_id, head->sender_id,
                head->receiver_id, head->transmission, (char *) data);

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler in  sender_info rank:       %4i send_to_id:    %4i receive_from_id: %4i transmission:   %4i sender_tag: %i receiver_tag: %i\n",
                uts_info.nodename, rank_id, callback_info[sender_id].rank,
                callback_info[sender_id].send_to_id, callback_info[sender_id].receive_from_id,
                callback_info[sender_id].transmission, callback_info[sender_id].sender_tag,
                callback_info[sender_id].receiver_tag);
    }

    /*
     * Verify that the shared message sender_id is the same as the
     * sender_id.
     */

    if (head->sender_id != sender_id) {

        /*
         * The shared message's sender_id was not the expected sender_id
         * value.
         */

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler shared message ERROR received sender_id: %d, expected sender_id: %d in message\n",
                uts_info.nodename, rank_id, head->sender_id, sender_id);

        INCREMENT_FAILED;
        mismatches++;
    }

    /*
     * Verify that the shared message sender_id is the same as
     * the caller's receive_from_id value.
     */

    if (head->sender_id != callback_info[sender_id].receive_from_id) {

        /*
         * The shared message's sender_id was not the expected sender_id
         * value.
         */

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler shared message callback ERROR received sender_id: %d, expected received_from_id: %d in message\n",
                uts_info.nodename, rank_id, head->sender_id,
                callback_info[sender_id].receive_from_id);

        INCREMENT_FAILED;
        mismatches++;
    }

    /*
     * Verify that the shared message receiver_id is the same as
     * the caller's rank value.
     */


    if (head->receiver_id != callback_info[sender_id].rank) {

        /*
         * The shared message's receiver_id was not the expected
         * receiver_id value.
         */

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler shared message ERROR received receiver_id: %d, expected receiver_id: %d in message\n",
                uts_info.nodename, rank_id, head->receiver_id,
                callback_info[sender_id].rank);

        INCREMENT_FAILED;
        mismatches++;
    }

    /*
     * Verify that the shared message transmission number is the same as
     * the caller's transmission value.
     */

    if (head->transmission > callback_info[sender_id].transmission) {

        /*
         * The shared message's transmission was not the expected
         * transmission value.
         */

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler shared message ERROR received transmission: %d, expected transmission: %d in message\n",
                uts_info.nodename, rank_id, head->transmission,
                callback_info[sender_id].transmission);

        INCREMENT_FAILED;
        mismatches++;
    }

    /*
     * Verify that the shared message tag value is the same as
     * the caller's receiver_tag value.
     */

    if (message_tag > callback_info[sender_id].receiver_tag) {

        /*
         * The shared message's message_tag was not the expected
         * receiver_tag value.
         */

        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler shared message ERROR received message_tag: %d, expected receiver_tag: %d in message\n",
                uts_info.nodename, rank_id, message_tag,
                callback_info[sender_id].receiver_tag);

        INCREMENT_FAILED;
        mismatches++;
    }

    /*
     * Update the callers received_count and last_received_transmission
     * values.  This allows the caller to know what message has been
     * received.
     */

    callback_info[sender_id].received_count++;
    callback_info[sender_id].last_received_transmission = head->transmission;

    if (mismatches == 0) {
        INCREMENT_PASSED;
        rc = 1;
    }

    if (v_option) {
        fprintf(stdout,
                "[%s] Rank: %4i msgq_callback_handler out sender_info rank:       %4i transmission:  %4i last received:   %4i received count: %4i returned status: %i\n",
                uts_info.nodename, rank_id, callback_info[sender_id].rank,
                callback_info[sender_id].transmission,
                callback_info[sender_id].last_received_transmission,
                callback_info[sender_id].received_count, rc);
    }

    return rc;
}

static inline int
is_rank_local(int rank, int number_of_ranks_on_node, int *ranks_on_node) {
    int index;

    if ((ranks_on_node != NULL) && (number_of_ranks_on_node > 0)) {
        for (index = 0; index < number_of_ranks_on_node; index++) {
            if (rank == ranks_on_node[index]) {
                return(1);
            }
        }
    }

    return(0);
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    int             all_to_all = 0;
    int             attributes_length;
    int             bytes_sent;
    gni_cdm_handle_t cdm_handle;
    int             cookie;
    gni_cq_entry_t  current_event;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    int             first_rank;
    int             first_spawned;
    int             i;
    int             last_rank;
    unsigned int    local_address;
    gni_msgq_attr_t msgq_attributes;
    int             msgq_block_mode = GNI_MSGQ_MODE_BLOCKING;
    gni_msgq_ep_attr_t msgq_endpoint_attributes;
    uint32_t        msgq_endpoint_attributes_length;
    gni_msgq_handle_t msgq_handle;
    uint32_t        msgq_mailbox_credits = 0;
    uint32_t        msgq_sender_id;
    uint8_t         msgq_sender_tag;
    int             modes = 0;
    callback_info_t *my_callback_info;
    int             my_id;
    uint8_t         my_recv_tag;
    uint8_t         my_send_tag;
    register int    n;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries;
    int             number_of_ranks;
    int             number_of_ranks_on_node = 0;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    gni_msgq_ep_attr_t peer_msgq_endpoint_attributes;
    gni_post_state_t post_state;
    uint8_t         ptag;
    int            *ranks_on_node;
    int             rc;
    void           *receive_attributes;
    char            receive_buffer[16];
    int             recv_from;
    unsigned int    receive_remote_address;
    unsigned int    receive_remote_id;
    unsigned int    remote_address;
    void           *send_attributes;
    msg_header_t    send_header;
    msg_data_t      send_data;
    int             send_to;
    int             send_to_rank;
    gni_cq_handle_t source_cq_handle;
    gni_return_t    status;
    char           *text_pointer;
    uint32_t        timeout_value = DEFAULT_TIMEOUT_VALUE;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;
    int             use_this_rank = 1;

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

    rc = PMI_Get_clique_size(&number_of_ranks_on_node);
    assert(rc == PMI_SUCCESS);

    rc = PMI_Get_rank(&rank_id);
    assert(rc == PMI_SUCCESS);

    /*
     * Get the number of ranks on the local node.
     */

    ranks_on_node = (int *) calloc(number_of_ranks_on_node, sizeof(int));
    assert(ranks_on_node != NULL);

    /*
     * Get the list of ranks on the local node.
     */

    rc = PMI_Get_clique_ranks(ranks_on_node, number_of_ranks_on_node);
    assert(rc == PMI_SUCCESS);

    while ((opt = getopt(argc, argv, "aBhm:n:t:Tv")) != -1) {
        switch (opt) {
        case 'a':

            /*
             * Send messages from all ranks to all of the other ranks.
             */

            all_to_all = 1;
            break;

        case 'B':

            /*
             * Set the shared message queue mode attribute to non-blocking.
             */

            msgq_block_mode = 0;

            /*
             * If the shared message queue mode attribute is non-blocking,
             * then the timeout value must be -1.
             */

            timeout_value = -1;
            break;

        case 'h':
            if (rank_id == 0) {
                print_help();
             }

            /*
             * Clean up the PMI information.
             */

            PMI_Finalize();

            exit(0);

        case 'm':

            /*
             * Set the number of mailbox credits that will be allocated
             * for the shared message queue.
             */

            msgq_mailbox_credits = atoi(optarg);
            if (msgq_mailbox_credits <= 0) {
                msgq_mailbox_credits = 0;
            }

            break;

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

        case 't':

            /*
             * Set the timeout value for GNI_MsgqProgress.
             */

            if (timeout_value != -1) {
                timeout_value = atoi(optarg);
            }
            break;

        case 'T':

            /*
             * Set the timeout value to non-blocking for GNI_MsgqProgress.
             */

            timeout_value = -1;
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
     * This test requires at least 2 nodes.
     */

    if (number_of_ranks < 2) {
        fprintf(stderr, "%s requires at least 2 nodes\n", command_name);
        exit(1);
    }

    /*
     * Find the lowest rank on this node.
     */

    for (i = 0; i < number_of_ranks_on_node; i++) {
        if (rank_id > ranks_on_node[i]) {
            use_this_rank = 0;
        }

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i Local Rank Info       number_of_local_ranks: %i local_rank: %4i\n",
                    uts_info.nodename, rank_id, number_of_ranks_on_node, ranks_on_node[i]);
        }
    }

    /*
     * Determine the number of passes required for this test to be
     * successful.
     */

    if (use_this_rank == 1) {
        if (all_to_all == 0) {
            if (number_of_ranks_on_node > 1) {
                expected_passed = (((number_of_ranks / number_of_ranks_on_node)
                                    - 1) * 3) + 1;

                if (((rank_id + 1) % number_of_ranks_on_node) == 0) {
                    /* This rank will send a message */
                    expected_passed = expected_passed + (transfers * 2);
                }

                if ((rank_id % number_of_ranks_on_node) == 0) {
                    /* This rank will receive a message */
                    expected_passed = expected_passed + transfers;
                }
            } else {
                expected_passed = (transfers * 3) + ((number_of_ranks - 1) * 3) + 1;
            }
        } else {
            expected_passed = (transfers * 3 *
                               (((number_of_ranks / number_of_ranks_on_node) - 1) *
                                number_of_ranks_on_node)) +
                              (((number_of_ranks / number_of_ranks_on_node) - 1) * 3) + 1;
        }
    } else {
        if (all_to_all == 0) {
            if (number_of_ranks_on_node > 1) {
                expected_passed = 1;

                if (((rank_id + 1) % number_of_ranks_on_node) == 0) {
                    /* This rank will send a message */
                    expected_passed = expected_passed + (transfers * 2);
                }

                if ((rank_id % number_of_ranks_on_node) == 0) {
                    /* This rank will receive a message */
                    expected_passed = expected_passed + transfers;
                }
            } else {
                expected_passed = (transfers * 3) + 1;
            }
        } else {
            expected_passed = (transfers * 3 *
                               (((number_of_ranks / number_of_ranks_on_node) - 1) *
                                number_of_ranks_on_node)) + 1;
        }
    }

    my_callback_info = (callback_info_t *) calloc(number_of_ranks,
                                           sizeof(callback_info_t));
    assert(my_callback_info != NULL);

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
                "[%s] Rank: %4i GNI_CdmCreate         ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_TEST;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate         with ptag %u cookie 0x%x\n",
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
                "[%s] Rank: %4i GNI_CdmAttach         ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmAttach         to NIC\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Determine the minimum number of completion queue entries, which is
     * the number of transactions outstanding at one time.  For this test,
     * since there are no barriers between transfers, the number of
     * outstanding transfers could be up to the number of transfers.
     */

    if (all_to_all == 0) {
        number_of_cq_entries = transfers;
    } else {
        number_of_cq_entries = transfers * (number_of_ranks - 1);
    }

    /*
     * Create the completion queue.
     *     nic_handle is the NIC handle that this completion queue will be
     *          associated with.
     *     number_of_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before
     *          an interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the
     *          callback function.
     *     source_cq_handle is the handle that is returned pointing to
     *          this newly created completion queue.
     */

    status =
        GNI_CqCreate(nic_handle, number_of_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &source_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate          source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate          source with %i entries\n",
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

    /*
     * Create the endpoints to all of the ranks.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (i == rank_id) {
            continue;
        }

        /*
         * You must do an EpCreate for each endpoint pair.  That is, for
         * each remote node that you will want to communicate with.
         * The EpBind request updates some fields in the endpoint_handle
         * so this is the reason that all pairs of endpoints need to be
         * created.
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
                    "[%s] Rank: %4i GNI_EpCreate          ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate          remote rank: %4i NIC: %p CQ: %p EP: %p\n",
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
         *     remote_address is the address that is being bound to this
         *         endpoint handler
         *     i is an unique user specified identifier for this bind.  In this
         *         test i refers to the instance id of the remote communication
         *         domain that we are binding to.
         */

        status = GNI_EpBind(endpoint_handles_array[i], remote_address, i);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind            ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }


        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind            remote rank: %4i EP:  %p remote_address: %u remote_id: %u\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i], remote_address, i);
        }
    }

    /*
     * Wait for all the processes to finish binding their endpoints.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Setup the message queue attributes.
     *    max_msg_sz determines the maximum message size.
     *    smsg_q_sz determines the maximum number of short
     *        messages that fit in the connection buffer.
     *    rcv_pool_sz determines the maximum number of messages
     *        that can be in the receive pool.
     *    num_msgq_eps determines the maximum number of connections
     *        that will be made to this shared message queue.
     *    nloc_insts determines the number of local instances
     *        attaching to the message queue system from this node.
     *    modes determines the type of blocking used for the
     *        message queue system.
     *            0 (zero) is used for non-blocking.
     *            GNI_MSGQ_MODE_BLOCKING is used for blocking.
     *    rcv_cq_sz determines the number of entries in the
     *        receive completion queue.
     */

    /*
     * The max_msg_sz for this test will be the sizeof the msg_header_t
     * plus the sizeof the msg_data_t.
     */

    msgq_attributes.max_msg_sz = sizeof(msg_header_t) + sizeof(msg_data_t);

    /*
     * The msgq_mailbox_credits should be at least one greater that the number
     * of messages that will be outstanding at one time.  For this test
     * the msgq_mailbox_credits should be set to two times the number of ranks.
     *
     * NOTE: The msgq_mailbox_credits value can be one, but if it is
     *       greater than one it can not be an odd value.
     */

    if (msgq_mailbox_credits == 0) {
       /*
        *If the mailbox has not been set, then set the initial mailbox size.
        */
        msgq_mailbox_credits = 2 * number_of_ranks;
    }

    if ((msgq_mailbox_credits != 1) && ((msgq_mailbox_credits % 2) == 1)) {
        msgq_mailbox_credits++;
    }

    msgq_attributes.smsg_q_sz = msgq_mailbox_credits;

    if (all_to_all == 0) {
        /*
         * The receive_pool_size for this test should be one.
         * Only one is needed because that is the number of transfers
         * that will be outstanding at one time.
         */

        msgq_attributes.rcv_pool_sz = 1;
    } else {
        /*
         * The receive_pool_size for this test should be number
         * of ranks minus one.  This amount is needed because
         * there can be one outstanding transfer from each rank.
         */

        msgq_attributes.rcv_pool_sz = number_of_ranks_on_node * number_of_ranks_on_node *
                                      (number_of_ranks / number_of_ranks_on_node) *
                                      ((number_of_ranks / number_of_ranks_on_node) - 1);
    }

    /*
     * The number_of_eps should be the number of nodes that this
     * application will connect to.  This value is the number of nodes
     * minus one.  Do not count ourself.
     */

    msgq_attributes.num_msgq_eps = number_of_ranks - 1;

    /*
     * The nloc_insts is the number of local instances that this
     * application will connect to on the local node.
     */

    msgq_attributes.nloc_insts = number_of_ranks_on_node;

    /*
     * The modes for this test is configurable.  It can be either
     * blocking or non-blocking.
     */

    msgq_attributes.modes = msgq_block_mode;

    if (all_to_all == 0) {
        /*
         * The rcv_cq_sz for this test should be number of ranks minus one.
         * The minus one is because we do not send to ourselves.
         */

        msgq_attributes.rcv_cq_sz = number_of_ranks - 1;
    } else {
        /*
         * The rcv_cq_sz for this test should be number of ranks minus one.
         * The minus one is because we do not send to ourselves.
         */

        msgq_attributes.rcv_cq_sz = number_of_ranks_on_node * number_of_ranks_on_node *
                                    (number_of_ranks / number_of_ranks_on_node) *
                                    ((number_of_ranks / number_of_ranks_on_node) - 1);
    }

    if (v_option) {
        fprintf(stdout, "[%s] Rank: %4i GNI_MsgqInit          max_msg_sz: %i smsg_q_sz: %i rcv_pool_sz: %i num_msgq_eps: %i nloc_insts: %i modes: %i rcv_cq_sz: %i\n",
                uts_info.nodename, rank_id,
                msgq_attributes.max_msg_sz,
                msgq_attributes.smsg_q_sz,
                msgq_attributes.rcv_pool_sz,
                msgq_attributes.num_msgq_eps,
                msgq_attributes.nloc_insts,
                msgq_attributes.modes,
                msgq_attributes.rcv_cq_sz);
    }

    /*
     * Create the resources needed for shared messaging.
     *     nic_handle is the NIC handle that the shared message queue
     *         will be attached to.
     *     msgq_callback_handler is the function that will be called
     *         to process the received messages.
     *     my_callback_info is some user data that is passed to the
     *         callback function.
     *     source_cq_handle is the completion queue that will be
     *         used to receive completion events.
     *     msgq_attributes contains the attributes to configure the
     *         share message queue.
     *     msgq_handle is the handle that is returned pointing to this
     *          newly created shared message queue.
     */

    status = GNI_MsgqInit(nic_handle, &msgq_callback_handler,
                          my_callback_info,
                          source_cq_handle, &msgq_attributes,
                          &msgq_handle);

    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MsgqInit          ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_ENDPOINT;
    }

    INCREMENT_PASSED;

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MsgqInit          successful\n",
                uts_info.nodename, rank_id);
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    /*
     * Wait for all the processes to finish creating the shared
     * message queues.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    if (use_this_rank == 1) {

        for (i = 0; i < number_of_ranks; i += number_of_ranks_on_node) {
            if (is_rank_local(i, number_of_ranks_on_node, ranks_on_node) == 1) {
                continue;
            }

            /*
             * Get the remote address to get connection attributes from.
             */

            remote_address = all_nic_addresses[i];

            if (v_option) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqGetConnAttrs  send to:   %4i remote addr: %u\n",
                        uts_info.nodename, rank_id, i, remote_address);
            }

            /*
             * Get the share message queue connection attributes.
             *     msgq_handle is the handle for the shared message queue.
             *     remote_address is the address of the the remote endpoint
             *         that we want to communication with.
             *     msgq_endpoint_attributes is the remote endpoint attributes.
             *     msgq_endpoint_attributes_length is the length of the
             *         msgq_endpoint_attributes structure.
             */

            status = GNI_MsgqGetConnAttrs(msgq_handle, remote_address,
                                          &msgq_endpoint_attributes,
                                          &msgq_endpoint_attributes_length);

            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqGetConnAttrs  ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_MSGQRELEASE;
            }

            INCREMENT_PASSED;

            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqGetConnAttrs  successful\n",
                        uts_info.nodename, rank_id);
            }

            /*
             * The message queue attribute structure is larger than the
             * maximum datagram payload size, so we need to send the
             * attributes in multiple messages.
             */
 
            bytes_sent = 0;

            while(bytes_sent < msgq_endpoint_attributes_length) {

                send_attributes = ((void *)&msgq_endpoint_attributes) + bytes_sent;
                receive_attributes = ((void *)&peer_msgq_endpoint_attributes) + bytes_sent;

                attributes_length = (msgq_endpoint_attributes_length - bytes_sent) >=
                                     GNI_DATAGRAM_MAXSIZE ?
                                     GNI_DATAGRAM_MAXSIZE :
                                     msgq_endpoint_attributes_length - bytes_sent;

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData        message send to:   %4i local addr: %p length: %i\n",
                            uts_info.nodename, rank_id, i, &msgq_attributes,
                            attributes_length);
                }

                /*
                 * Send the message attributes to the remote node.
                 *     endpoint_handles_array is the endpoint handle for
                 *         the remote node.
                 *     send_attributes is the message to be sent to the
                 *         remote node.
                 *     attributes_length is the length of the outgoing message.
                 *     receive_attributes is pointer to the message buffer
                 *         of the received message.
                 *     attributes_length is the length of the incoming message.
                 */

                status = GNI_EpPostData(endpoint_handles_array[i],
                                        send_attributes,
                                        attributes_length,
                                        receive_attributes,
                                        attributes_length);

                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData        ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_MSGQCONNRELEASE;
                }

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData        message successful\n",
                            uts_info.nodename, rank_id);
                }

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWait    send to:   %4i\n",
                            uts_info.nodename, rank_id, i);
                }

                post_state = GNI_POST_PENDING;

                do {

                    /*
                     * Wait for the message attributes to be received from the
                     * remote node.
                     *     endpoint_handles_array is the endpoint handle for
                     *         the remote node.
                     *     -1 states that we will block until a messages is
                     *         received.
                     *     post_state tells us the state of the transaction.
                     *     receive_remote_address tells us the physical
                     *         address of the nic for the remote peer.
                     *     receive_remote_id is the user specified id of the
                     *         remote instance of this job.
                     */

                    status = GNI_EpPostDataWait(endpoint_handles_array[i], -1,
                                                &post_state,
                                                &receive_remote_address,
                                                &receive_remote_id);

                    if (status != GNI_RC_SUCCESS) {

                        /*
                         * The transfer from the remote node was not successful.
                         */

                        fprintf(stdout, "[%s] Rank: %4i GNI_EpPostDataWait    ERROR "
                                "status: %s (%d)\n",
                                uts_info.nodename, rank_id, gni_err_str[status], status);
                        INCREMENT_ABORTED;
                        goto EXIT_MSGQCONNRELEASE;
                    }

                    if (v_option > 2) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWait    successful\n",
                                uts_info.nodename, rank_id);
                    }

                    if (post_state == GNI_POST_COMPLETED) {

                        /* Received a message, so lets continue on. */

                        break;
                    } else if (post_state != GNI_POST_PENDING) {

                        /*
                         * An error was returned. Since we can not get the
                         * remote attributes for the peer, we will terminate.
                         */

                        INCREMENT_ABORTED;
                        goto EXIT_MSGQCONNRELEASE;
                    }
                } while(1 == 1);

                /*
                 * Do a basic sanity check on the received remote small
                 * message attribute data.
                 */

                if (receive_remote_id != i) {

                    /*
                     * The received remote id was not from the peer that we
                     * expecting the message from.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWait    ERROR received remote id: %4i, expected remote id: %4i\n",
                            uts_info.nodename, rank_id, receive_remote_id, i);

                    INCREMENT_ABORTED;
                    goto EXIT_MSGQCONNRELEASE;
                }

                if (receive_remote_address != remote_address) {

                    /*
                     * The received remote address was not from the peer that
                     * we were expecting the message from.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWait    ERROR received remote address: %4i, expected remote address: %4i\n",
                            uts_info.nodename, rank_id, receive_remote_address, remote_address);

                    INCREMENT_ABORTED;
                    goto EXIT_MSGQCONNRELEASE;
                }

                bytes_sent += attributes_length;
            }

            INCREMENT_PASSED;

            if (v_option) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqConnect       remote rank: %4i remote address: %u\n",
                        uts_info.nodename, rank_id, i, remote_address);
            }

            /*
             * Connect the endpoints for shared messaging.
             *     msgq_handle is the handle for this shared message queue.
             *     remote_address is the address of the remote end point.
             *     peer_msgq_endpoint_attributes is the attributes received
             *         from the remote node.
             */

            status = GNI_MsgqConnect(msgq_handle, remote_address,
                              &peer_msgq_endpoint_attributes);
            if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MsgqConnect       ERROR remote rank: %4i remote address: %u status: %s (%d)\n",
                            uts_info.nodename, rank_id, i, remote_address, gni_err_str[status], status);
                    goto EXIT_MSGQCONNRELEASE;
            }

            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqConnect       successful\n",
                        uts_info.nodename, rank_id);
            }

            INCREMENT_PASSED;

            if (v_option) {

                /*
                 * Write out all of the output messages.
                 */

                fflush(stdout);
            }
        }
    }

    /*
     * Wait for all the processes to finish connecting to the shared
     * message queues.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    my_id = (rank_id & 0xffffff) << 24;
    my_send_tag = rank_id * MESSAGE_TAG_FACTOR;

    /*
     * Initialize the information that is passed to the callback
     * function.
     */

    for (send_to_rank = 0; send_to_rank < number_of_ranks; send_to_rank++) {
        if (send_to_rank == rank_id) {
            /* Do not initialize my callback information. */

            continue;
        }

        my_callback_info[send_to_rank].rank = rank_id;
        my_callback_info[send_to_rank].send_to_id = send_to_rank;
        my_callback_info[send_to_rank].receive_from_id = send_to_rank;
    }

    for (n = 0; n < transfers; n++) {

        if (all_to_all == 0) {
            recv_from = (number_of_ranks + rank_id - 1) % number_of_ranks;
            my_recv_tag = ((number_of_ranks + rank_id - 1) % number_of_ranks) * MESSAGE_TAG_FACTOR;
            first_rank = (rank_id + 1) % number_of_ranks;
            last_rank = first_rank + 1;
        } else {
            send_to = 0;
            recv_from = 0;
            my_recv_tag = 0;
            first_rank = 0;
            last_rank = number_of_ranks;
        }

        /*
         * Initialize the send id and message tag.
         */

        msgq_sender_id = my_id + n + 1;
        msgq_sender_tag = (my_send_tag + n + 1) % MESSAGE_TAG_SIZE;

        for (send_to_rank = 0; send_to_rank < number_of_ranks; send_to_rank++) {
            if (send_to_rank == rank_id) {
                /* Do not initialize my callback information. */

                continue;
            }

            my_recv_tag = send_to_rank * MESSAGE_TAG_FACTOR;

            /*
             * Initialize the information, that this passed to the callback
             * function, which changes for each message.
             */
 
            my_callback_info[send_to_rank].transmission = n + 1;
            my_callback_info[send_to_rank].receiver_tag = (my_recv_tag + n + 1) % MESSAGE_TAG_SIZE;
            my_callback_info[send_to_rank].sender_tag = msgq_sender_tag;
            sprintf(my_callback_info[send_to_rank].buffer, "msg%6.6x%6.6x", rank_id, (n + 1));
        }

        /*
         * Wait for all the processes to initialize the message.
         */

        rc = PMI_Barrier();
        assert(rc == PMI_SUCCESS);

        /* Send a message to the following ranks. */

        for (send_to_rank = first_rank; send_to_rank < last_rank; send_to_rank++) {

            /*
             * Determine who we are going to send our messages to and
             * who we are going to receive messages from.
             */

            if (all_to_all == 1) {
                /* This allows each rank on a node to send and
                 * receive a message from one rank on another node.
                 */
                recv_from = (rank_id + send_to_rank + number_of_ranks -
                             number_of_ranks_on_node) % number_of_ranks;
                send_to = (rank_id + send_to_rank + number_of_ranks_on_node)
                          % number_of_ranks;
            } else {
                send_to = send_to_rank;
            }

            if (is_rank_local(send_to, number_of_ranks_on_node, ranks_on_node) == 0) {
                /* Only send a message to ranks not on this node. */

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i sending messages   to rank: %4i \n",
                            uts_info.nodename, rank_id, send_to);
                }

                /*
                 * Initialize the sending message with our information.
                 */

                send_header.sender_id = rank_id;
                send_header.receiver_id = send_to;

                /*
                 * Initialize the message to be sent.
                 * The source message will look like: ' msglllllltttttt'
                 *     where: msg is the actual value
                 *            llllll is the rank for this process
                 *            tttttt is the transfer number
                 */

                send_header.transmission = n + 1;
                sprintf(send_data.buffer, "msg%6.6x%6.6x", rank_id, (n + 1));

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
                            "[%s] Rank: %4i GNI_MsgqSend          send to:   %4i transfer: %4i msg_id: 0x%8.8x msg_tag: %i data: '%16.16s'\n",
                            uts_info.nodename, rank_id, send_to, (n + 1),
                            msgq_sender_id, msgq_sender_tag, send_data.buffer);
                }

                /*
                 * Send the message.
                 *     msgq_handle is the handle for this shared message queue.
                 *     endpoint_handles_array is the endpoint handle for
                 *         the remote node.
                 *     send_header is the address to the message header to be sent.
                 *     sizeof(msg_header_t) is the length of the message header.
                 *     send_data.buffer is athe address of the message to be sent.
                 *     sizeof(msg_data_t) is the length of the message data.
                 *     msgq_sender_id is a user created message id that will
                 *         be returned in a successful completion event.
                 *     msgq_sender_tag is a user created message tag.
                 */

                do {
                    status =
                        GNI_MsgqSend(msgq_handle, endpoint_handles_array[send_to],
                                     (void *) &send_header,
                                     sizeof(msg_header_t), &send_data.buffer,
                                     sizeof(msg_data_t), msgq_sender_id,
                                     msgq_sender_tag);
                    if ((status != GNI_RC_SUCCESS) && (status != GNI_RC_NOT_DONE)) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_MsgqSend          ERROR status: %s (%d)\n",
                                uts_info.nodename, rank_id, gni_err_str[status], status);
                        INCREMENT_ABORTED;
                        goto EXIT_MSGQCONNRELEASE;
                    }
                } while (status == GNI_RC_NOT_DONE);

                INCREMENT_PASSED;

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MsgqSend          successful\n",
                            uts_info.nodename, rank_id);
                }

                /*
                 * Check the completion queue to verify that the message request
                 * has been sent.  The source completion queue needs to be checked and
                 * events to be removed so that it does not become full and cause
                 * succeeding calls to MsgqSend to fail.
                 */

                rc = get_cq_event(source_cq_handle, uts_info, rank_id, 1, 1, &current_event);
                if (rc == 0) {

                    /*
                     * Verify that the completion queue event contains the
                     * message id of the sender.
                     */

                    if (GNI_CQ_GET_MSG_ID(current_event) == msgq_sender_id) {
                        INCREMENT_PASSED;
                    } else {
                        fprintf(stdout,
                                "[%s] Rank: %4i get_cq_event          ERROR invalid msg_id: 0x%8.8x expected msg_id: 0x%8.8x\n",
                                uts_info.nodename, rank_id,
                                (unsigned int) GNI_CQ_GET_MSG_ID(current_event),
                                msgq_sender_id);
                        INCREMENT_FAILED;
                    }
                } else {

                    /*
                     * An error occurred while receiving the event.
                     */

                    INCREMENT_FAILED;
                }
            }

            if (is_rank_local(recv_from, number_of_ranks_on_node, ranks_on_node) == 0) {
                /* Only receive a message from ranks not on this node. */
                if (v_option) {
                    fprintf(stdout, "[%s] Rank: %4i GNI_MsgqProgress      timeout: %i blocked: %i\n",
                            uts_info.nodename, rank_id, timeout_value,
                            msgq_block_mode); 
                }

                do {
                    /*
                     * Get the next message from the shared message queue.
                     *     msgq_handle is the handle for this shared message queue.
                     *     timeout_value is the amount of time we will wait for
                     *         a message to be received.
                     *             -1 means that we will not block waiting for a
                     *                 message.
                     *             a value greater than zero means that we will
                     *                 block waiting for a message for this
                     *                 amount of milliseconds.
                     */

                    status = GNI_MsgqProgress(msgq_handle, timeout_value);

                    if ((status != GNI_RC_NOT_DONE) && (status != GNI_RC_SUCCESS)) {

                        /*
                         * An error occurred while receiving the message.
                         */

                        if (status == GNI_RC_INVALID_PARAM) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_MsgqProgress      ERROR: invalid timeout value for non-blocking MSGQ\n",
                                    uts_info.nodename, rank_id);
                            INCREMENT_ABORTED;
                            goto EXIT_BARRIER;
                        }

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_MsgqProgress      ERROR status: %d transmission: %4i last received: %4i received count:  %4i\n",
                                uts_info.nodename, rank_id, status, (n + 1),
                                my_callback_info[recv_from].last_received_transmission,
                                my_callback_info[recv_from].received_count);
                    }
                } while (status == GNI_RC_NOT_DONE);
            }
        }

        if (v_option) {

            /*
             * Write out all of the output messages.
             */

            fflush(stdout);
        }
    }

    /*
     * Check to make sure all of the messages have been received.
     */

    for (i = 0; i < number_of_ranks; i++) {
        if (my_callback_info[i].received_count != transfers) {

            /*
             * Not all messages have been received, try again.
             */

            sleep(1);

            do {
                status = GNI_MsgqProgress(msgq_handle, timeout_value);
            } while (status == GNI_RC_NOT_DONE);
        }
    }

  EXIT_BARRIER:

    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

  EXIT_MSGQCONNRELEASE:

    if (use_this_rank == 1) {

        /* Release the connection resources for the remote peer. */

        for (i = 0; i < number_of_ranks; i += number_of_ranks_on_node) {
            if (is_rank_local(i, number_of_ranks_on_node, ranks_on_node) == 1) {
                continue;
            }

            /*
             * Get the remote address to get connection attributes from.
             */

            remote_address = all_nic_addresses[i];

            /*
             * Release the connection resources.
             */

            status = GNI_MsgqConnRelease(msgq_handle, remote_address);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqConnRelease   ERROR remote rank: %4i remote address: %u status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, remote_address, gni_err_str[status], status);
                INCREMENT_ABORTED;
            } else if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MsgqConnRelease   remote rank: %4i remote address: %u\n",
                        uts_info.nodename, rank_id, i, remote_address);
            }
        }
    }

  EXIT_MSGQRELEASE:

    /*
     * Wait for all the leaders to finish Share Message Queue connection release.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Release the shared messages resources.
     */

    status = GNI_MsgqRelease(msgq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MsgqRelease       ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_MsgqRelease\n",
                uts_info.nodename, rank_id);
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
                    "[%s] Rank: %4i GNI_EpUnbind          ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpUnbind          remote rank: %4i EP:  %p\n",
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
                    "[%s] Rank: %4i GNI_EpDestroy         ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy         remote rank: %4i EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }
    }

    /*
     * Destroy the completion queue.
     *     source_cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(source_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy         source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy         source\n",
                uts_info.nodename, rank_id);
    }

  EXIT_DOMAIN:

    /*
     * Clean up the communication domain handle.
     */

    status = GNI_CdmDestroy(cdm_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmDestroy        ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmDestroy\n",
                uts_info.nodename, rank_id);
    }

  EXIT_TEST:

    free(my_callback_info);
    free(ranks_on_node);

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
