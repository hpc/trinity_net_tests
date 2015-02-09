/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * Test basic datagram functionality - this test only uses PMI
 * GNI_EpPostData, GNI_EpPostDataWId,
 * GNI_PostDataProbe, GNI_PostDataProbeById
 * GNI_EpPostDataWait, GNI_EpPostDataWaitById
 * GNI_EpPostDataTest and GNI_EpPostDataTestById
 *
 * There are 3 basic ways to send datagrams.
 *
 * The first way is to send a datagram with both end points being bound
 * without a datagram identifier.  This is the default setting for this
 * example.
 *
 * The second way is to send a datagram with both end points being bound
 * with an datagram identifier.  To execute this test case, you need
 * to add the '-i' parameter on the command line.
 *
 * The third way is to send a datagram with one of the end points being
 * bound and the other end point being unbound.  This case requires that
 * the datagram be sent with a datagram identifier.  To execute this test
 * case, you need to add the '-B' parameter on the command line.
 *
 * For each of the above 3 test cases, you can invoke the calling of:
 *   - the 'Probe' APIs by adding the '-p' parameter on the command line
 *   - the 'Wait' APIs by adding the '-w' parameter on the command line
 *   - both the 'Probe' and 'Wait' APIs by adding '-p -w' on the command
 *     line.
 */

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#include <errno.h>
#include "gni_pub.h"
#include "pmi.h"

#define DATAGRAM_ONE_ID_VALUE  0xfedcba9876543210
#define DATAGRAM_NODE_MASK     0xfffff
#define DATAGRAM_FROM_SHIFT    44
#define DATAGRAM_TO_SHIFT      24
#define DATAGRAM_TRANSFER_MASK 0xffffff
#define DEFAULT_TIMEOUT_VALUE  20
#define MAXIMUM_RETRY_COUNT    10000
#define NUMBER_OF_TRANSFERS    10

typedef struct {
    uint32_t        sender;
    uint32_t        receiver;
    uint32_t        transfer;
    char            buffer[24];
} my_datagram_t;

int             rank_id;
struct utsname  uts_info;
int             v_option = 0;

#include "utility_functions.h"

void print_help(void)
{
    fprintf(stdout,
"\n"
"DATAGRAM_POST_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the sending and receiving\n"
"    of a datagram between two bound communication endpoints or a local\n"
"    unbound communication endpoint with a bound remote communication\n"
"    endpoint.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_EpPostData() is used to post a datagram to a remote bounded\n"
"        communication endpoint.\n"
"      - GNI_EpPostDataWId() is used to post a datagram with the user\n"
"        supplied identifier to a remote communication bounded endpoint.\n"
"        The local communication endpoint can be an unbounded communication\n"
"        endpoint.\n"
"      - GNI_EpPostDataTest() is used to get the state of a previously\n"
"        posted datagram.\n"
"      - GNI_EpPostDataTestById() is used to get the state of a previously\n"
"        posted datagram with the specified user supplied identifier.\n"
"      - GNI_EpPostDataWait() is used to block until the previously posted\n"
"        datagram completes or times out.\n"
"      - GNI_EpPostDataWaitById() is used to block until the previously\n"
"        posted datagram with the specified user supplied identifier\n"
"        completes or times out.\n"
"      - GNI_PostDataProbe() is used to get the remote user supplied\n"
"        identifier and remote address of the first datagram found in a\n"
"        completed, terminated or timeout state.\n"
"      - GNI_PostDataProbeById() is used to get the remote user supplied\n"
"        identifier and remote address of the first datagram found with the\n"
"        specified user supplied identifier and in a completed, terminated\n"
"        or timeout state.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-B' specifies that one of the communication endpoints will be\n"
"          unbounded. This implies the '-i' option.\n"
"          The default value is both communication endpoints will be bound.\n"
"      2.  '-h' prints the help information for this example.\n"
"      3.  '-I' specifies that for the 'ID' APIs will use the same.\n"
"          datagram for all transfers and to all peers.\n"
"      4.  '-i' specifies that the sent of receive datagram will contain\n"
"          a user supplied identifier.\n"
"          The default value is that no user supplied identifier will be\n"
"          supplied with the datagram.\n"
"      5.  '-n' specifies the number of datagrams that will be sent.\n"
"          The default value is 10 datagrams to transfer.\n"
"      6.  '-p' specifies that the 'Probe' versions of the APIs will be used.\n"
"            -  For the 'PostDataProbe' API, it allows the receiver to query\n"
"               the first datagram in the queue for its remote address and\n"
"               remote user supplied identifier.\n"
"            -  For the 'PostDataProbeById' API, it allows the receiver to \n"
"               process the first datagram with the specified user supplied\n"
"               identifier.\n"
"            -  For the 'PostDataProbeWaitById' API, it allows the receiver\n"
"               to wait until the first datagram with the specified user\n"
"               supplied identifier arrives.\n"
"          The default value is that the 'Probe' APIs will not be used.\n"
"      7.  '-s' specifies that rank 0 will be created as an unbound\n"
"          endpoint acting as a server.  All of the other ranks will be\n"
"          bound endpoints and acting as clients.\n"
"      8.  '-t' specifies the timeout value that will be used with the\n"
"          'Wait' APIs.\n"
"          The default value is 20 milliseconds.\n"
"      9.  '-T' specifies the timeout value will be set to non-blocking for\n"
"          the 'Wait' APIs.\n"
"          The default value is 20 milliseconds.\n"
"     10.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"     11.  '-w' specifies that the 'Wait' versions of the APIs will be used.\n"
"            -  For the 'PostDataWait' API, it allows the receiver to block\n"
"               until the previous posted datagram completes or times out.\n"
"            -  For the 'PostDataWaitById' API, it allows the receiver to\n"
"               block until the datagram with the specified user supplied\n"
"               identifier completes or times out.\n"
"          The default value is that the 'Wait' APIs will not be used.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - datagram_post_pmi_example\n"
"      - datagram_post_pmi_example -i\n"
"      - datagram_post_pmi_example -i -p\n"
"      - datagram_post_pmi_example -i -w\n"
"      - datagram_post_pmi_example -i -p -w\n"
"      - datagram_post_pmi_example -i -I\n"
"      - datagram_post_pmi_example -B\n"
"      - datagram_post_pmi_example -B -p\n"
"      - datagram_post_pmi_example -B -w\n"
"      - datagram_post_pmi_example -B -p -w\n"
"      - datagram_post_pmi_example -B -s\n"
"      - datagram_post_pmi_example -B -s -I\n"
"\n"
    );
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    gni_cdm_handle_t cdm_handle;
    int             compared_failed;
    int             cookie;
    gni_cq_entry_t  current_event;
    uint64_t        datagram_id;
    int             device_id = 0;
    int             endpoint_index = 0;
    gni_ep_handle_t *endpoint_handles_array;
    char            expected_buffer[24];
    int             first_spawned;
    register int    i;
    unsigned int    local_address;
    int             modes = 0;
    register int    n;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries;
    int             number_of_ranks;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    gni_post_state_t post_state;
    uint8_t         ptag;
    int             rc;
    uint32_t        received_remote_address;
    uint32_t        received_remote_id;
    unsigned int    remote_address;
    my_datagram_t  *remote_datagram_array;
    uint32_t        responding_remote_address;
    uint32_t        responding_remote_id;
    int             retry_count = 0;
    my_datagram_t   send_datagram;
    gni_cq_handle_t source_cq_handle;
    gni_return_t    status;
    char           *text_pointer;
    uint32_t        timeout_value = DEFAULT_TIMEOUT_VALUE;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;
    int             use_bound = 1;
    int             use_id = 0;
    int             use_only_1_id = 0;
    int             use_probe = 0;
    int             use_wait = 0;
    int             use_0_as_server = 0;

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

    while ((opt = getopt(argc, argv, "BhiIn:pst:Tvw")) != -1) {
        switch (opt) {
        case 'B':
            use_bound = 0;

            /*
             * If using unbound endpoints, it is required that the
             * posting of datagrams use the "Id" APIs.  That is,
             * send and received datagrams with a user supplied
             * datagram identifier.
             */

            use_id = 1;
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

        case 'i':

            /*
             * Send and receive datagram transfers using a user
             * supplied datagram identifier.
             */

            use_id = 1;
            break;

        case 'I':

            /*
             * For the WId APIs only use one value for the id.
             */

            use_only_1_id = 1;
            break;

        case 'n':

            /*
             * Set the number of datagrams that will be sent.
             */

            transfers = atoi(optarg);
            if (transfers < 1) {
                transfers = NUMBER_OF_TRANSFERS;
            }

            break;

        case 'p':

            /*
             * Use the "Probe" APIs to determine which datagram to receive.
             */

            use_probe = 1;
            break;

        case 's':

            /*
             * Use rank 0 as a servre and all other ranks as clients.
             */

            use_0_as_server = 1;
            break;

        case 't':

            /*
             * Set the timeout value for the "Wait" APIs.
             */

            if (timeout_value != -1) {
                timeout_value = atoi(optarg);
            }
            break;

        case 'T':

            /*
             * Set the timeout value to non-blocking for the "Wait" APIs.
             */

            timeout_value = -1;
            break;

        case 'v':
            v_option++;
            break;

        case 'w':

            /*
             * Use the "Wait" APIs to wait for a datagram to arrive.
             */

            use_wait = 1;
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
     * If using rank 0 as a server, then bind all of the other ranks.
     */

    if ((use_0_as_server == 1) && (use_bound == 0) && (rank_id != 0)) {
        use_bound = 1;
        use_id = 0;
    }

    /*
     * Determine the number of passes required for this test
     * to be successful.
     */

    if (use_bound == 0) {
        if (rank_id == 0) {
            if (use_probe == 1) {
                expected_passed = transfers * (number_of_ranks - 1) * 4;
            } else {
                expected_passed = transfers * (number_of_ranks - 1) * 3;
            }
        } else {
            if (use_probe == 1) {
                expected_passed = transfers * 4;
            } else {
                expected_passed = transfers * 3;
            }
        }
    } else if (use_0_as_server == 1) {
        if (use_probe == 1) {
            expected_passed = transfers * 4;
        } else {
            expected_passed = transfers * 3;
        }
    } else {
        if (use_probe == 1) {
            expected_passed = transfers * (number_of_ranks - 1) * 4;
        } else {
            expected_passed = transfers * (number_of_ranks - 1) * 3;
        }
    }

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
                "[%s] Rank: %4i GNI_CdmCreate          ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_TEST;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate          with ptag %u cookie 0x%x\n",
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

    status = GNI_CdmAttach(cdm_handle, device_id, &local_address,
                           &nic_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmAttach          ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmAttach          to NIC\n",
                uts_info.nodename, rank_id);
    }

    /*
     * wait for all of the ranks to attach to the nic.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Set the number of completion queue entries to 1.
     */

    number_of_cq_entries = 1;

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

    status = GNI_CqCreate(nic_handle, number_of_cq_entries, 0,
                          GNI_CQ_NOBLOCK, NULL, NULL, &source_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate           source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate           source with %i entries\n",
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

        if ((use_bound == 1) ||
            ((use_bound == 0) && (rank_id == 0) && (i != 0)) ||
            ((use_bound == 0) && (rank_id != 0) && (i == 0))) {
            /*
             * You must do an EpCreate for each endpoint pair.
             * That is for each remote node that you will want to
             * communicate with.  The EpBind request updates some fields
             * in the endpoint_handle so this is the reason that all pairs
             * of endpoints need to be created.
             *
             * For the bound test case, create an endpoint for each rank.
             *
             * For the unbound test case, only create endpoints for rank 0
             * to all of the other ranks and for all the other ranks to
             * rank 0.
             *
             * Create the logical endpoint for each rank.
             *     nic_handle is our NIC handle.
             *     source_cq_handle is our completion queue handle.
             *     endpoint_handles_array will contain the handle that is
             *         returned for this endpoint instance.
             */

            status = GNI_EpCreate(nic_handle, source_cq_handle,
                                  &endpoint_handles_array[i]);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpCreate           ERROR remote rank: %4i status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_ENDPOINT;
            }

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpCreate           remote rank: %4i NIC: %p CQ: %p EP: %p\n",
                        uts_info.nodename, rank_id, i, nic_handle,
                        source_cq_handle, endpoint_handles_array[i]);
            }
        }

        /*
         * Get the remote address to bind to.
         */

        remote_address = all_nic_addresses[i];

        if ((use_bound == 1) ||
            ((use_bound == 0) && (rank_id != 0) && (i == 0))) {

            /*
             * For the bound test case, all ranks will bind to all other
             * ranks except itself.
             *
             * For the unbound test case, all ranks, other than rank 0,
             * will only bind to rank 0 and rank 0 will not bind to any
             * rank.
             *
             * Bind the remote address to the endpoint handler.
             *     endpoint_handles_array is the endpoint handle that is
             *         being bound
             *     remote_address is the address that is being bound to
             *         this endpoint handler
             *     i is an unique user specified identifier for this bind.
             *         In this test i refers to the instance id of the
             *         remote communication domain that we are binding to.
             */

            status = GNI_EpBind(endpoint_handles_array[i],
                                remote_address, i);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpBind             ERROR remote rank: %4i status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_ENDPOINT;
            }


            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpBind             remote rank: %4i EP:  %p remote_address: %u remote_id: %u\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i], remote_address, i);
            }
        }
    }

    /*
     * Allocate memory to hold the datagrams from the remote nodes.
     */

    remote_datagram_array =
        (my_datagram_t *) malloc(number_of_ranks * sizeof(my_datagram_t));
    assert(remote_datagram_array != NULL);

    /*
     * Set the sender field in the datagram to this rank.
     */

    send_datagram.sender = rank_id;

    /*
     * Send the datagram to the remote nodes
     * and receive a datagram from the remote nodes.
     */

    for (n = 0; n < transfers; n++) {

        /*
         * Set the transfer field in the datagram to the current
         * transfer that is being sent.
         */

        send_datagram.transfer = n + 1;

        /*
         * Initialize the message to be sent.
         * The datagram buffer will look like: 'datagramlllllltttttt'
         *     where: datagram is the actual value
         *            llllll is the rank for this process
         *            tttttt is the transfer number
         */

        sprintf(send_datagram.buffer, "datagram%6.6x%6.6x", rank_id,
                (n + 1));

        /*
         * Send a datagram to each of the remote nodes.
         */

        for (i = 0; i < number_of_ranks; i++) {

            if (i == rank_id) {

                /*
                 * Do NOT send a datagram to ourselves. 
                 */

                continue;
            }

            if ((use_bound == 0) && (rank_id != 0) && (i != 0)) {

                /*
                 * For the unbound test case, node 0 will send datagrams
                 * to all of the other nodes.  All of the other nodes will
                 * only send datagrams to node 0.
                 */

                continue;
            }

            if ((use_0_as_server == 1) && (rank_id != 0) && (i != 0)) {

                /*
                 * For the server test case, node 0 will send datagrams
                 * to all of the other nodes.  All of the other nodes will
                 * only send datagrams to node 0.
                 */

                continue;
            }

            /*
             * For the bound test case, all nodes will post
             * datagrams to all of the other nodes.
             *
             * For the unbound test case, node 0 posts a wilcard datagram
             * to all of the other nodes and all of the other nodes will
             * post a datagram to node 0.
             */

            if (v_option > 2) {
                fprintf(stdout, "[%s] Rank: %4i sending datagrams to rank: %4i transfer: %4i\n",
                        uts_info.nodename, rank_id, i, n);
            }

            /*
             * Set the receiver field in the datagram to the rank
             * that this datagram is being sent to.
             */

            send_datagram.receiver = i;

            if (use_id == 1) {

                /*
                 * Use the "Id" version of the APIs to send the
                 * datagram.
                 */

                if (use_only_1_id == 1) {
                    datagram_id = DATAGRAM_ONE_ID_VALUE;
                } else if ((use_probe == 0) && (use_bound == 0) && (rank_id == 0)) {

                    /*
                     * For the unbound test case that is not using a probe
                     * API, node 0 posts a wilcard datagram.  That is with
                     * a generic datagram id.
                     */

                    datagram_id =
                        ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_FROM_SHIFT) +
                        ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_TO_SHIFT) +
                        (send_datagram.transfer & DATAGRAM_TRANSFER_MASK);
                } else {

                    /*
                     * For the bound test case, set up a unique datagram
                     * id for each datagram to be posted.
                     *
                     * For the unbound test case using a probe API, set up
                     * a unique datagram id for each datagram to be posted.
                     */

                    datagram_id =
                        ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_FROM_SHIFT) +
                        ((uint64_t) (i & DATAGRAM_NODE_MASK) << DATAGRAM_TO_SHIFT) +
                        (send_datagram.transfer & DATAGRAM_TRANSFER_MASK);
                }

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWId      datagram send to:   %4i transfer: %4i datagram: '%20.20s' datagram id: 0x%16.16lx\n",
                            uts_info.nodename, rank_id, i, (n + 1),
                            send_datagram.buffer, datagram_id);
                }

                /*
                 * Send the datagram using the "Id" version of
                 * the API to the remote node.
                 *     endpoint_handles_array is the endpoint
                 *         handle for the remote node.
                 *     send_datagram is the datagram to be sent
                 *         to the remote node.
                 *     sizeof(my_datagram_t) is the length of
                 *         the outgoing datagram.
                 *     send_datagram_reply is a pointer to the
                 *         datagram buffer of the received
                 *         datagram reply.
                 *     sizeof(my_datagram_t) is the length of
                 *         the incoming datagram reply.
                 *     datagram_id is the user supplied datagram
                 *         identifier.
                 */

                status = GNI_EpPostDataWId(
                                         endpoint_handles_array[i],
                                         &send_datagram,
                                         sizeof(my_datagram_t),
                                         &remote_datagram_array[i],
                                         sizeof(my_datagram_t),
                                         datagram_id);

                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWId      ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_DONE;
                }

                INCREMENT_PASSED;

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostDataWId      datagram successful\n",
                            uts_info.nodename, rank_id);
                }
            } else {

                /*
                 * Use the non-"Id" version of the APIs to send
                 * the datagram.
                 */

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData         datagram send to:   %4i transfer: %4i datagram: '%20.20s'\n",
                            uts_info.nodename, rank_id, i, (n + 1),
                            send_datagram.buffer);
                    }

                /*
                 * Send the datagram to the remote node.
                 *     endpoint_handles_array is the endpoint handle
                 *         for the remote node.
                 *     send_datagram is the datagram to be sent to the
                 *         remote node.
                 *     sizeof(my_datagram_t) is the length of the
                 *         outgoing datagram.
                 *     send_datagram_reply is a pointer to the
                 *         datagram buffer of the received datagram
                 *         reply.
                 *     sizeof(my_datagram_t) is the length of the
                 *         incoming datagram reply.
                 */

                status = GNI_EpPostData(endpoint_handles_array[i],
                                        &send_datagram,
                                        sizeof(my_datagram_t),
                                        &remote_datagram_array[i],
                                        sizeof(my_datagram_t));

                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData         ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_DONE;
                }

                INCREMENT_PASSED;

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpPostData         datagram successful\n",
                            uts_info.nodename, rank_id);
                }
            }
        }

        /*
         * Receive a datagram from each of the remote nodes.
         */

        for (i = 0; i < number_of_ranks; i++) {
            if (i == rank_id) {

                /*
                 * Do NOT receive a datagram from ourselves. 
                 */

                continue;
            }

            if ((use_bound == 0) && (rank_id != 0) && (i != 0)) {

                /*
                 * For the unbound test case, all of the nodes, except
                 * node 0, will not receive datagrams from any node
                 * except node 0.
                 */

                continue;
            }

            if ((use_0_as_server == 1) && (rank_id != 0) && (i != 0)) {

                /*
                 * For the unbound test case, all of the nodes, except
                 * node 0, will not receive datagrams from any node
                 * except node 0.
                 */

                continue;
            }

            /*
             * For the bound test case, all nodes will receive datagrams
             * from all of the other nodes.
             *
             * For the unbound test case, node 0 will only receive
             * datagrams from other nodes and all of the other nodes
             * will only receive a datagram from node 0.
             */

            if (v_option > 2) {
                fprintf(stdout, "[%s] Rank: %4i waiting to receive datagrams from rank: %4i\n",
                        uts_info.nodename, rank_id, i);
            }

            post_state = GNI_POST_PENDING;

            if (use_id == 1) {

                /*
                 * Use the "Id" version of the APIs to send the
                 * datagram.
                 */

                retry_count = 0;

                if (use_probe == 1) {

                    /*
                     * Use the "Probe" version of the APIs to inquire
                     * about the datagrams.
                     *
                     * Probe the state of the nic and return the datagram
                     * identifier from the first available datagram.
                     *     nic_handle is our NIC handle.
                     *     datagram_id is the identifier of the
                     *         first available datagram.
                     */

                    do {
                        status = GNI_PostDataProbeById(nic_handle,
                                                       &datagram_id);

                        if (status != GNI_RC_SUCCESS) {

                            /*
                             * No datagrams were available or another
                             * error occurred.
                             */

                            retry_count++;
                            if (retry_count >= MAXIMUM_RETRY_COUNT) {

                                /*
                                 * No datagrams were available or another
                                 * error occurred.
                                 */

                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_PostDataProbeById  ERROR "
                                        "datagram id: 0x%16.16lx rank: %4i transfer: %4i retry count: %i status: %s (%d)\n",
                                        uts_info.nodename, rank_id,
                                        datagram_id, i, (n + 1),
                                        retry_count, gni_err_str[status], status);
                                INCREMENT_FAILED;
                                break;
                            }

                            if ((retry_count % (MAXIMUM_RETRY_COUNT / 10)) == 0 ) {
                                if (v_option > 1) {
                                    fprintf(stdout,
                                            "[%s] Rank: %4i GNI_PostDataProbeById  SLEEP "
                                            "datagram id: 0x%16.16lx rank: %4i transfer: %4i retry count: %i\n",
                                            uts_info.nodename, rank_id,
                                            datagram_id, i, (n + 1),
                                            retry_count);
                                }

                                /*
                                 * Sometimes it takes a little longer for
                                 * the datagram to arrive.
                                 */

                                sleep(1);
                            } else {
                                sched_yield();
                            }
                        }
                    } while (status == GNI_RC_NO_MATCH);

                    if (status != GNI_RC_SUCCESS) {
                        break;
                    } else {

                        /*
                         * The datagram was available from the remote node.
                         */

                        INCREMENT_PASSED;
                    }

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostDataProbeById  datagram id: 0x%16.16lx transfer:  %4i\n",
                                uts_info.nodename, rank_id,
                                datagram_id, (n + 1));
                    }

                    /*
                     * Determine which endpoint the datagram is
                     * ready to be received from.
                     */

                    endpoint_index = (datagram_id >> DATAGRAM_TO_SHIFT) & DATAGRAM_NODE_MASK;
                } else {

                    /*
                     * Set up the datagram identifier for the datagram
                     * that we want to receive.
                     */

                    if (use_only_1_id == 1) {
                        datagram_id = DATAGRAM_ONE_ID_VALUE;
                    } else if ((use_probe == 0) && (use_bound == 0) && (rank_id == 0)) {

                        /*
                         * For the unbound test case that is not using a
                         * probe API, node 0 posts a wilcard datagram.
                         * That is with a generic datagram id.
                         */

                        datagram_id =
                            ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_FROM_SHIFT) +
                            ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_TO_SHIFT) +
                            (send_datagram.transfer & DATAGRAM_TRANSFER_MASK);
                    } else {

                        /*
                         * For the bound test case, set up a unique
                         * datagram id for each datagram to be posted.
                         *
                         * For the unbound test case using a probe API,
                         * set up a unique datagram id for each datagram
                         * to be posted.
                         */

                        datagram_id =
                            ((uint64_t) (rank_id & DATAGRAM_NODE_MASK) << DATAGRAM_FROM_SHIFT) +
                            ((uint64_t) (i & DATAGRAM_NODE_MASK) << DATAGRAM_TO_SHIFT) +
                            (send_datagram.transfer & DATAGRAM_TRANSFER_MASK);
                    }

                    /*
                     * Set up the endpoint that we want to receive the
                     * datagram from.
                     */

                    endpoint_index = i;
                }

                if (use_wait == 1) {

                    /*
                     * Use the "Wait" version of the APIs to test the
                     * state of the datagram.
                     *     endpoint_handles_array is the endpoint
                     *         handle for the remote node to acquire
                     *         the datagram.
                     *     datagram_id is the identifier of the
                     *         datagram to receive.
                     *     timeout_value is the time out value used
                     *         for waiting for the datagram to arrive.
                     *     post_state is the state of the datagram
                     *         transaction.
                     *     responding_remote_address is the address of
                     *         the remoted node of the datagram for the
                     *         specified datagram identifier that has
                     *         been successfully transferred,
                     *         i.e. COMPLETED.
                     *     responding_remote_id is the identifier of
                     *         the remoted node for which the datagram
                     *         with the specified datagram identifier
                     *         has been successfully transferred,
                     *         i.e. COMPLETED.
                     */

                    status = GNI_EpPostDataWaitById(
                                                   endpoint_handles_array
                                                   [endpoint_index],
                                                   datagram_id,
                                                   timeout_value,
                                                   &post_state,
                                                   &responding_remote_address,
                                                   (unsigned int *)
                                                   &responding_remote_id);

                    if (status == GNI_RC_TIMEOUT) {

                        /*
                         * The datagram transfer with the specified
                         * datagram identifier was not successful.
                         * It was not available within the specified
                         * time limit, i.e. timed out.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWaitById ERROR "
                                "datagram id: 0x%16.16lx returned with TIMEOUT\n",
                                uts_info.nodename, rank_id,
                                datagram_id);
                        INCREMENT_FAILED;
                        break;
                    }

                    if (status != GNI_RC_SUCCESS) {

                        /*
                         * The datagram transfer with the specified
                         * datagram identifier is not available due to
                         * an error.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWaitById ERROR "
                                "datagram id: 0x%16.16lx status: %s (%d)\n",
                                uts_info.nodename, rank_id,
                                datagram_id, gni_err_str[status], status);
                        INCREMENT_FAILED;
                        break;
                    }

                    INCREMENT_PASSED;

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWaitById datagram id: 0x%16.16lx recv from: %4i transfer: %4i state:  %4i remote address: 0x%8.8x remote id: %i\n",
                                uts_info.nodename, rank_id,
                                datagram_id, endpoint_index, (n + 1),
                                post_state, responding_remote_address,
                                responding_remote_id);
                    }
                } else {
                    do {

                        /*
                         * Use the "Id" version of the APIs to test the
                         * state of the datagram.
                         *     endpoint_handles_array is the endpoint
                         *         handle for the remote node to
                         *         acquire the datagram.
                         *     datagram_id is the identifier of the
                         *         datagram to receive.
                         *     post_state is the state of the datagram
                         *         transaction.
                         *     responding_remote_address is the address
                         *         of the the remoted node of the
                         *         datagram for the specified datagram
                         *         identifier that has been
                         *         successfully transferred,
                         *         i.e. COMPLETED.
                         *     responding_remote_id is the identifier
                         *         of the remoted node for which the
                         *         datagram with the specified datagram
                         *         identifier has been successfully
                         *         transferred, i.e. COMPLETED.
                         */

                        status = GNI_EpPostDataTestById(
                                             endpoint_handles_array[endpoint_index],
                                             datagram_id, &post_state,
                                             &responding_remote_address,
                                             (unsigned int *)
                                             &responding_remote_id);

                        if (status != GNI_RC_SUCCESS) {

                            /*
                             * The datagram transfer with the specified
                             * datagram identifier is not available due
                             * to an error.
                             */

                            retry_count++;
                            if (retry_count >= MAXIMUM_RETRY_COUNT) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_EpPostDataTestById ERROR "
                                        "datagram id: 0x%16.16lx rank: %4i transfer: %4i endpoint index: %d retry count: %i status: %s (%d)\n",
                                        uts_info.nodename, rank_id,
                                        datagram_id, i, (n + 1), endpoint_index,
                                        retry_count, gni_err_str[status], status);
                                INCREMENT_FAILED;
                                break;
                            }

                            if ((retry_count % (MAXIMUM_RETRY_COUNT / 10)) == 0 ) {
                                if (v_option > 1) {
                                    fprintf(stdout,
                                            "[%s] Rank: %4i GNI_EpPostDataTestById SLEEP "
                                            "datagram id: 0x%16.16lx rank: %4i transfer: %4i endpoint index: %d retry count: %i\n",
                                            uts_info.nodename, rank_id,
                                            datagram_id, i, (n + 1), endpoint_index,
                                            retry_count);
                                }

                                /*
                                 * Sometimes it takes a little longer for
                                 * the datagram to arrive.
                                 */

                                sleep(1);
                            } else {
                                sched_yield();
                            }
                        }
                    } while (post_state != GNI_POST_COMPLETED);

                    if ((status == GNI_RC_SUCCESS) && (post_state == GNI_POST_COMPLETED)) {
                        INCREMENT_PASSED;

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_EpPostDataTestById datagram id: 0x%16.16lx resp from: %4i transfer: %4i state:  %4i remote address: 0x%8.8x remote id: %i\n",
                                    uts_info.nodename, rank_id,
                                    datagram_id, endpoint_index,
                                    (n + 1), post_state,
                                    responding_remote_address,
                                    responding_remote_id);
                        }
                    }
                }
            } else {

                if (use_probe == 1) {

                    /*
                     * Use the "Probe" version of the APIs to inquire
                     * about the datagrams.
                     *
                     * Probe the state of the nic until the datagram
                     * is received from a remote node.
                     *     nic_handle is our NIC handle.
                     *     responding_remote_address is the address of
                     *         the remoted node of the datagram for the
                     *         specified datagram identifier that has
                     *         been successfully transferred,
                     *         i.e. COMPLETED.
                     *     responding_remote_id is the identifier of
                     *         the remoted node for which the datagram
                     *         with the specified datagram identifier
                     *         has been successfully transferred,
                     *         i.e. COMPLETED.
                     */

                    do {
                        status = GNI_PostDataProbe(nic_handle,
                                                   &received_remote_address,
                                                   &received_remote_id);

                        if (status != GNI_RC_SUCCESS) {

                            /*
                             * The datagram transfer with the specified
                             * datagram identifier is not available due
                             * to an error.
                             */

                            retry_count++;
                            if (retry_count >= MAXIMUM_RETRY_COUNT) {

                                /*
                                 * The datagram transfer for the specified
                                 * datagram identifier is not available due
                                 * to an error.
                                 */

                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_PostDataProbe      ERROR "
                                        "rank: %4i transfer: %4i retry count: %i status: %s (%d)\n",
                                        uts_info.nodename, rank_id,
                                        i, (n + 1), retry_count, gni_err_str[status], status);
                                INCREMENT_FAILED;
                                break;
                            }

                            if ((retry_count % (MAXIMUM_RETRY_COUNT / 10)) == 0 ) {
                                if (v_option > 1) {
                                    fprintf(stdout,
                                            "[%s] Rank: %4i GNI_PostDataProbe      SLEEP "
                                            "rank: %4i transfer: %4i retry count: %i\n",
                                            uts_info.nodename, rank_id,
                                            i, (n + 1), retry_count);
                                }

                                /*
                                 * Sometimes it takes a little longer for
                                 * the datagram to arrive.
                                 */

                                sleep(1);
                            } else {
                                sched_yield();
                            }
                        }
                    } while (status == GNI_RC_NO_MATCH);

                    if (status != GNI_RC_SUCCESS) {

                        /*
                         * The transfer to this remote node was not
                         * successful.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostDataProbe      ERROR "
                                "status: %s (%d)\n", uts_info.nodename,
                                rank_id, gni_err_str[status], status);
                        INCREMENT_FAILED;
                        break;
                    } else {
                        INCREMENT_PASSED;
                    }

                    /*
                     * Determine which endpoint the datagram is
                     * ready to be received from.
                     */

                    endpoint_index = received_remote_id;

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostDataProbe      datagram recv from: %4i transfer: %4i remote address: 0x%8.8x remote id: %i\n",
                                uts_info.nodename, rank_id,
                                endpoint_index, (n + 1),
                                received_remote_address,
                                received_remote_id);
                    }
                } else {

                    /*
                     * Set up the endpoint that we want to receive the
                     * datagram from.
                     */

                    endpoint_index = i;
                }

                if (use_wait == 1) {

                    /*
                     * Use the "Wait" version of the APIs to test the
                     * state of the datagram.
                     *     endpoint_handles_array is the endpoint
                     *         handle for the remote node to acquire
                     *         the datagram.
                     *     timeout_value is the time out value used for
                     *         waiting for the datagram to arrive.
                     *     post_state is the state of the datagram
                     *         transaction.
                     *     responding_remote_address is the address of
                     *         the remoted node of the datagram that
                     *         has been successfully transferred,
                     *         i.e. COMPLETED.
                     *     responding_remote_id is the identifier of
                     *         the remoted node for which the datagram
                     *         has been successfully transferred,
                     *         i.e. COMPLETED.
                     */

                    status = GNI_EpPostDataWait(endpoint_handles_array
                                               [endpoint_index],
                                               timeout_value, &post_state,
                                               &responding_remote_address,
                                               (unsigned int *)
                                               &responding_remote_id);

                    if (status == GNI_RC_TIMEOUT) {

                        /*
                         * The datagram transfer was not successful.
                         * It was not available within the specified
                         * time limit, i.e. timed out.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWait     ERROR "
                                "rank: %4i returned with TIMEOUT\n",
                                uts_info.nodename, rank_id,
                                endpoint_index);
                        INCREMENT_FAILED;
                        break;
                    }

                    if (status != GNI_RC_SUCCESS) {

                        /*
                         * The datagram transfer is not available due
                         * to an error.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWait     ERROR "
                                "rank: %4i status: %s (%d)\n",
                                uts_info.nodename, rank_id,
                                endpoint_index, gni_err_str[status], status);
                        INCREMENT_FAILED;
                        break;
                    }

                    INCREMENT_PASSED;

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_EpPostDataWait     datagram recv from: %4i transfer: %4i state:  %4i remote address: 0x%8.8x remote id: %i\n",
                                uts_info.nodename, rank_id,
                                endpoint_index, (n + 1), post_state,
                                responding_remote_address,
                                responding_remote_id);
                    }
                } else {
                    do {

                        /*
                         * Use the "Test" version of the APIs to test
                         * the state of the datagram.
                         *     endpoint_handles_array is the endpoint
                         *         handle for the remote node to
                         *         acquire the datagram.
                         *     post_state is the state of the datagram
                         *         transaction.
                         *     responding_remote_address is the address
                         *         of the the remoted node of the
                         *         datagram for the specified datagram
                         *         identifier that has been
                         *         successfully transferred,
                         *         i.e. COMPLETED.
                         *     responding_remote_id is the identifier
                         *         of the remoted node for which the
                         *         datagram with the specified datagram
                         *         identifier has been successfully
                         *         transferred, i.e. COMPLETED.
                         */

                        status = GNI_EpPostDataTest(endpoint_handles_array
                                                   [endpoint_index],
                                                   &post_state,
                                                   &responding_remote_address,
                                                   (unsigned int *)
                                                   &responding_remote_id);

                        if (status != GNI_RC_SUCCESS) {

                            /*
                             * The datagram transfer is not available
                             * due to an error.
                             */

                            retry_count++;
                            if (retry_count >= MAXIMUM_RETRY_COUNT) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_EpPostDataTest     ERROR "
                                        "rank: %4i transfer: %4i endpoint index: %d retry count: %i status: %s (%d)\n",
                                        uts_info.nodename, rank_id,
                                        i, (n + 1), endpoint_index,
                                        retry_count, gni_err_str[status], status);
                                INCREMENT_FAILED;
                                break;
                            }

                            if ((retry_count % (MAXIMUM_RETRY_COUNT / 10)) == 0 ) {
                                if (v_option > 1) {
                                    fprintf(stdout,
                                            "[%s] Rank: %4i GNI_EpPostDataTest     SLEEP "
                                            "rank: %4i transfer: %4i endpoint index: %d retry count: %i\n",
                                            uts_info.nodename, rank_id,
                                            i, (n + 1), endpoint_index,
                                            retry_count);
                                }

                                /*
                                 * Sometimes it takes a little longer for
                                 * the datagram to arrive.
                                 */

                                sleep(1);
                            } else {
                                sched_yield();
                            }
                        }
                    } while (post_state != GNI_POST_COMPLETED);

                    if ((status == GNI_RC_SUCCESS) && (post_state == GNI_POST_COMPLETED)) {
                        INCREMENT_PASSED;

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_EpPostDataTest   datagram resp from: %4i transfer: %4i state:  %4i remote address: 0x%8.8x remote id: %i\n",
                                    uts_info.nodename, rank_id,
                                    endpoint_index, (n + 1),
                                    post_state,
                                    responding_remote_address,
                                    responding_remote_id);
                        }
                    }
                }
            }

            compared_failed = 0;

            if (post_state == GNI_POST_COMPLETED) {

                /*
                 * The datagram was successfully received.
                 */

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Received               datagram recv from: %4i transfer: %4i sender: %4i receiver: %4i transfer: %4i datagram: '%20.20s'\n",
                            uts_info.nodename, rank_id, endpoint_index,
                            (n + 1),
                            remote_datagram_array[endpoint_index].sender,
                            remote_datagram_array[endpoint_index].receiver,
                            remote_datagram_array[endpoint_index].transfer,
                            remote_datagram_array[endpoint_index].buffer);
                }

                /*
                 * Do some basic sanity checks on the received datagram.
                 *
                 * Verify that that received datagram was received on
                 * the correct endpoint.
                 */

                if (use_bound == 1) {
                    if (use_only_1_id == 0) {

                        /*
                         * If the same datagram id is used for all peers
                         * and datagrams, then do not verify the recieved
                         * endpoint id because it could have been sent
                         * by any of the peers.
                         *
                         * For the unbound test case, do not verify the
                         * responding remote id against the expected endpoint
                         * because the datagram that is received is the
                         * first datagram that is in the queue which might not
                         * have been for this node.
                         */

                        if (responding_remote_id != endpoint_index) {
                            compared_failed++;
                            INCREMENT_FAILED;

                            fprintf(stdout,
                                    "[%s] Rank: %4i ERROR invalid remote id received: %i expected: %i\n",
                                    uts_info.nodename, rank_id,
                                    responding_remote_id, endpoint_index);
                        }
                    }
                }

                /*
                 * Verify that that received datagram contained the
                 * correct sending node.
                 */

                if (remote_datagram_array[endpoint_index].sender !=
                        responding_remote_id) {
                    compared_failed++;
                    INCREMENT_FAILED;

                    fprintf(stdout,
                            "[%s] Rank: %4i ERROR invalid datagram sender received: %i expected: %i endpoint: %i\n",
                            uts_info.nodename, rank_id,
                            remote_datagram_array[endpoint_index].sender,
                            responding_remote_id, endpoint_index);
                }

                if (use_bound == 1) {

                    if (use_0_as_server == 0) {
                        /*
                         * Only verify this value if rank 0 is not acting as a server.
                         *
                         * For the bound test case, verify that that received
                         * datagram contained the correct receiving node.
                         *
                         * For the unbound test case, do not verify the
                         * datagram's receiver fied against the expected rank
                         * because the datagram that is received is the
                         * first datagram that is in the queue which might not
                         * have been for this node.
                         */

                        if (remote_datagram_array[endpoint_index].receiver !=
                                rank_id) {
                            compared_failed++;
                            INCREMENT_FAILED;

                            fprintf(stdout,
                                    "[%s] Rank: %4i ERROR invalid datagram receiver received: %i expected: %i endpoint: %i\n",
                                    uts_info.nodename, rank_id,
                                    remote_datagram_array[endpoint_index].receiver,
                                    rank_id, endpoint_index);
                        }
                    }
                }

                sprintf(expected_buffer, "datagram%6.6x%6.6x",
                        responding_remote_id, (n + 1));

                /*
                 * Verify that that received datagram contained the
                 * correct contents in its buffer.
                 */

                if (strcmp(remote_datagram_array[endpoint_index].buffer,
                        expected_buffer)) {
                    compared_failed++;
                    INCREMENT_FAILED;

                    fprintf(stdout,
                            "[%s] Rank: %4i ERROR invalid datagram buffer received: '%20.20s' expected: '%20.20s' endpoint: %i\n",
                            uts_info.nodename, rank_id,
                            remote_datagram_array[endpoint_index].buffer,
                            expected_buffer, responding_remote_id);
                }

                if (compared_failed == 0) {
                    INCREMENT_PASSED;
                }
            }
        }

        /*
         * Wait for all the processes to finish transmitting and receiving
         * their datagrams before the next datagram is posted.
         */

        rc = PMI_Barrier();
        assert(rc == PMI_SUCCESS);
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

    if (0 != 0) {
        /*
         * This code is here to prevent a compilation error
         * for an unused function.
         */
        rc = get_cq_event(source_cq_handle, uts_info, rank_id, 1, 1, &current_event);
    }

    if (v_option) {

        /*
         * Write out all of the output messages.
         */

        fflush(stdout);
    }

  EXIT_DONE:

    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

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

        if ((use_bound == 1) ||
            ((use_bound == 0) && (rank_id != 0) && (i == 0))) {

            /*
             * For the bound test case, all ranks will unbind from all
             * other ranks.
             *
             * For the unbound test case, all ranks, other than rank 0,
             * will only unbind from rank 0.
             *
             * Unbind the remote address from the endpoint handler.
             *     endpoint_handles_array is the endpoint handle that
             *         is being unbound
             */

            status = GNI_EpUnbind(endpoint_handles_array[i]);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpUnbind           ERROR remote rank: %4i status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, gni_err_str[status], status);
                INCREMENT_ABORTED;
                continue;
            }

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpUnbind           remote rank: %4i EP:  %p\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i]);
            }
        }

        /*
         * For the bound test case, destroy the endpoint for each rank.
         *
         * For the unbound test case, only destroy endpoints for rank 0
         * to all of the other ranks and for all the other ranks to
         * rank 0.
         *
         * You must do an EpDestroy for each endpoint pair.
         *
         * Destroy the logical endpoint for each rank.
         *     endpoint_handles_array is the endpoint handle that is being
         *         destroyed.
         */

        status = GNI_EpDestroy(endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy          ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy          remote rank: %4i EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }
    }

    /*
     * Free allocated memory.
     */

    free(endpoint_handles_array);

    /*
     * Destroy the completion queue.
     *     source_cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(source_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy          source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy          source\n",
                uts_info.nodename, rank_id);
    }

  EXIT_DOMAIN:

    /*
     * Clean up the communication domain handle.
     */

    status = GNI_CdmDestroy(cdm_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmDestroy         ERROR status: %s (%d)\n",
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
