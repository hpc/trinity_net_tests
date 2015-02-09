/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * Memory Registration test example - this test only uses PMI
 *
 * APIs exercised:
 *     GNI_MemRegister, GNI_MemRegisterSegments
 *     GNI_PostFma,     GNI_PostRdma
 *
 * NOTE: FMA Put does not support segmented source buffers. 
 */

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <sys/utsname.h>
#include <errno.h>
#include "gni_pub.h"
#include "pmi.h"

#define DATA                     0xdd00000000000000
#define FLAG                     0xff00000000000000
#define NUMBER_OF_SEGMENTS       10
#define SEGMENT_PATTERNS         11
#define SHIFT_DEST_PHYSICAL      24
#define SHIFT_DEST_LOGICAL       16
#define SHIFT_RANK_ID            48
#define SHIFT_SEND_PATTERN       8
#define SHIFT_SOURCE_PHYSICAL    40
#define SHIFT_SOURCE_LOGICAL     32
#define TRANSFER_LENGTH          512
#define SEGMENT_SIZE             (TRANSFER_LENGTH*sizeof(uint64_t))
#define TRANSFER_LENGTH_IN_BYTES (SEGMENT_SIZE*NUMBER_OF_SEGMENTS)

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
"MEMORY_REGISTRATION_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the usage of a single\n"
"    memory region or multiple non-contiguous memory regions in regards\n"
"    to their use during a FMA Get, FMA Put, RDMA Get or RDMA Put request.\n"
"    NOTE: A FMA 'PUT' request does not allow using a absolute offset for\n"
"          the source buffer. Therefore, each segment must be sent\n"
"          individually using the segments virtual address on separate\n"
"          FMA Put requests.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_MemRegister() is used to define a memory region that a NIC\n"
"        will use to transfer data.\n"
"      - GNI_MemRegisterSegments() is used to define multiple memory regions,\n"
"        that may not be contiguous in physical memory, that will be used\n"
"        by the NIC to transfer data.\n"
"      - GNI_PostFma() is used to with the 'PUT' and 'GET' types to send\n"
"        or receive a data transaction to or from the remote location.\n"
"      - GNI_PostRdma() is used to with the 'PUT' and 'GET' types to send\n"
"        or receive a data transaction to or from the remote location.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-f' specifies that the transfer should be done using FMA.\n"
"          The default value is that the transfers will be done using RDMA.\n"
"      2.  '-g' specifies that a 'GET' transfer will be done.\n"
"          The default value is that a 'PUT' transfers will be done.\n"
"      3.  '-h' prints the help information for this example.\n"
"      4.  '-p' specifies which segment pattern type will be used during\n"
"          the receiving of the data.\n"
"          The default value is that all segment patterns will be used\n"
"          during the receiving of the transfers.\n"
"      5.  '-P' specifies which segment pattern type will be used during\n"
"          the sending of the data.\n"
"          The default value is that all segment patterns will be used\n"
"          during the sending of the transfers.\n"
"      6.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - memory_registration_pmi_example\n"
"      - memory_registration_pmi_example -g\n"
"      - memory_registration_pmi_example -f\n"
"      - memory_registration_pmi_example -f -g\n"
"\n"
    );
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    gni_cdm_handle_t cdm_handle;
    uint64_t        compare_data = 0;
    int             cookie;
    gni_cq_handle_t cq_handle;
    gni_cq_entry_t  current_event;
    gni_post_descriptor_t data_desc;
    uint64_t       *destination_buffer = NULL;
    gni_cq_handle_t destination_cq_handle = NULL;
    uint64_t        destination_flag = 0;
    int             destination_failed = 0;
    int             destination_max_pattern = SEGMENT_PATTERNS;
    gni_mem_handle_t destination_memory_handle;
    gni_mem_segment_t destination_memory_segments[NUMBER_OF_SEGMENTS];
    int             destination_min_pattern = 0;
    int             destination_pattern;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    uint32_t        event_inst_id;
    gni_post_descriptor_t *event_post_desc_ptr;
    int             first_spawned;
    uint64_t        flag = 0;
    gni_post_descriptor_t flag_desc;
    gni_mem_handle_t flag_memory_handle;
    volatile uint64_t *flag_ptr;
    int             fma_put_segment = 0;
    int             fma_put_next_segment = 0;
    register uint64_t i;
    register int    j;
    uint64_t        length = sizeof(uint64_t);
    unsigned int    local_address;
    register uint64_t logical_segment;
    int             modes = 0;
    uint64_t        my_id;
    uint64_t        my_receive_from;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries;
    int             number_of_dest_cq_entries;
    int             number_of_dest_events;
    int             number_of_ranks;
    int             number_of_transfers;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    int             pattern;
    register uint64_t physical_segment;
    uint8_t         ptag;
    int             rc;
    int             receive_from;
    unsigned int    remote_address;
    mdh_addr_t      remote_memory_handle;
    mdh_addr_t     *remote_memory_handle_array = NULL;
    char           *request_type = "Put";
    uint64_t       
        segment_patterns[SEGMENT_PATTERNS][NUMBER_OF_SEGMENTS];
    int             send_to;
    uint64_t       *source_buffer;
    int             source_max_pattern = SEGMENT_PATTERNS;
    gni_mem_handle_t source_memory_handle;
    gni_mem_segment_t source_memory_segments[NUMBER_OF_SEGMENTS];
    int             source_min_pattern = 0;
    int             source_pattern;
    register uint64_t source_physical_segment;
    gni_return_t    status = GNI_RC_SUCCESS;
    uint64_t        temp;
    char           *text_pointer;
    int             use_fma = 0;
    int             use_get = 0;
    uint32_t        vmdh_index = -1;

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

    while ((opt = getopt(argc, argv, "fghp:P:v")) != -1) {
        switch (opt) {
        case 'f':

            /*
             * Use FMA to transfer data.
             */

            use_fma = 1;
            break;

        case 'g':

            /*
             *Get the data from a remote system.
             */

            use_get = 1;
            request_type = "Get";
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

        case 'p':

            /*
             * Set the destination pattern number.
             */

            destination_min_pattern = atoi(optarg);
            if ((destination_min_pattern < 0) ||
                (destination_min_pattern >= SEGMENT_PATTERNS)) {
                destination_min_pattern = 0;
                destination_max_pattern = SEGMENT_PATTERNS;
            } else {
                destination_max_pattern = destination_min_pattern + 1;
            }
            break;

        case 'P':

            /*
             * Set the source pattern number.
             */

            source_min_pattern = atoi(optarg);
            if ((source_min_pattern < 0) ||
                (source_min_pattern >= SEGMENT_PATTERNS)) {
                source_min_pattern = 0;
                source_max_pattern = SEGMENT_PATTERNS;
            } else {
                source_max_pattern = source_min_pattern + 1;
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
     * Determine the number of passes required for this test to be
     * successful.
     */

    number_of_transfers =
        (destination_max_pattern - destination_min_pattern) *
        (source_max_pattern - source_min_pattern);

    if (use_get == 1) {
        expected_passed = (number_of_transfers * 6) +
            ((source_max_pattern - source_min_pattern) * 2);
    } else {
        if (use_fma == 1) {
            expected_passed = (number_of_transfers * (NUMBER_OF_SEGMENTS + 8)) +
                ((source_max_pattern - source_min_pattern) * 2);
        } else {
            expected_passed = (number_of_transfers * 9) +
                ((source_max_pattern - source_min_pattern) * 2);
        }
    }

    /*
     * Create a handle to the communication domain.
     *    rank_id is the rank of the instance of the job.
     *    ptag is the protection tab for the job.
     *    cookie is a unique identifier created by the system.
     *    modes is a bit mask used to enable various flags.
     *    cdm_handle is the handle that is returned pointing to 
     *        the communication domain.
     */

    status = GNI_CdmCreate(rank_id, ptag, cookie, modes, &cdm_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate           ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_TEST;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate           with ptag %u cookie 0x%x\n",
                uts_info.nodename, rank_id, ptag, cookie);
    }

    /*
     * Attach the communication domain handle to the NIC.
     *    cdm_handle is the handle pointing to the communication domain.
     *    device_id is the device identifier of the NIC that be attached to.
     *    local_address is the PE address that is returned for 
     *        the communication domain that this NIC is attached to.
     *    nic_handle is the handle that is returned pointing to the NIC.
     */

    status =
        GNI_CdmAttach(cdm_handle, device_id, &local_address, &nic_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmAttach           ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmAttach           to NIC\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Determine the minimum number of completion queue entries, which
     * is the number of outstanding transactions at one time.
     * Only one transaction will be outstanding at a time because there is
     * a barrier after each transaction.  Therefore, only one completion
     * queue entry is needed.
     */

    number_of_cq_entries = 1;

    /*
     * Create the completion queue.
     *     nic_handle is the NIC handle that this completion queue will
     *          be associated with.
     *     number_of_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before 
     *          an interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the 
     *          callback function.
     *     cq_handle is the handle that is returned pointing to this 
     *          newly created completion queue.
     */

    status =
        GNI_CqCreate(nic_handle, number_of_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate            source ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate            source with %i entries\n",
                uts_info.nodename, rank_id, number_of_cq_entries);
    }

    if ((use_get == 0) && (use_fma == 1)) {
        /*
         * For a FMA Put request.
         * Determine the minimum number of completion queue entries, which
         * is the number of transactions outstanding at one time.  For this
         * test, since each segment has to be sent individually for each transfer
         * and there is a barrier after each completed transfer, the number
         * of outstanding transfers should the number of segments plus one for
         * the flag. Therefore, only 11 completion queue entries are needed.
         */

        number_of_dest_cq_entries = NUMBER_OF_SEGMENTS + 1;
    } else {
        /*
         * Determine the minimum number of completion queue entries, which
         * is the number of transactions outstanding at one time.  For this
         * test, there is a barrier after each transfers, the number
         * of outstanding transfers should be no more than 2.  Therefore,
         * only 2 completion queue entries are needed.
         */

        number_of_dest_cq_entries = 2;
    }

    /*
     * Create the destination completion queue.
     *     nic_handle is the NIC handle that this completion queue will 
     *          be associated with.
     *     number_of_dest_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events 
     *          an interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to 
     *          this callback function.
     *     destination_cq_handle is the handle that is returned pointing
     *          to this newly created completion queue.
     */

    status = GNI_CqCreate(nic_handle, number_of_dest_cq_entries, 0,
                          GNI_CQ_NOBLOCK, NULL, NULL,
                          &destination_cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate            destination ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_CQ;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate            destination with %i entries\n",
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
         * The EpBind request updates some fields in the endpoint_handle
         * this is the reason that all pairs of endpoints need to be created.
         *
         * Create the logical endpoint for each rank.
         *     nic_handle is our NIC handle.
         *     cq_handle is our completion queue handle.
         *     endpoint_handles_array will contain the handle that is
         *         returned for this endpoint instance.
         */

        status =
            GNI_EpCreate(nic_handle, cq_handle, &endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate            ERROR remote rank: %4li status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }


        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate            remote rank: %4li NIC: %p, CQ: %p, EP: %p\n",
                    uts_info.nodename, rank_id, i, nic_handle, cq_handle,
                    endpoint_handles_array[i]);
        }

        /*
         * Get the remote address to bind to.
         */

        remote_address = all_nic_addresses[i];

        /*
         * Bind the remote address to the endpoint handler.
         *     endpoint_handles_array is the endpoint handle that is being bound
         *     remote_address is the address that is being bound to this 
         *     endpoint handler.
         *     i is an unique user specified identifier for this bind.
         *         In this test i refers to the instance id of the remote
         *         communication domain that we are binding to.
         */

        status = GNI_EpBind(endpoint_handles_array[i], remote_address, i);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind              ERROR remote rank: %4li status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind              remote rank: %4li EP:  %p remote_address: %u, remote_id: %lu\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i], remote_address, i);
        }
    }

    if (use_get == 0) {
        
        /*
         * A flag is only sent on a PUT request.
         *
         * Register the memory associated for the flag with the NIC.
         *     nic_handle is our NIC handle.
         *     flag is the memory location of the flag.
         *     length is the size of the memory allocated to the flag.
         *     NULL means that no destination completion queue handle is
         *         specified. We are sending the flag from this buffer not
         *         receiving.
         *     GNI_MEM_READWRITE is the read/write attribute for the flag'
         *         memory region.
         *     vmdh_index specifies the index within the allocated memory
         *         region, a value of -1 means that the GNI library will
         *         determine this index.
         *     flag_memory_handle is the handle for this memory region.
         */

        status =
            GNI_MemRegister(nic_handle, (uint64_t) & flag, length,
                            NULL, GNI_MEM_READWRITE,
                            vmdh_index, &flag_memory_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemRegister         flag ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemRegister         flag  size: %lu address: %p\n",
                    uts_info.nodename, rank_id, length, &flag);
        }
    }

    /*
     * Allocate the buffer that will contain the data to be sent.
     */

    rc = posix_memalign((void **) &source_buffer, SEGMENT_SIZE,
                        TRANSFER_LENGTH_IN_BYTES);
    assert(rc == 0);

    /*
     * Allocate the buffer that will receive the data.  The allocation is
     * for a buffer large enough to hold the received data for one transfer.
     */

    rc = posix_memalign((void **) &destination_buffer, SEGMENT_SIZE,
                        TRANSFER_LENGTH_IN_BYTES);
    assert(rc == 0);

    /*
     * Determine who we are going to send our data to and
     * who we are going to receive data from.
     */

    send_to = (rank_id + 1) % number_of_ranks;
    receive_from = (number_of_ranks + rank_id - 1) % number_of_ranks;
    if (use_get == 0) {
        temp = receive_from & 0xff;
    } else {
        temp = send_to & 0xff;
    }
    my_receive_from = temp << SHIFT_RANK_ID;
    temp = rank_id & 0xff;
    my_id = temp << SHIFT_RANK_ID;

    for (pattern = 0; pattern < SEGMENT_PATTERNS; pattern++) {
        /*
         * The segment_patterns is a two dimensioned array.
         * The first array index is the pattern number.
         * The second array index is the logical segment.
         * The value is the physical segment.
         */

        if (pattern == 0) {
            /*
             * This pattern will use the MemRegister API.
             *
             * Segment pattern 0 will have the physical memory page
             * segments ordered as:    0, 1, 2, 3, 4, 5, 6, 7, 8, 9
             */

            segment_patterns[pattern][0] = 0;
            segment_patterns[pattern][1] = 1;
            segment_patterns[pattern][2] = 2;
            segment_patterns[pattern][3] = 3;
            segment_patterns[pattern][4] = 4;
            segment_patterns[pattern][5] = 5;
            segment_patterns[pattern][6] = 6;
            segment_patterns[pattern][7] = 7;
            segment_patterns[pattern][8] = 8;
            segment_patterns[pattern][9] = 9;
        } else if (pattern == 1) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 1 will have the physical memory page
             * segments ordered as:    0, 1, 2, 3, 4, 5, 6, 7, 8, 9
             */

            segment_patterns[pattern][0] = 0;
            segment_patterns[pattern][1] = 1;
            segment_patterns[pattern][2] = 2;
            segment_patterns[pattern][3] = 3;
            segment_patterns[pattern][4] = 4;
            segment_patterns[pattern][5] = 5;
            segment_patterns[pattern][6] = 6;
            segment_patterns[pattern][7] = 7;
            segment_patterns[pattern][8] = 8;
            segment_patterns[pattern][9] = 9;
        } else if (pattern == 2) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 2 will have the physical memory page
             * segments ordered as:    1, 3, 5, 7, 9, 0, 2, 4, 6, 8
             */

            segment_patterns[pattern][0] = 1;
            segment_patterns[pattern][1] = 3;
            segment_patterns[pattern][2] = 5;
            segment_patterns[pattern][3] = 7;
            segment_patterns[pattern][4] = 9;
            segment_patterns[pattern][5] = 0;
            segment_patterns[pattern][6] = 2;
            segment_patterns[pattern][7] = 4;
            segment_patterns[pattern][8] = 6;
            segment_patterns[pattern][9] = 8;
        } else if (pattern == 3) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 3 will have the physical memory page
             * segments ordered as:    0, 2, 4, 7, 8, 1, 3, 5, 7, 9
             */

            segment_patterns[pattern][0] = 0;
            segment_patterns[pattern][1] = 2;
            segment_patterns[pattern][2] = 4;
            segment_patterns[pattern][3] = 6;
            segment_patterns[pattern][4] = 8;
            segment_patterns[pattern][5] = 1;
            segment_patterns[pattern][6] = 3;
            segment_patterns[pattern][7] = 5;
            segment_patterns[pattern][8] = 7;
            segment_patterns[pattern][9] = 9;
        } else if (pattern == 4) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 4 will have the physical memory page
             * segments ordered as:    9, 8, 7, 6, 5, 4, 3, 2, 1, 0
             */

            segment_patterns[pattern][0] = 9;
            segment_patterns[pattern][1] = 8;
            segment_patterns[pattern][2] = 7;
            segment_patterns[pattern][3] = 6;
            segment_patterns[pattern][4] = 5;
            segment_patterns[pattern][5] = 4;
            segment_patterns[pattern][6] = 3;
            segment_patterns[pattern][7] = 2;
            segment_patterns[pattern][8] = 1;
            segment_patterns[pattern][9] = 0;
        } else if (pattern == 5) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 5 will have the physical memory page
             * segments ordered as:    8, 6, 4, 2, 0, 9, 7, 5, 3, 1
             */

            segment_patterns[pattern][0] = 8;
            segment_patterns[pattern][1] = 6;
            segment_patterns[pattern][2] = 4;
            segment_patterns[pattern][3] = 2;
            segment_patterns[pattern][4] = 0;
            segment_patterns[pattern][5] = 9;
            segment_patterns[pattern][6] = 7;
            segment_patterns[pattern][7] = 5;
            segment_patterns[pattern][8] = 3;
            segment_patterns[pattern][9] = 1;
        } else if (pattern == 6) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 6 will have the physical memory page
             * segments ordered as:    9, 7, 5, 3, 1, 8, 6, 4, 2, 0
             */

            segment_patterns[pattern][0] = 9;
            segment_patterns[pattern][1] = 7;
            segment_patterns[pattern][2] = 5;
            segment_patterns[pattern][3] = 3;
            segment_patterns[pattern][4] = 1;
            segment_patterns[pattern][5] = 8;
            segment_patterns[pattern][6] = 6;
            segment_patterns[pattern][7] = 4;
            segment_patterns[pattern][8] = 2;
            segment_patterns[pattern][9] = 0;
        } else if (pattern == 7) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 7 will have the physical memory page
             * segments ordered as:    4, 5, 3, 6, 2, 7, 1, 8, 0, 9
             */

            segment_patterns[pattern][0] = 4;
            segment_patterns[pattern][1] = 5;
            segment_patterns[pattern][2] = 3;
            segment_patterns[pattern][3] = 6;
            segment_patterns[pattern][4] = 2;
            segment_patterns[pattern][5] = 7;
            segment_patterns[pattern][6] = 1;
            segment_patterns[pattern][7] = 8;
            segment_patterns[pattern][8] = 0;
            segment_patterns[pattern][9] = 9;
        } else if (pattern == 8) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 8 will have the physical memory page
             * segments ordered as:    5, 4, 6, 3, 7, 2, 8, 1, 9, 0
             */

            segment_patterns[pattern][0] = 5;
            segment_patterns[pattern][1] = 4;
            segment_patterns[pattern][2] = 6;
            segment_patterns[pattern][3] = 3;
            segment_patterns[pattern][4] = 7;
            segment_patterns[pattern][5] = 2;
            segment_patterns[pattern][6] = 8;
            segment_patterns[pattern][7] = 1;
            segment_patterns[pattern][8] = 9;
            segment_patterns[pattern][9] = 0;
        } else if (pattern == 9) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 9 will have the physical memory page
             * segments ordered as:    0, 1, 9, 3, 4, 8, 6, 7, 5, 2
             */

            segment_patterns[pattern][0] = 0;
            segment_patterns[pattern][1] = 1;
            segment_patterns[pattern][2] = 9;
            segment_patterns[pattern][3] = 3;
            segment_patterns[pattern][4] = 4;
            segment_patterns[pattern][5] = 8;
            segment_patterns[pattern][6] = 6;
            segment_patterns[pattern][7] = 7;
            segment_patterns[pattern][8] = 5;
            segment_patterns[pattern][9] = 2;
        } else if (pattern == 10) {
            /*
             * This pattern will use the MemRegisterSegments API.
             *
             * Segment pattern 10 will have the physical memory page
             * segments ordered as:    0, 9, 2, 7, 4, 5, 6, 3, 8, 1
             */

            segment_patterns[pattern][0] = 0;
            segment_patterns[pattern][1] = 9;
            segment_patterns[pattern][2] = 2;
            segment_patterns[pattern][3] = 7;
            segment_patterns[pattern][4] = 4;
            segment_patterns[pattern][5] = 5;
            segment_patterns[pattern][6] = 6;
            segment_patterns[pattern][7] = 3;
            segment_patterns[pattern][8] = 8;
            segment_patterns[pattern][9] = 1;
        }
    }

    for (source_pattern = source_min_pattern;
         source_pattern < source_max_pattern; source_pattern++) {

        if (source_pattern == 0) {
            /*
             * Use MemRegister API 
             *
             * Register the memory associated for the send buffer with
             * the NIC. We are sending the data from this buffer not
             * receiving into it.
             *     nic_handle is our NIC handle.
             *     source_buffer is the memory location of the send buffer.
             *     TRANSFER_LENGTH_IN_BYTES is the size of the memory
             *          allocated to the send buffer.
             *     destination_cq_handle is the destination completion
             *         queue handle.
             *     GNI_MEM_READWRITE is the read/write attribute for the
             *         send buffer memory region.
             *     vmdh_index specifies the index within the allocated
             *         memory region, a value of -1 means that the GNI
             *         library will determine this index.
             *     source_memory_handle is the handle for this memory
             *         region.
             */

            status = GNI_MemRegister(nic_handle, (uint64_t) source_buffer,
                                     TRANSFER_LENGTH_IN_BYTES,
                                     destination_cq_handle,
                                     GNI_MEM_READWRITE, vmdh_index,
                                     &source_memory_handle);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemRegister         source_buffer      ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_MEMORY_FLAG;
            }

            INCREMENT_PASSED;

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemRegister         source_buffer      size: %u addr: %p\n",
                        uts_info.nodename, rank_id,
                        (unsigned int) TRANSFER_LENGTH_IN_BYTES,
                        source_buffer);
            }
        } else {
            /*
             * Use MemRegisterSegments API 
             */

            /*
             * Set up the order of the segments of the send buffer
             * for the memory register request.
             */

            for (logical_segment = 0; logical_segment < NUMBER_OF_SEGMENTS;
                 logical_segment++) {
                source_memory_segments[logical_segment].address =
                    (uint64_t) & source_buffer[TRANSFER_LENGTH *
                                               segment_patterns
                                               [source_pattern]
                                               [logical_segment]];
                source_memory_segments[logical_segment].length =
                    SEGMENT_SIZE;
            }

            /*
             * Register the memory segments associated for the send buffer
             * with the NIC. We are sending the data from this buffer not
             * receiving into it.
             *     nic_handle is our NIC handle.
             *     source_memory_segments contains the addresses and sizes
             *         of the memory segments of the send buffer.
             *     NUMBER_OF_SEGMENTS is the number of the memory segments
             *         decribed in  source_memory_segments.
             *     destination_cq_handle is the destination completion
             *         queue handle.
             *     GNI_MEM_READWRITE is the read/write attribute for the
             *         send buffer' memory region.
             *     vmdh_index specifies the index within the allocated
             *         memory region, a value of -1 means that the GNI
             *         library will determine this index.
             *     source_memory_handle is the handle for this memory
             *         region.
             */

            status =
                GNI_MemRegisterSegments(nic_handle, source_memory_segments,
                                        NUMBER_OF_SEGMENTS,
                                        destination_cq_handle,
                                        GNI_MEM_READWRITE, -1,
                                        &source_memory_handle);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemRegisterSegments source_buffer      ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_MEMORY_FLAG;
            }

            INCREMENT_PASSED;

            if (v_option > 1) {
                for (logical_segment = 0;
                     logical_segment < NUMBER_OF_SEGMENTS;
                     logical_segment++) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MemRegisterSegments source_buffer      pattern: %i physical: %li logical: %li size: %u addr: 0x%16.16lx\n",
                            uts_info.nodename, rank_id, source_pattern,
                            segment_patterns[source_pattern] [logical_segment],
                            logical_segment, (unsigned int)
                            source_memory_segments[logical_segment].length,
                            source_memory_segments[logical_segment].address);
                }
            }
        }

        for (destination_pattern = destination_min_pattern;
             destination_pattern < destination_max_pattern;
             destination_pattern++) {
            /*
             * Initialize the sending buffer to all zeros.
             */

            memset(source_buffer, 0, TRANSFER_LENGTH_IN_BYTES);

            /*
             * Initialize the receiving buffer to all zeros.
             */

            memset(destination_buffer, 0, TRANSFER_LENGTH_IN_BYTES);

            if (destination_pattern == 0) {
                /*
                 * Use MemRegister API 
                 *
                 * Register the memory associated for the receive buffer
                 * with the NIC. We are receiving the data into this
                 * buffer.
                 *     nic_handle is our NIC handle.
                 *     destination_buffer is the memory location of the
                 *         receive buffer.
                 *     (TRANSFER_LENGTH_IN_BYTES * number_of_transfers) is
                 *         the size of the memory allocated to the receive
                 *         buffer.
                 *     destination_cq_handle is the destination completion
                 *         queue handle.
                 *     GNI_MEM_READWRITE is the read/write attribute for
                 *         the receive buffer's memory region.
                 *     vmdh_index specifies the index within the allocated
                 *         memory region, a value of -1 means that the GNI
                 *         library will determine this index.
                 *     destination_memory_handle is the handle for this
                 *         memory region.
                 */

                status = GNI_MemRegister(nic_handle,
                                    (uint64_t) destination_buffer,
                                    TRANSFER_LENGTH_IN_BYTES,
                                    destination_cq_handle,
                                    GNI_MEM_READWRITE, vmdh_index,
                                    &destination_memory_handle);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MemRegister         destination_buffer ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_MEMORY_SOURCE;
                } else {

                    INCREMENT_PASSED;

                    if (v_option > 1) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_MemRegister         destination_buffer size: %u addr: %p\n",
                                uts_info.nodename, rank_id,
                                TRANSFER_LENGTH, destination_buffer);
                    }
                }
            } else {
                /*
                 * Use MemRegisterSegments API 
                 *
                 * Set up the order of the segments of the receive buffer
                 * for the memory register request.
                 */

                for (logical_segment = 0;
                     logical_segment < NUMBER_OF_SEGMENTS;
                     logical_segment++) {
                    destination_memory_segments[logical_segment].address =
                        (uint64_t) & destination_buffer[TRANSFER_LENGTH *
                                                        segment_patterns
                                                        [destination_pattern]
                                                        [logical_segment]];
                    destination_memory_segments[logical_segment].length =
                        SEGMENT_SIZE;
                }

                /*
                 * Register the memory segments associated for the receive
                 * buffer with the NIC.
                 *     nic_handle is our NIC handle.
                 *     destination_memory_segments contains the addresses
                 *         and sizes of the memory segments of the receive
                 *         buffer.
                 *     NUMBER_OF_SEGMENTS is the number of the memory
                 *         segments decribed in destination_memory_segments.
                 *     destination_cq_handle is the destination completion
                 *         queue handle.
                 *     GNI_MEM_READWRITE is the read/write attribute for
                 *         the send buffer' memory region.
                 *     vmdh_index specifies the index within the allocated
                 *         memory region, a value of -1 means that the GNI
                 *         library will determine this index.
                 *     destination_memory_handle is the handle for this
                 *         memory region.
                 */

                status = GNI_MemRegisterSegments(nic_handle,
                                            destination_memory_segments,
                                            NUMBER_OF_SEGMENTS,
                                            destination_cq_handle,
                                            GNI_MEM_READWRITE, -1,
                                            &destination_memory_handle);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MemRegisterSegments destination_buffer ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_MEMORY_FLAG;
                } else {

                    INCREMENT_PASSED;

                    if (v_option > 1) {
                        for (logical_segment = 0;
                             logical_segment < NUMBER_OF_SEGMENTS;
                             logical_segment++) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_MemRegisterSegments destination_buffer pattern: %i physical: %li logical: %li size: %u addr: 0x%16.16lx\n",
                                    uts_info.nodename, rank_id,
                                    destination_pattern,
                                    segment_patterns[destination_pattern]
                                    [logical_segment], logical_segment,
                                    (unsigned int) destination_memory_segments
                                    [logical_segment].length,
                                    destination_memory_segments
                                    [logical_segment].address);
                        }
                    }
                }
            }

            /*
             * Initialize the send data buffers. 
             */

            for (logical_segment = 0; logical_segment < NUMBER_OF_SEGMENTS;
                 logical_segment++) {
                for (j = 0; j < TRANSFER_LENGTH; j++) {
                    /*
                     * Initialize the data to be sent.
                     * The source data will look like: 0xddrrppllqqmmsstt
                     *     where: dd is the actual characters 'dd'
                     *            rr is the rank for this process
                     *            pp is the sending physical segment number
                     *            ll is the sending logical segment number
                     *            qq is the receiving physical segment number
                     *            mm is the receiving logical segment number
                     *            ss is the sending segment pattern
                     *            tt is the receiving segment pattern
                     */

                    source_buffer[(segment_patterns[source_pattern]
                                   [logical_segment] * TRANSFER_LENGTH) +
                                  j] =
                        DATA + my_id +
                        (segment_patterns[source_pattern][logical_segment]
                         << SHIFT_SOURCE_PHYSICAL) +
                        (logical_segment << SHIFT_SOURCE_LOGICAL) +
                        (segment_patterns[destination_pattern]
                         [logical_segment] << SHIFT_DEST_PHYSICAL) +
                        (logical_segment << SHIFT_DEST_LOGICAL) +
                        (source_pattern << SHIFT_SEND_PATTERN) +
                        destination_pattern;
                }

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Source                  data pattern: (%i/%i) source physical: %li logical: %li destination physical: %li logical: %li data: 0x%016lx addr: %p\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern,
                            segment_patterns[source_pattern]
                            [logical_segment], logical_segment,
                            segment_patterns[destination_pattern]
                            [logical_segment], logical_segment,
                            source_buffer[(segment_patterns[source_pattern]
                                           [logical_segment] *
                                           TRANSFER_LENGTH)],
                            &(source_buffer
                              [(segment_patterns[source_pattern]
                                [logical_segment] * TRANSFER_LENGTH)]));
                }
            }

            /*
             * Allocate a buffer to contain all of the remote memory handle's.
             */

            remote_memory_handle_array =
                (mdh_addr_t *) calloc(number_of_ranks, sizeof(mdh_addr_t));
            assert(remote_memory_handle_array);

            if (use_get == 1) {
                remote_memory_handle.addr = (uint64_t) source_buffer;
                remote_memory_handle.mdh = source_memory_handle;
            } else {
                remote_memory_handle.addr = (uint64_t) destination_buffer;
                remote_memory_handle.mdh = destination_memory_handle;
            }

            /*
             * Gather up all of the remote memory handle's.
             * This also acts as a barrier to get all of the ranks to
             * sync up.
             */

            allgather(&remote_memory_handle, remote_memory_handle_array,
                      sizeof(mdh_addr_t));

            if ((v_option > 1) && (rank_id == 0)) {
                fprintf(stdout,
                        "[%s] rank address     mdh.qword1            mdn.qword2\n",
                        uts_info.nodename);
                for (i = 0; i < number_of_ranks; i++) {
                    fprintf(stdout,
                            "[%s] %4li 0x%lx    0x%016lx    0x%016lx\n",
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
             * Setup the data request.
             */

            if (use_fma == 1) {
                if (use_get == 1) {
                    /*
                     * Setup the FMA Get data request.
                     *     type is FMA_GET.
                     */

                    data_desc.type = GNI_POST_FMA_GET;
                } else {
                    /*
                     * Setup the FMA Put data request.
                     *     type is FMA_PUT.
                     */

                    data_desc.type = GNI_POST_FMA_PUT;
                }
            } else {
                if (use_get == 1) {
                    /*
                     * Setup the RDMA Get data request.
                     *     type is RDMA_GET.
                     *     rdma_mode is 0, which means that the completion
                     *         processing is not delayed.
                     */

                    data_desc.type = GNI_POST_RDMA_GET;
                    data_desc.rdma_mode = 0;
                } else {
                    /*
                     * Setup the RDMA Put data request.
                     *     type is RDMA_PUT.
                     *     rdma_mode is 0, which means that the completion
                     *         processing is not delayed.
                     */

                    data_desc.type = GNI_POST_RDMA_PUT;
                    data_desc.rdma_mode = 0;
                }
            }

            /*
             * Continue setting up the data request.
             *    cq_mode states what type of events should be sent.
             *         GNI_CQMODE_GLOBAL_EVENT allows for the sending of
             *             an event to the local node after the receipt
             *             of the data.
             *         GNI_CQMODE_REMOTE_EVENT allows for the sending of
             *             an event to the remote node after the receipt
             *             of the data.
             *    dlvr_mode states the delivery mode.
             *    length is the amount of data to transfer.
             *    remote_mem_hndl is the memory handle of the receiving
             *        buffer.
             *    src_cq_hdl handles the source completion queue events.
             */

            data_desc.cq_mode = GNI_CQMODE_GLOBAL_EVENT |
                GNI_CQMODE_REMOTE_EVENT;
            data_desc.dlvr_mode = GNI_DLVMODE_PERFORMANCE;
            data_desc.length = TRANSFER_LENGTH_IN_BYTES;
            data_desc.remote_mem_hndl =
                remote_memory_handle_array[send_to].mdh;
            data_desc.src_cq_hndl = cq_handle;

            if (source_pattern == 0) {
                /*
                 * The source memory buffer was register with the
                 * MemRegister API.
                 */

                if (use_get == 1) {
                    /*
                     * This is a Get request, so the remote_addr contains
                     * the virtual address of the source buffer of the
                     * data on the remote node.
                     */

                    data_desc.remote_addr =
                        remote_memory_handle_array[send_to].addr;
                } else {
                    /*
                     * This is a Put request, so the local_addr contains
                     * the virtual address of the source buffer of the
                     * data on the local node.
                     */

                    data_desc.local_addr = (uint64_t) source_buffer;
                }
            } else {
                /*
                 * The source memory buffer was register with the
                 * MemRegisterSegments API.
                 */

                if (use_get == 1) {
                    /*
                     * This is a Get request, so the remote_addr
                     * is the absolute offset into the source buffer
                     * of the data on the remote node.
                     */

                    data_desc.remote_addr = (uint64_t) 0;
                } else {
                    if (use_fma == 1) {
                        /*
                         * This is a FMA Put request.
                         * Set the local_addr to the virtual address into
                         * the source buffer of the data on the local node.
                         */

                        data_desc.local_addr = (uint64_t) & source_buffer[TRANSFER_LENGTH *
                                               segment_patterns [source_pattern] [0]];
                        data_desc.length = SEGMENT_SIZE;
                    } else {
                        /*
                         * This is a RDMA Put request, so the local_addr
                         * is the absolute offset into the source buffer
                         * of the data on the local node.
                         */

                        data_desc.local_addr = (uint64_t) 0;
                    }
                }
            }

            if (destination_pattern == 0) {
                /*
                 * The destination memory buffer was register with the
                 * MemRegister API.
                 */

                if (use_get == 1) {
                    /*
                     * This is a Get request, so the local_addr contains
                     * the virtual address of the destination buffer for
                     * the data on the local node.
                     */

                    data_desc.local_addr = (uint64_t) destination_buffer;
                } else {
                    /*
                     * This is a Put request, so the remote_addr contains
                     * the virtual address of the destination buffer for
                     * the data on the remote node.
                     */

                    data_desc.remote_addr =
                        remote_memory_handle_array[send_to].addr;
                }
            } else {
                /*
                 * The destination memory buffer was register with the
                 * MemRegisterSegments API.
                 */

                if (use_get == 1) {
                    /*
                     * This is a Get request, so the local_addr
                     * is the absolute offset into the destination buffer
                     * for the data on the local node.
                     */

                    data_desc.local_addr = (uint64_t) 0;
                } else {
                    /*
                     * This is a Put request, so the remote_addr
                     * is the absolute offset into the destination buffer
                     * for the data on the remote node.
                     */

                    data_desc.remote_addr = (uint64_t) 0;
                }
            }

            /*
             * Finish setting up the data request.
             */
            if (use_get == 1) {
                /*
                 * This is a Get request.
                 *
                 *    remote_mem_hndl is the memory handle of the
                 *        receiving buffer.
                 */

                data_desc.local_mem_hndl = destination_memory_handle;
            } else {
                data_desc.local_mem_hndl = source_memory_handle;
            }

            if (use_fma == 1) {
                /*
                 * Send or receive the data via FMA.
                 */

                fma_put_segment = 0;
                fma_put_next_segment = 0;

PUT_NEXT_FMA_SEGMENT:
                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_PostFma (%s)       data pattern: (%i/%i) local addr: 0x%lx send to:   %4i remote addr: 0x%lx data length: %li\n",
                            uts_info.nodename, rank_id, request_type,
                            source_pattern, destination_pattern,
                            data_desc.local_addr, send_to,
                            data_desc.remote_addr, data_desc.length);
                }

                status = GNI_PostFma(endpoint_handles_array[send_to],
                                     &data_desc);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_PostFma (%s)       data ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, request_type,
                            gni_err_str[status], status);
                    INCREMENT_FAILED;
                    goto DEREGISTER_DESTINATION_BUFFER;
                } else {

                    if ((use_get == 0) && (fma_put_segment < (NUMBER_OF_SEGMENTS - 1))) {
                        /*
                         * Since a FMA Put request can not use an absolute
                         * offset for the source buffer, we must send each
                         * segment as a separate FMA Put request.  The length
                         * for the FMA Put request will be the length of
                         * the segment.
                         */

                        fma_put_segment++;
                        fma_put_next_segment = 1;

                        data_desc.local_addr = (uint64_t) & source_buffer[TRANSFER_LENGTH *
                                               segment_patterns [source_pattern] [fma_put_segment]];
                        data_desc.length = SEGMENT_SIZE;
                        data_desc.remote_addr =
                            data_desc.remote_addr + SEGMENT_SIZE;
                    } else {

                        INCREMENT_PASSED;
                        fma_put_next_segment = 0;

                        if (v_option > 2) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_PostFma (%s)       data pattern: (%i/%i) successful\n",
                                    uts_info.nodename, rank_id, request_type,
                                    source_pattern, destination_pattern);
                        }
                    }
                }
            } else {
                /*
                 * Send or receive the data via RDMA.
                 */

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_PostRdma (%s)      data pattern: (%i/%i) local addr: 0x%lx send to:   %4i remote addr: 0x%lx data length: %li\n",
                            uts_info.nodename, rank_id, request_type,
                            source_pattern, destination_pattern,
                            data_desc.local_addr, send_to,
                            data_desc.remote_addr, data_desc.length);
                }

                status = GNI_PostRdma(endpoint_handles_array[send_to],
                                      &data_desc);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_PostRdma (%s)      data ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, request_type,
                            gni_err_str[status], status);
                    INCREMENT_FAILED;
                    goto DEREGISTER_DESTINATION_BUFFER;
                } else {

                    INCREMENT_PASSED;

                    if (v_option > 2) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostRdma (%s)      data pattern: (%i/%i) successful\n",
                                uts_info.nodename, rank_id, request_type,
                                source_pattern, destination_pattern);
                    }
                }
            }

            if (v_option > 2) {
                if (fma_put_next_segment == 0) {
                    fprintf(stdout,
                            "[%s] Rank: %4i data transfer complete, checking CQ events\n",
                            uts_info.nodename, rank_id);
                } else {
                    fprintf(stdout,
                            "[%s] Rank: %4i checking CQ events\n",
                            uts_info.nodename, rank_id);
                }
            }

            /*
             * Check the completion queue to verify that the message
             * request has been sent.  The source completion queue needs
             * to be checked and events removed so that it does not become
             * full and cause succeeding calls to PostFma or PostRdma to
             * fail.
             */

            rc = get_cq_event(cq_handle, uts_info, rank_id, 1, 1, &current_event);
            if (rc == 0) {

                /*
                 * An event was received.
                 *
                 * Complete the event, which removes the current event's
                 * post descriptor from the event queue.
                 */

                status = GNI_GetCompleted(cq_handle, current_event,
                                     &event_post_desc_ptr);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_GetCompleted         data ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);

                    INCREMENT_FAILED;
                } else {

                    /*
                     * Validate the current event's instance id with the
                     * expected id.
                     */

                    event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                    if (event_inst_id != send_to) {

                        /*
                         * The event's inst_id was not the expected
                         * inst_id value.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i CQ Event                 data ERROR received inst_id: %d, expected inst_id: %d in event_data\n",
                                uts_info.nodename, rank_id, event_inst_id,
                                send_to);

                        INCREMENT_FAILED;
                    } else {
                        if (fma_put_next_segment == 1) {
                            goto PUT_NEXT_FMA_SEGMENT;
                        }

                        INCREMENT_PASSED;
                    }
                }
            } else {

                /*
                 * An error occurred while receiving the event.
                 */

                fprintf(stdout,
                        "[%s] Rank: %4i get_cq_event             ERROR no source event returned\n",
                        uts_info.nodename, rank_id);

                INCREMENT_FAILED;
            }

            if (use_get == 0) {
                /*
                 * This is a PUT request so send a flag data transaction
                 * to signal the completion of this data transaction.
                 */

                flag = FLAG + my_id +
                    (segment_patterns[source_pattern][0] <<
                     SHIFT_SOURCE_PHYSICAL) +
                    (segment_patterns[destination_pattern][0] <<
                     SHIFT_DEST_PHYSICAL) +
                    (source_pattern << SHIFT_SEND_PATTERN) +
                    destination_pattern;

                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Source                  flag pattern: (%i/%i) source physical: %li logical: %i destination physical: %li logical: %i data: 0x%016lx addr: %p\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern,
                            segment_patterns[source_pattern][0], 0,
                            segment_patterns[destination_pattern][0], 0,
                            flag, &flag);
                }

                /*
                 * Setup the flag request.  This is to let the remote node know
                 * that the data has been sent.
                 */

                if (use_fma == 1) {
                    /*
                     * Setup the FMA flag request.
                     *     type is FMA_PUT.
                     */

                    flag_desc.type = GNI_POST_FMA_PUT;
                } else {
                    /*
                     * Setup the RDMA flag request.
                     *     type is RDMA_PUT.
                     *     rdma_mode is 0, which means that the completion
                     *         processing is not delayed.
                     */

                    flag_desc.type = GNI_POST_RDMA_PUT;
                    flag_desc.rdma_mode = 0;
                }

                /*
                 * Continue setting up the flag request.
                 *     cq_mode states what type of events should be sent.
                 *         GNI_CQMODE_GLOBAL_EVENT allows for the sending
                 *             of an event to the local node after the
                 *             receipt of the data.
                 *         GNI_CQMODE_REMOTE_EVENT allows for the sending
                 *             of an event to the remote node after the
                 *             receipt of the data.
                 *     dlvr_mode states the delivery mode.
                 *     local_addr is the address of the sending flag.
                 *     local_mem_hndl is the memory handle of the sending
                 *         buffer.
                 *     remote_mem_hndl is the memory handle of the
                 *         receiving flag.
                 *     length is the amount of data to transfer.
                 *     src_cq_hdl handles the source completion queue events.
                 */

                flag_desc.cq_mode = GNI_CQMODE_GLOBAL_EVENT |
                    GNI_CQMODE_REMOTE_EVENT;
                flag_desc.dlvr_mode = GNI_DLVMODE_PERFORMANCE;
                flag_desc.local_addr = (uint64_t) & flag;
                flag_desc.local_mem_hndl = flag_memory_handle;
                flag_desc.remote_mem_hndl =
                    remote_memory_handle_array[send_to].mdh;
                flag_desc.length = sizeof(uint64_t);
                flag_desc.src_cq_hndl = cq_handle;

                if (destination_pattern == 0) {
                    /*
                     * The destination memory buffer was register with the
                     * MemRegister API.
                     *
                     * This is a Put request, so the remote_addr contains
                     * the virtual address of the destination buffer for
                     * the data on the remote node.
                     */

                    flag_desc.remote_addr =
                        remote_memory_handle_array[send_to].addr;
                } else {
                    /*
                     * The destination memory buffer was register with the
                     * MemRegisterSegments API.
                     *
                     * This is a Put request, so the remote_addr
                     * is the absolute offset into the destination buffer
                     * for the data on the remote node.
                     */

                    flag_desc.remote_addr = 0;
                }

                if (use_fma == 1) {
                    /*
                     * Send the flag via FMA.
                     */

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostFma (%s)       flag pattern: (%i/%i) send to:   %4i remote addr: 0x%lx flag: 0x%016lx\n",
                                uts_info.nodename, rank_id, request_type,
                                source_pattern, destination_pattern,
                                send_to, flag_desc.remote_addr, flag);
                    }

                    status = GNI_PostFma(endpoint_handles_array[send_to],
                                    &flag_desc);
                    if (status != GNI_RC_SUCCESS) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostFma (%s)       flag ERROR status: %s (%d)\n",
                                uts_info.nodename, rank_id, request_type,
                                gni_err_str[status], status);
                        INCREMENT_FAILED;
                        goto DEREGISTER_DESTINATION_BUFFER;
                    }

                    INCREMENT_PASSED;

                    if (v_option > 2) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostFma (%s)       flag pattern: (%i/%i) successful\n",
                                uts_info.nodename, rank_id, request_type,
                                source_pattern, destination_pattern);
                    }
                } else {
                    /*
                     * Send the flag via RDMA.
                     */

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostRdma (%s)      flag pattern: (%i/%i) send to:   %4i remote addr: 0x%lx flag: 0x%016lx\n",
                                uts_info.nodename, rank_id, request_type,
                                source_pattern, destination_pattern,
                                send_to, flag_desc.remote_addr, flag);
                    }

                    status = GNI_PostRdma(endpoint_handles_array[send_to],
                                     &flag_desc);
                    if (status != GNI_RC_SUCCESS) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostRdma (%s)      flag ERROR status: %s (%d)\n",
                                uts_info.nodename, rank_id, request_type,
                                gni_err_str[status], status);
                        INCREMENT_FAILED;
                        goto DEREGISTER_DESTINATION_BUFFER;
                    }

                    INCREMENT_PASSED;

                    if (v_option > 2) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_PostRdma (%s)      flag pattern: (%i/%i) successful\n",
                                uts_info.nodename, rank_id, request_type,
                                source_pattern, destination_pattern);
                    }
                }

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i flag transfer complete, checking CQ events\n",
                            uts_info.nodename, rank_id);
                }

                /*
                 * Check the completion queue to verify that the message
                 * request has been sent.  The source completion queue
                 * needs to be checked and events to be removed so that
                 * it does not become full and cause succeeding calls to
                 * PostFma to fail.
                 */

                rc = get_cq_event(cq_handle, uts_info, rank_id, 1, 1, &current_event);
                if (rc == 0) {

                    /*
                     * An event was received.
                     *
                     * Complete the event, which removes the current
                     * event's post descriptor from the event queue.
                     */

                    status = GNI_GetCompleted(cq_handle, current_event,
                                         &event_post_desc_ptr);
                    if (status != GNI_RC_SUCCESS) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_GetCompleted         flag ERROR status: %s (%d)\n",
                                uts_info.nodename, rank_id, gni_err_str[status], status);

                        INCREMENT_FAILED;
                    } else {

                        /*
                         * Validate the current event's instance id
                         * with the expected id.
                         */

                        event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                        if (event_inst_id != send_to) {

                            /*
                             * The event's inst_id was not the expected
                             * inst_id value.
                             */

                            fprintf(stdout,
                                    "[%s] Rank: %4i CQ Event                 flag ERROR received inst_id: %d, expected inst_id: %d in event_data\n",
                                    uts_info.nodename, rank_id, event_inst_id,
                                    send_to);

                            INCREMENT_FAILED;
                        } else {

                            INCREMENT_PASSED;
                        }
                    }
                } else {

                    /*
                     * An error occurred while receiving the event.
                     */

                    fprintf(stdout,
                            "[%s] Rank: %4i get_cq_event             ERROR no destination event returned\n",
                            uts_info.nodename, rank_id);

                    INCREMENT_FAILED;
                    goto DEREGISTER_DESTINATION_BUFFER;
                }

                /*
                 * Wait for the arrival of the flag from the remote node.
                 */

                destination_flag = FLAG + my_receive_from +
                    (segment_patterns[source_pattern][0] <<
                     SHIFT_SOURCE_PHYSICAL) +
                    (segment_patterns[destination_pattern][0] <<
                     SHIFT_DEST_PHYSICAL) +
                    (source_pattern << SHIFT_SEND_PATTERN) +
                    destination_pattern;
                flag_ptr = (uint64_t *) & destination_buffer[TRANSFER_LENGTH *
                                                      segment_patterns
                                                      [destination_pattern]
                                                      [0]];
                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Wait                    flag pattern: (%i/%i) recv from: %4i remote addr: %p flag: 0x%016lx\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern, receive_from, flag_ptr,
                            destination_flag);
                }

                while (*flag_ptr != destination_flag) {
                    sleep(1);
                };

                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Arrived                 flag pattern: (%i/%i) recv from: %4i remote addr: %p flag: 0x%016lx\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern, receive_from, flag_ptr,
                            *flag_ptr);

                    fprintf(stdout,
                            "[%s] Rank: %4i Wait for destination completion queue events recv from: %4i\n",
                            uts_info.nodename, rank_id, receive_from);
                }

                if (use_fma == 1) {
                    /*
                     * For a FMA PUT request, there will be NUMBER_OF_SEGMENTS
                     * completion queue events sent to the destination queue.
                     * One for each of the segments and one for the flag request.
                     */

                    number_of_dest_events = NUMBER_OF_SEGMENTS + 1;
                } else {
                    /*
                     * For a PUT request, there will be 2 completion
                     * queue events sent to the destination queue.
                     * One for the data request and one for the flag request.
                     */

                    number_of_dest_events = 2;
                }
            } else {
                /*
                 * For a GET request, there will be 1 completion
                 * queue event sent to the destination queue.
                 * One for the data request.
                 */

                number_of_dest_events = 1;
            }

            /*
             * Check the completion queue to verify that the data and flag
             * has been received.  The destination completion queue needs
             * to be checked and events to be removed so that it does not
             * become full and cause succeeding events to be lost.
             */

            for (j = 0; j < number_of_dest_events; j++) {
                rc = get_cq_event(destination_cq_handle, uts_info, rank_id,
                                 0, 1, &current_event);
                if (rc == 0) {

                    /*
                     * An event was received.
                     *
                     * Validate the current event's instance id with the
                     * expected id.
                     */

                    event_inst_id = GNI_CQ_GET_INST_ID(current_event);
                    if (event_inst_id != receive_from) {

                        /*
                         * The event's inst_id was not the expected
                         * inst_id value.
                         */

                        fprintf(stdout,
                                "[%s] Rank: %4i CQ Event destination ERROR received inst_id: %d, expected inst_id: %d in event_data\n",
                                uts_info.nodename, rank_id, event_inst_id,
                                receive_from);

                        destination_failed++;
                        INCREMENT_FAILED;
                    } else {

                        INCREMENT_PASSED;
                    }
                } else {

                    /*
                     * An error occurred while receiving the event.
                     */

                    if (use_get == 0) {
                        fprintf(stdout,
                                "[%s] Rank: %4i CQ Event ERROR destination queue did not receive"
                                " flag or data event\n",
                                uts_info.nodename, rank_id);
                    } else {
                        fprintf(stdout,
                                "[%s] Rank: %4i CQ Event ERROR destination queue did not receive"
                                " data event\n",
                                uts_info.nodename, rank_id);
                    }

                    destination_failed++;
                    INCREMENT_FAILED;
                    goto DEREGISTER_DESTINATION_BUFFER;
                }
            }

            /*
             * Verify the received data.
             */


            for (physical_segment = 0;
                 physical_segment < NUMBER_OF_SEGMENTS;
                 physical_segment++) {
                compare_data_failed = 0;
                logical_segment = 0;

                /*
                 * For the destination pattern determine what the logical
                 * segment is for the given physical segment.
                 */

                for (j = 0; j < NUMBER_OF_SEGMENTS; j++) {
                    if (segment_patterns[destination_pattern][j] ==
                        physical_segment) {
                        logical_segment = j;
                        break;
                    }
                }

                /*
                 * From the destination pattern logical segment determine
                 * the sources physical segment.
                 */

                source_physical_segment =
                    segment_patterns[source_pattern][logical_segment];

                /*
                 * Initialize the data to be sent.
                 * The received data should look like: 0xddrrpplllqqmmsstt
                 *     where: dd is the actual characters 'dd'
                 *            rr is the rank for this process
                 *            pp is the sending physical segment number
                 *            ll is the sending logical segment number
                 *            qq is the receiving physical segment number
                 *            mm is the receiving logical segment number
                 *            ss is the sending segment pattern
                 *            tt is the receiving segment pattern
                 */

                for (j = 0; j < TRANSFER_LENGTH; j++) {

                    if ((use_get == 0)
                        && (segment_patterns[destination_pattern][0] ==
                            physical_segment) && (j == 0)) {
                        /*
                         * This is a PUT request, the first element in the
                         * first logical segment of the buffer is the flag.
                         * Set up what the expected flag should look like.
                         */

                        compare_data =
                            FLAG + my_receive_from +
                            (source_physical_segment <<
                             SHIFT_SOURCE_PHYSICAL) +
                            (physical_segment << SHIFT_DEST_PHYSICAL) +
                            (source_pattern << SHIFT_SEND_PATTERN) +
                            destination_pattern;
                    } else {
                        /*
                         * For a PUT or GET request, all other elements
                         * will contain data.
                         * Set up what the expected data should look like.
                         */

                        compare_data =
                            DATA + my_receive_from +
                            (source_physical_segment <<
                             SHIFT_SOURCE_PHYSICAL) +
                            (logical_segment << SHIFT_SOURCE_LOGICAL) +
                            (physical_segment << SHIFT_DEST_PHYSICAL) +
                            (logical_segment << SHIFT_DEST_LOGICAL) +
                            (source_pattern << SHIFT_SEND_PATTERN) +
                            destination_pattern;
                    }

                    if (destination_buffer
                        [j + (TRANSFER_LENGTH * physical_segment)] !=
                        compare_data) {

                        /*
                         * The data was not what was expected.
                         */

                        compare_data_failed++;
                        fprintf(stdout,
                                "[%s] Rank: %4i Destination             data ERROR in pattern: (%i/%i) addr: %p element: %5li of received"
                                " data value 0x%016lx, should be 0x%016lx\n",
                                uts_info.nodename, rank_id, source_pattern,
                                destination_pattern,
                                (uint64_t *) & destination_buffer[j +
                                             (SEGMENT_SIZE * physical_segment)],
                                j + (SEGMENT_SIZE * physical_segment),
                                destination_buffer[j +
                                          (TRANSFER_LENGTH * physical_segment)],
                                compare_data);
                    } else if (v_option) {
                        if (segment_patterns[destination_pattern][0] ==
                            physical_segment) {
                            if ((use_get == 0) && (j == 0)) {
                                /*
                                 * For a PUT request, the first word of the
                                 * first logical segment of the data buffer
                                 * contains the flag value.
                                 *
                                 * Print the flag value.
                                 */

                                fprintf(stdout,
                                        "[%s] Rank: %4i Destination             flag pattern: (%i/%i) recv from: %4i addr: %p flag: 0x%016lx\n",
                                        uts_info.nodename, rank_id,
                                        source_pattern,
                                        destination_pattern, receive_from,
                                        &(destination_buffer [j +
                                          (TRANSFER_LENGTH * physical_segment)]),
                                        destination_buffer[j +
                                          (TRANSFER_LENGTH * physical_segment)]);
                            } else if (j == 1) {
                                /*
                                 * For all requests, the second word of the
                                 * first logical segment of the data buffer
                                 * contains the data value.
                                 *
                                 * Print only the first data value.
                                 */

                                fprintf(stdout,
                                        "[%s] Rank: %4i Destination             data pattern: (%i/%i) recv from: %4i addr: %p data: 0x%016lx\n",
                                        uts_info.nodename, rank_id,
                                        source_pattern,
                                        destination_pattern, receive_from,
                                        &(destination_buffer [j +
                                          (TRANSFER_LENGTH * physical_segment)]),
                                        destination_buffer[j +
                                          (TRANSFER_LENGTH * physical_segment)]);
                            }
                        } else if (j == 0) {
                            /*
                             * For all requests, the first word of all of
                             * the  other logical segment of the data
                             * buffer contains the data value.
                             *
                             * Print only the first data value.
                             */

                            fprintf(stdout,
                                    "[%s] Rank: %4i Destination             data pattern: (%i/%i) recv from: %4i addr: %p data: 0x%016lx\n",
                                    uts_info.nodename, rank_id,
                                    source_pattern, destination_pattern,
                                    receive_from,
                                    &(destination_buffer [j + (TRANSFER_LENGTH
                                       * physical_segment)]),
                                    destination_buffer[j +
                                       (TRANSFER_LENGTH * physical_segment)]);
                        }
                    }

                    /*
                     * Only print the first 5 data compare errors.
                     */

                    if (compare_data_failed > 4) {
                        break;
                    }
                }
            }

            if (compare_data_failed != 0) {

                /*
                 * The data did not compare correctly.
                 * Increment the failed test count.
                 */

                INCREMENT_FAILED;
                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Compare                 ERROR pattern(%i/%i)\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern);
                }
            } else {

                /*
                 * The data compared correctly.
                 * Increment the passed test count.
                 */

                INCREMENT_PASSED;
                if (v_option > 2) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Compare                 SUCCESSFUL pattern(%i/%i)\n",
                            uts_info.nodename, rank_id, source_pattern,
                            destination_pattern);
                }
            }

DEREGISTER_DESTINATION_BUFFER:

            /*
             * Wait for each transfer to complete before we send the
             * next one.
             */

            rc = PMI_Barrier();
            assert(rc == PMI_SUCCESS);

            /*
             * Deregister the memory associated for the receive buffer
             * with the NIC.
             *     nic_handle is our NIC handle.
             *     destination_memory_handle is the handle for this memory
             *         region.
             */

            status = GNI_MemDeregister(nic_handle, &destination_memory_handle);
            if (status != GNI_RC_SUCCESS) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemDeregister       destination_buffer ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
                INCREMENT_ABORTED;
            } else {
                INCREMENT_PASSED;

                if (v_option > 1) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_MemDeregister       destination_buffer NIC: %p\n",
                            uts_info.nodename, rank_id, nic_handle);
                }
            }

            /*
             * Free allocated memory.
             */

            free(remote_memory_handle_array);
        }

EXIT_MEMORY_SOURCE:

        /*
         * Deregister the memory associated for the send buffer with
         * the NIC.
         *     nic_handle is our NIC handle.
         *     source_memory_handle is the handle for this memory region.
         */

        status = GNI_MemDeregister(nic_handle, &source_memory_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister       source_buffer     ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
        } else {
            INCREMENT_PASSED;

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemDeregister       source_buffer      NIC: %p\n",
                        uts_info.nodename, rank_id, nic_handle);
            }
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

    free(destination_buffer);

    /*
     * Free allocated memory.
     */

    free(source_buffer);

EXIT_MEMORY_FLAG:

    if (use_get == 0) {
        /*
         * Deregister the memory associated for the flag with the NIC.
         *     nic_handle is our NIC handle.
         *     flag_memory_handle is the handle for this memory region.
         */

        status = GNI_MemDeregister(nic_handle, &flag_memory_handle);
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
         *     endpoint_handles_array is the endpoint handle that is
         *     being unbound.
         */

        status = GNI_EpUnbind(endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpUnbind      ERROR remote rank: %4li status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpUnbind      remote rank: %4li EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }

        /*
         * You must do an EpDestroy for each endpoint pair.
         *
         * Destroy the logical endpoint for each rank.
         *     endpoint_handles_array is the endpoint handle that is 
         *         destroyed.
         */

        status = GNI_EpDestroy(endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy     ERROR remote rank: %4li status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
            INCREMENT_ABORTED;
            continue;
        }

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpDestroy     remote rank: %4li EP:  %p\n",
                    uts_info.nodename, rank_id, i,
                    endpoint_handles_array[i]);
        }
    }

    /*
     * Free allocated memory.
     */

    free(endpoint_handles_array);

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
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy     destination\n",
                uts_info.nodename, rank_id);
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
