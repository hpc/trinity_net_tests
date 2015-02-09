/*
 * Copyright 2011 Cray Inc.  All Rights Reserved.
 */

/*
 * FMA AMO first generation 64 bit test example - this test only uses PMI
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
#include <sys/utsname.h>
#include <errno.h>
#include "gni_pub.h"
#include "pmi.h"

#define ALL_ONES_DATA            0xfffffffffff
#define ALL_ZEROS_DATA           0x0
#define BIND_ID_MULTIPLIER       100
#define BYTE_ALIGNMENT           8
#define CDM_ID_MULTIPLIER        1000
#define DATA_MASK                0xfffffffffff
#define DEFAULT_DATA             0x5b5b5b5b5b5
#define INTEGER_ADD_VALUE        3
#define LOCAL_EVENT_ID_BASE      10000000
#define NODE_SHIFT               56
#define RANK_MULTIPLIER          4
#define REMOTE_EVENT_ID_BASE     11000000
#define TRANSFER_COUNT_SHIFT     44
#define ATOMIC_OPERAND_LENGTH    TRANSFER_COUNT_SHIFT
#define NUMBER_OF_TRANSFERS      ATOMIC_OPERAND_LENGTH*2
#define SHIFT_COUNT_MODULO       (ATOMIC_OPERAND_LENGTH/RANK_MULTIPLIER)

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
"FMA_AMO_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the receiving of a data\n"
"    transaction from a remote communication endpoint using a FMA AMO\n"
"    first generation 64 bit request.\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_PostFma() is used to with the 'AMO' type to receive a data\n"
"        transaction from a remote location.  The default 'AMO' command\n"
"        is 'AND'.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      1.  '-a' use the ADD AMO command.\n"
"      2.  '-c' use the COMPARE and SWAP AMO command.\n"
"      3.  '-C' use the CACHE variant of the AMO command.\n"
"      4.  '-e' specifies that the GNI_EpSetEventId API will be used.\n"
"      5.  '-f' use a fetching AMO command.\n"
"      6.  '-h' prints the help information for this example.\n"
"      7.  '-n' specifies the number of data transactions that will be sent.\n"
"          The default value is 440 data transactions to be sent.\n"
"      8.  '-o' use the OR AMO command.\n"
"      9.  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"          messages to be displayed.  With each additional 'v' more\n"
"          information will be displayed.\n"
"          The default value is no output or debug messages will be\n"
"          displayed.\n"
"     10.  '-x' use the XOR AMO command.\n"
"     11.  '-X' use the AX AMO command.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - fma_amo_pmi_example\n"
"      - fma_amo_pmi_example -C\n"
"      - fma_amo_pmi_example -e\n"
"      - fma_amo_pmi_example -f\n"
"      - fma_amo_pmi_example -f -C\n"
"      - fma_amo_pmi_example -f -e\n"
"      - fma_amo_pmi_example -a\n"
"      - fma_amo_pmi_example -a -C\n"
"      - fma_amo_pmi_example -a -e\n"
"      - fma_amo_pmi_example -a -f\n"
"      - fma_amo_pmi_example -a -f -C\n"
"      - fma_amo_pmi_example -a -f -e\n"
"      - fma_amo_pmi_example -c\n"
"      - fma_amo_pmi_example -c -C\n"
"      - fma_amo_pmi_example -c -e\n"
"      - fma_amo_pmi_example -o\n"
"      - fma_amo_pmi_example -o -C\n"
"      - fma_amo_pmi_example -o -e\n"
"      - fma_amo_pmi_example -o -f\n"
"      - fma_amo_pmi_example -o -f -C\n"
"      - fma_amo_pmi_example -o -f -e\n"
"      - fma_amo_pmi_example -x\n"
"      - fma_amo_pmi_example -x -C\n"
"      - fma_amo_pmi_example -x -e\n"
"      - fma_amo_pmi_example -x -f\n"
"      - fma_amo_pmi_example -x -f -C\n"
"      - fma_amo_pmi_example -x -f -e\n"
"      - fma_amo_pmi_example -X\n"
"      - fma_amo_pmi_example -X -C\n"
"      - fma_amo_pmi_example -X -e\n"
"      - fma_amo_pmi_example -X -f\n"
"      - fma_amo_pmi_example -X -f -C\n"
"      - fma_amo_pmi_example -X -f -e\n"
"\n"
    );
}

int
main(int argc, char **argv)
{
    gni_fma_cmd_type_t amo_command = GNI_FMA_ATOMIC_AND;
    unsigned int   *all_nic_addresses;
    uint32_t        bind_id;
    int             byte_alignment = BYTE_ALIGNMENT;
    gni_cdm_handle_t cdm_handle;
    uint32_t        cdm_id;
    int             cookie;
    gni_cq_handle_t cq_handle;
    gni_cq_entry_t  current_event;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    uint32_t        event_inst_id;
    gni_post_descriptor_t *event_post_desc_ptr;
    uint64_t        expected_data = -1;
    uint32_t        expected_local_event_id;
    uint32_t        expected_remote_event_id;
    int             first_spawned;
    gni_post_descriptor_t *fma_data_desc = NULL;
    int             i;
    int             j;
    unsigned int    local_address;
    uint32_t        local_event_id;
    int             modes = 0;
    mdh_addr_t      my_memory_handle;
    uint64_t        my_rank;
    uint64_t        my_receive_from;
    uint64_t        my_send_to;
    gni_nic_handle_t nic_handle;
    int             number_of_cq_entries;
    int             number_of_source_cq_entries;
    int             number_of_ranks;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    uint8_t         ptag;
    int             rc;
    int             receive_from;
    unsigned int    remote_address;
    uint32_t        remote_event_id;
    int             send_to;
    int             shift_count;
    uint64_t       *source_buffer;
    gni_cq_handle_t source_cq_handle;
    uint64_t        source_data;
    uint64_t        source_init_value = -1;
    gni_mem_handle_t source_memory_handle;
    mdh_addr_t     *source_memory_handle_array;
    gni_return_t    status = GNI_RC_SUCCESS;
    uint64_t       *target_buffer;
    uint64_t        target_data;
    uint64_t        target_init_value = -1;
    gni_mem_handle_t target_memory_handle;
    char           *text_pointer;
    uint64_t        transfer_count;
    int             transfer_length = 1;
    uint64_t        transfer_length_in_bytes;
    uint32_t        transfers = NUMBER_OF_TRANSFERS;
    int             use_cache_request = 0;
    int             use_event_id = 0;
    int             use_fetch = 0;
    uint64_t        value;
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

    local_event_id = rank_id;

    while ((opt = getopt(argc, argv, "acCefhn:ovxX")) != -1) {
        switch (opt) {
        case 'a':
            amo_command = GNI_FMA_ATOMIC_ADD;

            break;

        case 'c':
            amo_command = GNI_FMA_ATOMIC_CSWAP;

            break;

        case 'C':
            use_cache_request = 1;
            break;

        case 'e':
            use_event_id = 1;
            break;

        case 'f':
            use_fetch = 1;

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

        case 'n':

            /*
             * Set the number of messages that will be sent to the
             * shared message queue.
             */

            transfers = atoi(optarg);
            if (transfers < 1) {
                transfers = NUMBER_OF_TRANSFERS;
            } else if (transfers > NUMBER_OF_TRANSFERS) {
                transfers = NUMBER_OF_TRANSFERS;
            }

            break;

        case 'o':
            amo_command = GNI_FMA_ATOMIC_OR;

            break;

        case 'v':
            v_option++;
            break;

        case 'x':
            amo_command = GNI_FMA_ATOMIC_XOR;

            break;

        case 'X':
            amo_command = GNI_FMA_ATOMIC_AX;
            transfer_length = 2;
            byte_alignment = BYTE_ALIGNMENT * transfer_length;

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

    if (use_fetch == 1) {
        switch (amo_command) {
        case GNI_FMA_ATOMIC_ADD:
            amo_command = GNI_FMA_ATOMIC_FADD;
            break;
        case GNI_FMA_ATOMIC_AND:
            amo_command = GNI_FMA_ATOMIC_FAND;
            break;
        case GNI_FMA_ATOMIC_AX:
            amo_command = GNI_FMA_ATOMIC_FAX;
            transfer_length = 1;
            byte_alignment = BYTE_ALIGNMENT;
            break;
        case GNI_FMA_ATOMIC_OR:
            amo_command = GNI_FMA_ATOMIC_FOR;
            break;
        case GNI_FMA_ATOMIC_XOR:
            amo_command = GNI_FMA_ATOMIC_FXOR;
            break;
        default:
            break;
        }
    }

    if (use_cache_request == 1) {
        modes = GNI_CDM_MODE_CACHED_AMO_ENABLED;

        switch (amo_command) {
        case GNI_FMA_ATOMIC_ADD:
            amo_command = GNI_FMA_ATOMIC_ADD_C;
            break;
        case GNI_FMA_ATOMIC_FADD:
            amo_command = GNI_FMA_ATOMIC_FADD_C;
            break;
        case GNI_FMA_ATOMIC_AND:
            amo_command = GNI_FMA_ATOMIC_AND_C;
            break;
        case GNI_FMA_ATOMIC_FAND:
            amo_command = GNI_FMA_ATOMIC_FAND_C;
            break;
        case GNI_FMA_ATOMIC_AX:
            amo_command = GNI_FMA_ATOMIC_AX_C;
            break;
        case GNI_FMA_ATOMIC_FAX:
            amo_command = GNI_FMA_ATOMIC_FAX_C;
            break;
        case GNI_FMA_ATOMIC_OR:
            amo_command = GNI_FMA_ATOMIC_OR_C;
            break;
        case GNI_FMA_ATOMIC_FOR:
            amo_command = GNI_FMA_ATOMIC_FOR_C;
            break;
        case GNI_FMA_ATOMIC_XOR:
            amo_command = GNI_FMA_ATOMIC_XOR_C;
            break;
        case GNI_FMA_ATOMIC_FXOR:
            amo_command = GNI_FMA_ATOMIC_FXOR_C;
            break;
        default:
            break;
        }
    }

    transfer_length_in_bytes = sizeof(uint64_t) * transfer_length;

    /*
     * Determine the number of passes required for this test to be successful.
     */

    expected_passed = transfers * 5;

    cdm_id = rank_id * CDM_ID_MULTIPLIER;

    /*
     * Create a handle to the communication domain.
     *    cdm_id is the rank of this instance of the job.
     *    ptag is the protection tab for the job.
     *    cookie is a unique identifier created by the system.
     *    modes is a bit mask used to enable various flags.
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
                "[%s] Rank: %4i GNI_CdmCreate     inst_id: %i ptag: %u cookie: 0x%x modes: 0x%x\n",
                uts_info.nodename, rank_id, cdm_id, ptag, cookie, modes);
    }

    /*
     * Attach the communication domain handle to the NIC.
     *    cdm_handle is the handle pointing to the communication domain.
     *    device_id is the device identifier of the NIC that be attached to.
     *    local_address is the PE address that is returned for the
     *        communication domain that this NIC is attached to.
     *    nic_handle is the handle that is returned pointing to the NIC.
     */

    status = GNI_CdmAttach(cdm_handle, device_id, &local_address, &nic_handle);
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
     * test, only one transaction will be outstanding at a time.
     */

    number_of_cq_entries = 2;

    /*
     * Create the local completion queue.
     *     nic_handle is the NIC handle that this completion queue will be
     *          associated with.
     *     number_of_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before
     *          an interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the
     *          callback function.
     *     cq_handle is the handle that is returned pointing to this newly
     *          created completion queue.
     */

    status = GNI_CqCreate(nic_handle, number_of_cq_entries, 0, GNI_CQ_NOBLOCK,
                     NULL, NULL, &cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      local ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      local with %i entries\n",
                uts_info.nodename, rank_id, number_of_cq_entries);
    }

    /*
     * Determine the minimum number of completion queue entries, which
     * is the number of outstanding transactions at one time.  For this
     * test, two transactions per rank will be needed.
     */

    number_of_source_cq_entries = (number_of_ranks - 1) * 2;

    /*
     * Create the source completion queue.
     *     nic_handle is the NIC handle that this completion queue will be
     *          associated with.
     *     number_of_source_cq_entries is the size of the completion queue.
     *     zero is the delay count is the number of allowed events before
     *          an interrupt is generated.
     *     GNI_CQ_NOBLOCK states that the operation mode is non-blocking.
     *     NULL states that no user supplied callback function is defined.
     *     NULL states that no user supplied pointer is passed to the
     *          callback function.
     *     cq_handle is the handle that is returned pointing to this newly
     *          created completion queue.
     */

    status = GNI_CqCreate(nic_handle, number_of_source_cq_entries, 0, GNI_CQ_NOBLOCK,
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
                uts_info.nodename, rank_id, number_of_source_cq_entries);
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
         *     cq_handle is our completion queue handle.
         *     endpoint_handles_array will contain the handle that is returned
         *         for this endpoint instance.
         */

        status = GNI_EpCreate(nic_handle, cq_handle, &endpoint_handles_array[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpCreate      ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
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
         *         endpoint handler.
         *     bind_id is an unique user specified identifier for this bind.
         *         In this test bind_id refers to the instance id of the remote
         *         communication domain that we are binding to.
         */

        status = GNI_EpBind(endpoint_handles_array[i], remote_address, bind_id);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpBind        ERROR remote rank: %4i status: %s (%d)\n",
                    uts_info.nodename, rank_id, i, gni_err_str[status], status);
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
     * Allocate the buffer that will contain the data to be sent.
     */

    rc = posix_memalign((void **) &source_buffer, byte_alignment,
                        (transfer_length_in_bytes * transfers));
    assert(rc == 0);

    /*
     * Register the memory associated for the source buffer with the NIC.
     * We are sending the data from this buffer not receiving into it.
     *     nic_handle is our NIC handle.
     *     source_buffer is the memory location of the source buffer.
     *     transfer_length_in_bytes is the size of the memory allocated to the
     *         source buffer.
     *     source_cq_handle is the source completion queue handle.
     *     GNI_MEM_READWRITE is the read/write attribute for the source buffer's
     *         memory region.
     *     vmdh_index specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     source_memory_handle is the handle for this memory region.
     */

    status = GNI_MemRegister(nic_handle, (uint64_t) source_buffer,
                             (transfer_length_in_bytes * transfers),
                             source_cq_handle, GNI_MEM_READWRITE,
                             vmdh_index, &source_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   source_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_ENDPOINT;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   source_buffer  size %u address %p\n",
                uts_info.nodename, rank_id,
                (unsigned int) (transfer_length_in_bytes * transfers), source_buffer);
    }

    /*
     * Allocate the buffer that will receive the data.  This allocation is
     * creating a buffer large enough to hold all of the received data for
     * all of the transfers.
     */

    rc = posix_memalign((void **) &target_buffer, byte_alignment,
                        (transfer_length_in_bytes * transfers));
    assert(rc == 0);

    /*
     * Register the memory associated for the receive buffer with the NIC.
     * We are receiving the data into this buffer.
     *     nic_handle is our NIC handle.
     *     target_buffer is the memory location of the receive buffer.
     *     (transfer_length_in_bytes * transfers) is the size of the
     *         memory allocated to the target buffer.
     *     NULL means that no destination completion queue handle is specified.
     *         We are sending the data from this buffer not receiving.
     *     GNI_MEM_READWRITE is the read/write attribute for the target buffer's
     *         memory region.
     *     vmdh_index specifies the index within the allocated memory region,
     *         a value of -1 means that the GNI library will determine this index.
     *     target_memory_handle is the handle for this memory region.
     */

    status = GNI_MemRegister(nic_handle, (uint64_t) target_buffer,
                             (transfer_length_in_bytes * transfers), NULL,
                             GNI_MEM_READWRITE, vmdh_index,
                             &target_memory_handle);

    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   target_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_MEMORY_SOURCE;
    }

    if (v_option > 1) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemRegister   target_buffer  size: %u address: %p\n",
                uts_info.nodename, rank_id,
                (unsigned int) (transfer_length_in_bytes * transfers),
                target_buffer);
    }

    /*
     * Allocate a buffer to contain all of the remote memory handle's.
     */

    source_memory_handle_array =
        (mdh_addr_t *) malloc(number_of_ranks * sizeof(mdh_addr_t));
    assert(source_memory_handle_array);

    my_memory_handle.addr = (uint64_t) source_buffer;
    my_memory_handle.mdh = source_memory_handle;

    /*
     * Gather up all of the remote memory handle's.
     * This also acts as a barrier to get all of the ranks to sync up.
     */

    allgather(&my_memory_handle, source_memory_handle_array,
              sizeof(mdh_addr_t));

    if ((v_option > 1) && (rank_id == 0)) {
        fprintf(stdout,
                "[%s] rank address     mdh.qword1            mdn.qword2\n",
                uts_info.nodename);
        for (i = 0; i < number_of_ranks; i++) {
            fprintf(stdout, "[%s] %4i 0x%lx    0x%016lx    0x%016lx\n",
                    uts_info.nodename, i,
                    source_memory_handle_array[i].addr,
                    source_memory_handle_array[i].mdh.qword1,
                    source_memory_handle_array[i].mdh.qword2);
        }
    }

    /*
     * Determine who we are going to receive data from.
     */

    send_to = (rank_id + 1) % number_of_ranks;
    my_send_to = (uint64_t) send_to << NODE_SHIFT;
    receive_from = (number_of_ranks + rank_id - 1) % number_of_ranks;
    my_receive_from = (uint64_t) receive_from << NODE_SHIFT;
    my_rank = (uint64_t) rank_id << NODE_SHIFT;

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

    /*
     * Initialize the source and target buffers.
     *
     * The data will look like: 0xrrtttxxxxxxxxxxx
     *     where: rr is the rank value
     *            ttt is the transfer number
     *            xxxxxxxxxxx is one of the following values:
     *            00000000000 - ALL_ZEROS
     *            11111111111 - ALL_ONES
     *            5b5b5b5b5b5 - DEFAULT
     */

    switch (amo_command) {
    case GNI_FMA_ATOMIC_ADD:
    case GNI_FMA_ATOMIC_ADD_C:
    case GNI_FMA_ATOMIC_FADD:
    case GNI_FMA_ATOMIC_FADD_C:
        source_init_value = (uint64_t) (my_rank | ALL_ZEROS_DATA);
        target_init_value = (uint64_t) (my_rank | ALL_ZEROS_DATA);
        break;
    case GNI_FMA_ATOMIC_AND:
    case GNI_FMA_ATOMIC_AND_C:
    case GNI_FMA_ATOMIC_FAND:
    case GNI_FMA_ATOMIC_FAND_C:
    case GNI_FMA_ATOMIC_AX:
    case GNI_FMA_ATOMIC_AX_C:
    case GNI_FMA_ATOMIC_FAX:
    case GNI_FMA_ATOMIC_FAX_C:
        source_init_value = (uint64_t) (my_rank | ALL_ONES_DATA);
        target_init_value = (uint64_t) (my_rank | DEFAULT_DATA);
        break;
    case GNI_FMA_ATOMIC_CSWAP:
    case GNI_FMA_ATOMIC_CSWAP_C:
    case GNI_FMA_ATOMIC_OR:
    case GNI_FMA_ATOMIC_OR_C:
    case GNI_FMA_ATOMIC_FOR:
    case GNI_FMA_ATOMIC_FOR_C:
    case GNI_FMA_ATOMIC_XOR:
    case GNI_FMA_ATOMIC_XOR_C:
    case GNI_FMA_ATOMIC_FXOR:
    case GNI_FMA_ATOMIC_FXOR_C:
        source_init_value = (uint64_t) (my_rank | ALL_ZEROS_DATA);
        target_init_value = (uint64_t) (my_rank | DEFAULT_DATA);
        break;
    default:
        break;
    }

    for (i = 0; i < transfers; i++) {
        if ((amo_command == GNI_FMA_ATOMIC_CSWAP) ||
            (amo_command == GNI_FMA_ATOMIC_CSWAP_C)  ||
            (amo_command == GNI_FMA_ATOMIC_XOR)  ||
            (amo_command == GNI_FMA_ATOMIC_XOR_C)  ||
            (amo_command == GNI_FMA_ATOMIC_FXOR)  ||
            (amo_command == GNI_FMA_ATOMIC_FXOR_C)) {
            shift_count = ((rank_id * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO;
            value = (uint64_t) 1 << shift_count;
        } else {
            value = 0;
        } 

        transfer_count = (uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT;

        for (j = 0; j < transfer_length; j++) {
            source_buffer[((i * transfer_length) + j)] =
                source_init_value | transfer_count | value;
            target_buffer[((i * transfer_length) + j)] =
                target_init_value | transfer_count;

            if (v_option > 1) {
                fprintf(stdout,
                        "[%s] Rank: %4i Source data init: element: %4i"
                        " addr: 0x%lx data value: 0x%016lx\n",
                        uts_info.nodename, rank_id,
                        j + (transfer_length * i),
                        &source_buffer[((i * transfer_length) + j)],
                        source_buffer[((i * transfer_length) + j)]);

                fprintf(stdout,
                        "[%s] Rank: %4i Target data init: element: %4i"
                        " addr: 0x%lx data value: 0x%016lx\n",
                        uts_info.nodename, rank_id,
                        j + (transfer_length * i),
                        &target_buffer[((i * transfer_length) + j)],
                        target_buffer[((i * transfer_length) + j)]);
            }
        }
    }

    /*
     * wait for all the processes to initialize their data
     */
    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Allocate the fma_data_desc array.
     */

    fma_data_desc = (gni_post_descriptor_t *) calloc(transfers,
                                              sizeof(gni_post_descriptor_t));
    assert(fma_data_desc != NULL);

    for (i = 0; i < transfers; i++) {
        shift_count = ((rank_id * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO;

        /*
         * Setup the data request.
         *    type is AMO.
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
         *    amo_cmd is the atomic operation that is done on the data
         *        on the remote endpoint.  If the amo_cmd is a 'fetching'
         *        operation, the data will be sent before the atomic
         *        operation is done.
         *    first_operand is used by the atomic operation as one of
         *        the operands.
         *    second_operand is used by the atomic Compare and Swap
         *       operation as one of the operands.
         */

        fma_data_desc[i].type = GNI_POST_AMO;
        fma_data_desc[i].cq_mode = GNI_CQMODE_GLOBAL_EVENT |
            GNI_CQMODE_REMOTE_EVENT;
        fma_data_desc[i].dlvr_mode = GNI_DLVMODE_PERFORMANCE;
        fma_data_desc[i].local_addr = (uint64_t) target_buffer;
        fma_data_desc[i].local_addr += i * transfer_length_in_bytes;
        fma_data_desc[i].local_mem_hndl = target_memory_handle;
        fma_data_desc[i].remote_addr =
            source_memory_handle_array[send_to].addr;

        /*
         * If this is an add operation, only add to the first
         * element of the destination buffer.
         */

        if ((amo_command != GNI_FMA_ATOMIC_ADD) &&
                   (amo_command != GNI_FMA_ATOMIC_FADD)) {
            fma_data_desc[i].remote_addr += i * transfer_length_in_bytes;
        }

        fma_data_desc[i].remote_mem_hndl =
            source_memory_handle_array[send_to].mdh;
        fma_data_desc[i].length = transfer_length_in_bytes;
        fma_data_desc[i].amo_cmd = amo_command;

        /*
         * Set up the operand values.
         */

        switch (amo_command) {
        case GNI_FMA_ATOMIC_ADD:
        case GNI_FMA_ATOMIC_ADD_C:
            fma_data_desc[i].first_operand = (uint64_t) INTEGER_ADD_VALUE;
            break;
        case GNI_FMA_ATOMIC_FADD:
        case GNI_FMA_ATOMIC_FADD_C:
            fma_data_desc[i].first_operand = (uint64_t) 1 << TRANSFER_COUNT_SHIFT |
                (uint64_t) INTEGER_ADD_VALUE;
            break;
        case GNI_FMA_ATOMIC_AND:
        case GNI_FMA_ATOMIC_AND_C:
        case GNI_FMA_ATOMIC_FAND:
        case GNI_FMA_ATOMIC_FAND_C:
            fma_data_desc[i].first_operand = ~((uint64_t) 1 << shift_count);
            break;
        case GNI_FMA_ATOMIC_AX:
        case GNI_FMA_ATOMIC_AX_C:
        case GNI_FMA_ATOMIC_FAX:
        case GNI_FMA_ATOMIC_FAX_C:
            fma_data_desc[i].first_operand = (my_send_to |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << ((send_to * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO));
            fma_data_desc[i].second_operand = (my_rank |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << shift_count));
            break;
        case GNI_FMA_ATOMIC_CSWAP:
        case GNI_FMA_ATOMIC_CSWAP_C:
            fma_data_desc[i].first_operand = (my_send_to |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << ((send_to * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO));
            fma_data_desc[i].second_operand = (my_rank |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << shift_count));
            break;
        case GNI_FMA_ATOMIC_OR:
        case GNI_FMA_ATOMIC_OR_C:
        case GNI_FMA_ATOMIC_FOR:
        case GNI_FMA_ATOMIC_FOR_C:
            fma_data_desc[i].first_operand = ((uint64_t) 1 << shift_count);
            break;
        case GNI_FMA_ATOMIC_XOR:
        case GNI_FMA_ATOMIC_XOR_C:
        case GNI_FMA_ATOMIC_FXOR:
        case GNI_FMA_ATOMIC_FXOR_C:
            transfer_count = (uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT;
            fma_data_desc[i].first_operand = (my_rank | transfer_count |
                ((uint64_t) 1 << shift_count));
            break;
        default:
            break;
        }

        if (v_option) {
            if ((amo_command == GNI_FMA_ATOMIC_CSWAP) ||
                (amo_command == GNI_FMA_ATOMIC_CSWAP_C) ||
                (amo_command == GNI_FMA_ATOMIC_AX) ||
                (amo_command == GNI_FMA_ATOMIC_AX_C) ||
                (amo_command == GNI_FMA_ATOMIC_FAX) ||
                (amo_command == GNI_FMA_ATOMIC_FAX_C)) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma       amo request transfer: %4i amo: 0x%04x sent to:  %4i remote addr: 0x%lx length: %4i operand1: 0x%016lx operand2: 0x%016lx\n",
                        uts_info.nodename, rank_id, (i + 1),
                        fma_data_desc[i].amo_cmd, send_to,
                        fma_data_desc[i].remote_addr, fma_data_desc[i].length,
                        fma_data_desc[i].first_operand,
                        fma_data_desc[i].second_operand);
            } else {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma       amo request transfer: %4i amo: 0x%04x sent to:  %4i length: %4i remote addr: 0x%lx operand: 0x%016lx\n",
                        uts_info.nodename, rank_id, (i + 1),
                        fma_data_desc[i].amo_cmd, send_to,
                        fma_data_desc[i].length,
                        fma_data_desc[i].remote_addr, fma_data_desc[i].first_operand);
            }
        }

        /*
         * Post the request.
         */

        status = GNI_PostFma(endpoint_handles_array[send_to], &fma_data_desc[i]);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostFma       amo request ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_FAILED;
            goto BARRIER_WAIT;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostFma       amo request transfer: %4i successful\n",
                    uts_info.nodename, rank_id, (i + 1));
        }

        /*
         * Check the completion queue to verify that the data request has
         * been sent.  The local completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostFma to fail.
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
                        "[%s] Rank: %4i GNI_GetCompleted  local ERROR status: %s (%d)\n",
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
                            "[%s] Rank: %4i CQ Event data ERROR received inst_id: %u, expected inst_id: %u in event_data\n",
                            uts_info.nodename, rank_id, event_inst_id, expected_local_event_id);

                    INCREMENT_FAILED;
                } else {

                    INCREMENT_PASSED;
                }
            }
        } else {
            /*
             * An error occurred while receiving the event.
             */

            INCREMENT_FAILED;
            continue;
        }

        /*
         * Check the completion queue to verify that the data request has
         * been modified.  The source completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostFma to fail.
         */

        rc = get_cq_event(source_cq_handle, uts_info, rank_id, 0, 1, &current_event);
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
                        "[%s] Rank: %4i CQ Event source ERROR received inst_id: %u, expected inst_id: %u in event_data\n",
                        uts_info.nodename, rank_id, event_inst_id, expected_remote_event_id);

                INCREMENT_FAILED;
            } else {

                INCREMENT_PASSED;
            }
        } else {
            /*
             * An error occurred while receiving the event.
             */

            INCREMENT_FAILED;
        }
    }

    /*
     * wait for all the processes to process their requests
     */
    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    if (v_option > 2) {
        fprintf(stdout, "[%s] Rank: %4i Verify the source data.\n",
                uts_info.nodename, rank_id);
    }

    for (i = 0; i < transfers; i++) {
        transfer_count = (uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT;
        shift_count = ((receive_from * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO;

        /*
         * Verify the source data.  The source data should be the
         * result of the AMO command being applied against the
         * original data.
         */

        compare_data_failed = 0;

        switch (amo_command) {
        case GNI_FMA_ATOMIC_ADD:
        case GNI_FMA_ATOMIC_ADD_C:
            if (i == 0) {
                transfer_count = (uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT;
                expected_data =  my_rank | transfer_count |
                    ((uint64_t) (transfers * INTEGER_ADD_VALUE));
            } else {
                expected_data =  my_rank | transfer_count;
            }
            break;
        case GNI_FMA_ATOMIC_FADD:
        case GNI_FMA_ATOMIC_FADD_C:
            if (i == 0) {
                transfer_count = (uint64_t) (transfers + 1) << TRANSFER_COUNT_SHIFT;
                expected_data =  my_rank | transfer_count |
                    ((uint64_t) (transfers * INTEGER_ADD_VALUE));
            } else {
                expected_data =  my_rank | transfer_count;
            }
            break;
        case GNI_FMA_ATOMIC_AND:
        case GNI_FMA_ATOMIC_AND_C:
        case GNI_FMA_ATOMIC_FAND:
        case GNI_FMA_ATOMIC_FAND_C:
            expected_data =  my_rank | transfer_count |
                (~((uint64_t) 1 << shift_count) & DATA_MASK);
            break;
        case GNI_FMA_ATOMIC_CSWAP:
        case GNI_FMA_ATOMIC_CSWAP_C:
            expected_data = (uint64_t) (my_receive_from |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << shift_count));
            break;
        case GNI_FMA_ATOMIC_OR:
        case GNI_FMA_ATOMIC_OR_C:
        case GNI_FMA_ATOMIC_FOR:
        case GNI_FMA_ATOMIC_FOR_C:
            expected_data =  my_rank | transfer_count |
                (((uint64_t) 1 << shift_count) & DATA_MASK);
            break;
        case GNI_FMA_ATOMIC_AX:
        case GNI_FMA_ATOMIC_AX_C:
        case GNI_FMA_ATOMIC_FAX:
        case GNI_FMA_ATOMIC_FAX_C:
        case GNI_FMA_ATOMIC_XOR:
        case GNI_FMA_ATOMIC_XOR_C:
        case GNI_FMA_ATOMIC_FXOR:
        case GNI_FMA_ATOMIC_FXOR_C:
            expected_data =  (my_rank ^ my_receive_from) |
                (((uint64_t) 1 << shift_count) & DATA_MASK);
            shift_count = ((rank_id * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO;
            expected_data =  expected_data ^
                (((uint64_t) 1 << shift_count) & DATA_MASK);
            break;
        default:
            break;
        }

        for (j = 0; j < transfer_length; j = j + transfer_length) {
            source_data = source_buffer[j + (transfer_length * i)];

            if (source_data != expected_data) {

                /*
                 * The data was not what was expected.
                 */

                compare_data_failed++;
                fprintf(stdout,
                        "[%s] Rank: %4i ERROR Source data transfer: %4i element: %4i of source"
                        " addr: 0x%lx data value: 0x%016lx, should be: 0x%016lx\n",
                        uts_info.nodename, rank_id, (i + 1),
                        j + (transfer_length * i),
                        &source_buffer[j + (transfer_length * i)],
                        source_data,
                        expected_data);
            } else if (j == 0) {
                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Source data transfer: %4i element: %4i"
                            " addr: 0x%lx data value: 0x%016lx\n",
                            uts_info.nodename, rank_id, (i + 1),
                            j + (transfer_length * i),
                            &source_buffer[j + (transfer_length * i)],
                            source_data);
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
            if (v_option > 2) {
                fprintf(stdout, "[%s] Rank: %4i ERROR Source(%d) data\n",
                        uts_info.nodename, rank_id, i);
            }
        } else {

            /*
             * The data compared correctly.
             * Increment the passed test count.
             */

            INCREMENT_PASSED;
            if (v_option > 2) {
                fprintf(stdout, "[%s] Rank: %4i Source(%d) data verified successful\n",
                        uts_info.nodename, rank_id, i);
            }
        }
    }   /* end of for loop for verify source data */

    if (v_option > 2) {
        fprintf(stdout, "[%s] Rank: %4i Verify the target data.\n",
                uts_info.nodename, rank_id);
    }

    for (i = 0; i < transfers; i++) {
        transfer_count = (uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT;
        shift_count = ((send_to * RANK_MULTIPLIER) + i) % SHIFT_COUNT_MODULO;

        /*
         * Verify the target data.  If the AMO command was a fetching
         * command, then the target data will be the data from the
         * remote node prior to the AMO command being applied.  If
         * the AMO command was a non-fetching command, then the data
         * will be the same as what it was initialized as.
         */

        compare_data_failed = 0;

        switch (amo_command) {
        case GNI_FMA_ATOMIC_ADD:
        case GNI_FMA_ATOMIC_ADD_C:
            expected_data = (uint64_t) (my_rank | transfer_count);
            break;
        case GNI_FMA_ATOMIC_FADD:
        case GNI_FMA_ATOMIC_FADD_C:
            expected_data = (uint64_t) (my_send_to | transfer_count | (i * INTEGER_ADD_VALUE));
            break;
        case GNI_FMA_ATOMIC_AND:
        case GNI_FMA_ATOMIC_AND_C:
            expected_data = (uint64_t) (my_rank | transfer_count | DEFAULT_DATA);
            break;
        case GNI_FMA_ATOMIC_FAND:
        case GNI_FMA_ATOMIC_FAND_C:
            expected_data =  my_send_to | transfer_count | ALL_ONES_DATA;
            break;
        case GNI_FMA_ATOMIC_AX:
        case GNI_FMA_ATOMIC_AX_C:
            expected_data = (uint64_t) (my_rank | transfer_count | DEFAULT_DATA);
            break;
        case GNI_FMA_ATOMIC_FAX:
        case GNI_FMA_ATOMIC_FAX_C:
            expected_data = (uint64_t) (my_send_to | transfer_count | ALL_ONES_DATA);
            break;
        case GNI_FMA_ATOMIC_CSWAP:
        case GNI_FMA_ATOMIC_CSWAP_C:
            expected_data = (uint64_t) (my_send_to |
                ((uint64_t) (i + 1) << TRANSFER_COUNT_SHIFT) |
                ((uint64_t) 1 << shift_count));
            break;
        case GNI_FMA_ATOMIC_FOR:
        case GNI_FMA_ATOMIC_FOR_C:
            expected_data =  my_send_to | transfer_count | ALL_ZEROS_DATA;
            break;
        case GNI_FMA_ATOMIC_FXOR:
        case GNI_FMA_ATOMIC_FXOR_C:
            expected_data =  my_send_to | transfer_count | 
                ((uint64_t) 1 << shift_count);
            break;
        case GNI_FMA_ATOMIC_OR:
        case GNI_FMA_ATOMIC_OR_C:
        case GNI_FMA_ATOMIC_XOR:
        case GNI_FMA_ATOMIC_XOR_C:
            expected_data = (uint64_t) (my_rank | transfer_count | DEFAULT_DATA);
            break;
        default:
            break;
        }

        for (j = 0; j < transfer_length; j++) {
            target_data = target_buffer[j + (transfer_length * i)];

            if (target_data != expected_data) {

                /*
                 * The data was not what was expected.
                 */

                compare_data_failed++;
                fprintf(stdout,
                        "[%s] Rank: %4i ERROR Target data transfer: %4i element: %4i of target"
                        " addr: 0x%lx data value: 0x%016lx, should be: 0x%016lx\n",
                        uts_info.nodename, rank_id, (i + 1),
                        j + (transfer_length * i),
                        &target_buffer[j + (transfer_length * i)],
                        target_data,
                        expected_data);
            } else if (j == 0) {
                if (v_option) {
                    fprintf(stdout,
                            "[%s] Rank: %4i Target data transfer: %4i element: %4i"
                            " addr: 0x%lx data value: 0x%016lx\n",
                            uts_info.nodename, rank_id, (i + 1),
                            j + (transfer_length * i),
                            &target_buffer[j + (transfer_length * i)],
                            target_data);
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
            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i ERROR Target(%d) data\n",
                        uts_info.nodename, rank_id, i);
            }
        } else {

            /*
             * The data compared correctly.
             * Increment the passed test count.
             */

            INCREMENT_PASSED;
            if (v_option > 2) {
                fprintf(stdout, "[%s] Rank: %4i Target(%d) data verified successful\n",
                        uts_info.nodename, rank_id, i);
            }
        }
    }   /* end of for loop for verify target data */

    BARRIER_WAIT:
    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    /*
     * Free allocated memory.
     */

    free(fma_data_desc);

    /*
     * Free allocated memory.
     */

    free(source_memory_handle_array);

    /*
     * Deregister the memory associated for the receive buffer with the NIC.
     *     nic_handle is our NIC handle.
     *     target_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &target_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister target_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else {
        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister target_buffer     NIC: %p\n",
                    uts_info.nodename, rank_id, nic_handle);
        }

        /*
         * Free allocated memory.
         */

        free(target_buffer);
    }

  EXIT_MEMORY_SOURCE:

    /*
     * Deregister the memory associated for the source buffer with the NIC.
     *     nic_handle is our NIC handle.
     *     source_memory_handle is the handle for this memory region.
     */

    status = GNI_MemDeregister(nic_handle, &source_memory_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_MemDeregister source_buffer ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else {
        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister source_buffer     NIC: %p\n",
                    uts_info.nodename, rank_id, nic_handle);
        }

        /*
         * Free allocated memory.
         */

        free(source_buffer);
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
     * Destroy the completion queue.
     *     cq_handle is the handle that is being destroyed.
     */

    status = GNI_CqDestroy(cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqDestroy     local ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
    } else if (v_option > 1) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy     local\n",
                uts_info.nodename, rank_id);
    }

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
