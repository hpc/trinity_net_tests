/*
 * Copyright 2013 Cray Inc.  All Rights Reserved.
 */

/*
 * FMA CE scatter/gather test example - this test only uses PMI
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
#include <pthread.h>
#include "gni_pub.h"
#include "pmi.h"

#define ALL_ONES_DATA            0xffffffffffffffff
#define BIND_ID_MULTIPLIER       100
#define CE_RED_ID                1234
#define CDM_ID_MULTIPLIER        1000
#define DOUBLE_MULTIPLIER        10000000000.0
#define FLOAT_MULTIPLIER         1000.0
#define INT_MULTIPLIER           1000
#define LONG_MULTIPLIER          10000000000
#define LONG_SHIFT_COUNT         32
#define MAXIMUM_LONG_INT         0x7fffffffffffffff
#define MAXIMUM_SHORT_INT        0x7fffffff
#define SHORT_SHIFT_COUNT        16

typedef union ce_result {
	uint64_t u64;
	uint32_t u32;
	int64_t  i64;
	int32_t  i32;
} ce_result_t;

int             rank_id;
struct utsname  uts_info;
int             v_option = 0;

#include "utility_functions.h"

void print_help(void)
{
    fprintf(stdout,
"\n"
"CE_PMI_EXAMPLE\n"
"  Purpose:\n"
"    The purpose of this example is to demonstrate the FMA CE scatter/gather\n"
"    requests.\n"
"\n"
"    To successfully execute a FMA CE floating point request, the user will\n"
"    need to do a little indirection to be able to pass the floating point\n"
"    operarand to the FMA CE request.  The user will need to create a variable\n"
"    that is a pointer to an uint64_t and assign the address of the floating\n"
"    point double operand variable to it.  Then the first_operand field will be\n"
"    assigned the dereferenced value of the uint64_t pointer variable.\n"
"\n"
"    The source code should look similar to this:\n"
"        double double_operand = 2.5;\n"
"        unint64_t *operand_ptr;\n"
"\n"
"        operand_ptr = (uint64_t *)&double_operand;\n"
"        post_desc.first_operand = *operand_ptr;\n"
"\n"
"    The base CE configuration created by this example is:\n"
"        Root: ***************\n"
"              |Node 0, VCE 0| ----------|-----|-----|-----|\n"
"              ***************           |Node0|Node0|Node0|\n"
"               |                 Local: |Inst0|Inst1|Inst2|...\n"
"               |              ----------|-----|-----|-----|\n"
"               |\n"
"               |\n"
"              ***************\n"
"              |Node 1, VCE 0| ----------|-----|-----|-----|\n"
"              ***************           |Node1|Node1|Node1|\n"
"               |                 Local: |Inst0|Inst1|Inst2|...\n"
"               |              ----------|-----|-----|-----|\n"
"               |\n"
"               |\n"
"              ***************\n"
"              |Node 2, VCE 0| ----------|-----|-----|-----|\n"
"              ***************           |Node2|Node2|Node2|\n"
"                                 Local: |Inst0|Inst1|Inst2|...\n"
"                              ----------|-----|-----|-----|\n"
"\n"
"\n"
"    Another CE configuration created by this example, when using the '-B'\n"
"    option.  An example for '-B 3' is:\n"
"        Root:                        ***************\n"
"                                     |Node 0, VCE 0|\n"
"                                     |  Child Id   |\n"
"                           /-------- | 1    2     3| ----------\\\n"
"                          /          ***************            \\\n"
"                         /                  |                    \\\n"
"                        /                   |                     \\\n"
"                       /                    |                      \\\n"
"       ***************               ***************                ***************\n"
"       | Parent Id 1 |               | Parent Id 2 |                | Parent Id 3 |\n"
"       |Node 1, VCE 0|               |Node 2, VCE 0|                |Node 3, VCE 0|\n"
"       |  Child Id   |               |  Child Id   |                |  Child Id   |\n"
"       | 1    2     3|               | 1    2     3|                | 1    2     3|\n"
"       ***************               ***************                ***************\n"
"       /      |       \\             /      |       \\              /      |       \\\n"
"      /       |        \\           /       |        \\            /       |        \\\n"
"     /        |         \\         /        |         \\          /        |         \\\n"
" ********  ********  ********  ********  ********  ********   ********  ********  ********\n"
" |P Id 1|  |P Id 2|  |P Id 3|  |P Id 1|  |P Id 2|  |P Id 3|   |P Id 1|  |P Id 2|  |P Id 3|\n"
" |Node 4|  |Node 7|  |Node10|  |Node 5|  |Node 8|  |Node11|   |Node 6|  |Node 9|  |Node12|\n"
" | VCE 0|  | VCE 0|  | VCE 0|  | VCE 0|  | VCE 0|  | VCE 0|   | VCE 0|  | VCE 0|  | VCE 0|\n"
" ********  ********  ********  ********  ********  ********   ********  ********  ********\n"
"\n"
"\n"
"  APIs:\n"
"    This example will concentrate on using the following uGNI APIs:\n"
"      - GNI_CeCreate() to create the CE environment.\n"
"      - GNI_GetCeIds() to retrieve the ID of the CE environment.\n"
"      - GNI_EpSetCeAttr() and GNI_CeConfigure() to configure the CE\n"
"        environment.\n"
"      - GNI_PostFma() is used to with the 'CE' type to scatter a\n"
"        data transaction.  The default 'CE' command is 'AND'.\n"
"      - GNI_CeCheckResult() to retrieve the gathered results.\n"
"      - gni_ce_res_get_red_id() to retrieve the CE Id of the gathered\n"
"        results.\n"
"      - gni_ce_res_status_ok() to verify the status of the gathered\n"
"        results.\n"
"\n"
"  Parameters:\n"
"    Additional parameters for this example are:\n"
"      -  '-a' use the Integer ADD CE command.\n"
"      -  '-A' use the Floating Point ADD CE command.\n"
"      -  '-B' use to configure the CE enviroment with multiple branches\n"
"          on each node.  Without this parameter, there will be only\n"
"          one branch per node.  The allowable values for this option\n"
"          are 2, 3 or 4.\n"
"      -  '-f' use the Floating Point Minimum Greatest Index CE command.\n"
"      -  '-F' use the Floating Point Maximum Greatest Index CE command.\n"
"      -  '-h' prints the help information for this example.\n"
"      -  '-i' use the Integer Minimum Greatest Index CE command.\n"
"      -  '-I' use the Integer Maximum Greatest Index CE command.\n"
"      -  '-l' use the Integer Minimum Lowest Index CE command.\n"
"      -  '-L' use the Integer Maximum Lowest Index CE command.\n"
"      -  '-m' use the Floating Point Minimum Lowest Index CE command.\n"
"      -  '-M' use the Floating Point Maximum Lowest Index CE command.\n"
"      -  '-o' use the Integer OR CE command.\n"
"      -  '-s' use the Short versions of the CE commands.\n"
"      -  '-v', '-vv' or '-vvv' allows various levels of output or debug\n"
"         messages to be displayed.  With each additional 'v' more\n"
"         information will be displayed.\n"
"         The default value is no output or debug messages will be\n"
"         displayed.\n"
"      -  '-x' use the Integer XOR CE command.\n"
"\n"
"  Execution:\n"
"    The following is a list of suggested example executions with various\n"
"    options:\n"
"      - ce_pmi_example [-s] [-B n]\n"
"      - ce_pmi_example -a [-s] [-B n]\n"
"      - ce_pmi_example -A [-s] [-B n]\n"
"      - ce_pmi_example -f [-s] [-B n]\n"
"      - ce_pmi_example -F [-s] [-B n]\n"
"      - ce_pmi_example -i [-s] [-B n]\n"
"      - ce_pmi_example -I [-s] [-B n]\n"
"      - ce_pmi_example -l [-s] [-B n]\n"
"      - ce_pmi_example -L [-s] [-B n]\n"
"      - ce_pmi_example -m [-s] [-B n]\n"
"      - ce_pmi_example -M [-s] [-B n]\n"
"      - ce_pmi_example -o [-s] [-B n]\n"
"      - ce_pmi_example -x [-s] [-B n]\n"
"\n"
    );
}

void *ce_err_handler(void *ce_cq)
{
    gni_return_t status = GNI_RC_SUCCESS;
    gni_cq_handle_t cq_hndl = (gni_cq_handle_t)ce_cq;
    gni_cq_entry_t event;

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    if (v_option > 1) {
        fprintf(stderr, "[%s] Rank: %4i %s: Monitoring VCE channel errors, VCE CQ: %p\n",
                uts_info.nodename, rank_id, __FUNCTION__, cq_hndl);
    }

    while (1) {
        status = GNI_CqWaitEvent(cq_hndl, 10000, &event);
        if (status == GNI_RC_SUCCESS) {
            fprintf(stdout,
                        "[%s] Rank: %4i GNI_CqWaitEvent   ce_err_handler ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);
            PMI_Abort(-1, "GNI_CqWaitEvent error in ce_err_handler(): ");
        }
    }
}

int
main(int argc, char **argv)
{
    unsigned int   *all_nic_addresses;
    uint32_t        branches = 1;
    uint32_t        bind_id;
    gni_cdm_handle_t cdm_handle;
    uint32_t        cdm_id;
    gni_fma_cmd_type_t ce_command = GNI_FMA_CE_AND;
    gni_ce_handle_t ce_handle;
    uint32_t        ce_id = -1;
    uint32_t        *ce_ids;
    uint32_t        ce_mode = GNI_CEMODE_TWO_OP;
    gni_ep_handle_t *child_eps;
    uint32_t        child_vce;
    gni_ep_handle_t child_vce_ep;
    gni_post_descriptor_t *completed_post_desc_ptr;
    int             cookie;
    gni_cq_handle_t cq_handle;
    uint32_t        cq_mode = GNI_CQ_BLOCKING;
    int32_t         cur_level;
    gni_cq_entry_t  current_event;
    int             device_id = 0;
    gni_ep_handle_t *endpoint_handles_array;
    double          expected_double_1 = (double) 0.0;
    double          expected_double_2 = (double) 0.0;
    float           expected_float_1 = (float) 0.0;
    float           expected_float_2 = (float) 0.0;
    ce_result_t     expected_result_1;
    ce_result_t     expected_result_2;
    int             first_spawned;
    int             i;
    gni_nic_device_t interconnect = GNI_DEVICE_GEMINI;
    int             j;
    uint32_t        last_leader = -1;
    uint32_t        leaf_child;
    gni_ep_handle_t leaf_ep = NULL;
    uint32_t        leaf_vce;
    unsigned int    local_address;
    gni_ep_handle_t *local_endpoint_handles_array = NULL;
    int             modes = 0;
    int             next_leader = -1;
    gni_nic_handle_t nic_handle;
    uint32_t        node_count;
    uint32_t        node_leader = 1;
    uint32_t        *node_leaders;
    uint32_t        nodes_on_current_level;
    uint32_t        nodes_on_previous_level;
    int             number_of_children_eps;
    int             number_of_cq_entries;
    int             number_of_leaders = 0;
    int             number_of_ranks;
    int             number_of_ranks_on_node = 0;
    int             number_to_process;
    uint32_t        only_leaders = 0;
    double          operand_double_1;
    double          operand_double_2;
    float           operand_float_1;
    float           operand_float_2;
    uint64_t       *operand_1_ptr;
    uint64_t       *operand_2_ptr;
    char            opt;
    extern char    *optarg;
    extern int      optopt;
    uint32_t        parent_vce;
    uint32_t        parent_vce_child_id;
    gni_ep_handle_t parent_vce_ep = NULL;
    gni_post_descriptor_t post_desc;
    int             previous_leader = -1;
    int32_t         previous_level_high_node;
    uint32_t        previous_level_low_node;
    uint8_t         ptag;
    int            *ranks_on_node;
    int             rc;
    unsigned int    remote_address;
    gni_ce_result_t *result_buffer;
    double          result_double_1 = (double) 0.0;
    double          result_double_2 = (double) 0.0;
    float           result_float_1 = (float) 0.0;
    float           result_float_2 = (float) 0.0;
    gni_mem_handle_t result_memory_handle;
    uint64_t       *result_1_ptr;
    uint64_t       *result_2_ptr;
    gni_return_t    status = GNI_RC_SUCCESS;
    double          temp_dp;
    float           temp_sp;
    int             temp_int32;
    int64_t         temp_int64;
    char           *text_pointer;
    pthread_t       thread_id;
    uint32_t        timeout = 1000;
    int             use_short = 0;
    gni_cq_handle_t vce_cq_handle = NULL;
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

    status = GNI_GetDeviceType(&interconnect);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout, "[%s] Rank: %4i GNI_GetDeviceType failed, Error status: %d\n",
                uts_info.nodename, rank_id, status);
        PMI_Finalize();

        exit(0);
    }

    if (interconnect == GNI_DEVICE_GEMINI) {
        fprintf(stdout, "[%s] Rank: %4i This example is not supported on a Gemini interconnect.\n",
                uts_info.nodename, rank_id);
        PMI_Finalize();

        exit(0);
    }

    /*
     * Get the number of ranks on the local node.
     */

    rc = PMI_Get_clique_size(&number_of_ranks_on_node);
    assert(rc == PMI_SUCCESS);

    ranks_on_node = (int *) calloc(number_of_ranks_on_node, sizeof(int));
    assert(ranks_on_node != NULL);

    /*
     * Get the list of ranks on the local node.
     */

    rc = PMI_Get_clique_ranks(ranks_on_node, number_of_ranks_on_node);
    assert(rc == PMI_SUCCESS);

    /*
     * Find the lowest rank on this node.
     */

    for (i = 0; i < number_of_ranks_on_node; i++) {
        if (rank_id > ranks_on_node[i]) {
            node_leader = 0;
            break;
        }
    }

    while ((opt = getopt(argc, argv, "aAB:fFiIhlLmMoOstvx")) != -1) {
        switch (opt) {
        case 'a':
            ce_command = GNI_FMA_CE_IADD;

            break;

        case 'A':
            ce_command = GNI_FMA_CE_FPADD;

            break;

        case 'B':

            /*
             * Set the number of branches for each VCE node.
             */

            branches = atoi(optarg);
            if (branches < 0) {
                branches = 1;
            } else if (branches > 4) {
                branches = 4;
            }

            break;

        case 'f':
            ce_command = GNI_FMA_CE_FPMIN_GIDX;

            break;

        case 'F':
            ce_command = GNI_FMA_CE_FPMAX_GIDX;

            break;

        case 'i':
            ce_command = GNI_FMA_CE_IMIN_GIDX;

            break;

        case 'I':
            ce_command = GNI_FMA_CE_IMAX_GIDX;

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

        case 'l':
            ce_command = GNI_FMA_CE_IMIN_LIDX;

            break;

        case 'L':
            ce_command = GNI_FMA_CE_IMAX_LIDX;

            break;

        case 'm':
            ce_command = GNI_FMA_CE_FPMIN_LIDX;

            break;

        case 'M':
            ce_command = GNI_FMA_CE_FPMAX_LIDX;

            break;

        case 'o':
            ce_command = GNI_FMA_CE_OR;

            break;

        case 'O':
            only_leaders = 1;

            break;

        case 's':
            use_short = 1;

            break;

        case 't':

            /*
             * Set the timeout value for the GNI_CqWaitEvent.
             */

            timeout = atoi(optarg);
            if (timeout < 1000) {
                timeout = 1000;
            } else if (timeout > 1000000) {
                timeout = 1000000;
            }

            break;

        case 'v':
            v_option++;
            break;

        case 'x':
            ce_command = GNI_FMA_CE_XOR;

            break;

        case '?':
            break;
        }
    }

    if (branches > 1) {
        only_leaders = 1;

        if (number_of_ranks_on_node > 1) {
            fprintf(stdout, "[%s] Rank: %4i When using branches, only one instance is allowed per node, requesting %i instances.\n",
                    uts_info.nodename, rank_id, number_of_ranks_on_node);
            PMI_Finalize();

            exit(0);
        }
    } else {
        /*
         * Verify that the number of ranks on a node does not exceed
         * the maximun number of VCE channels that can be created.
         */
        if (number_of_ranks_on_node >= GNI_CE_MAX_CHILDREN) {
            fprintf(stdout, "[%s] Rank: %4i Too many intances per node to create a VCE tree, requested: %i, maximum: %i.\n",
                    uts_info.nodename, rank_id, number_of_ranks_on_node,
                    GNI_CE_MAX_CHILDREN);
            PMI_Finalize();

            exit(0);
        }
    }

    if (v_option) {
        fprintf(stdout,
                "[%s] Rank: %4i Local Rank Info   local_ranks: %4i, node_leader: %4i, branches: %i, only_leaders: %i\n",
                uts_info.nodename, rank_id, number_of_ranks_on_node,
                node_leader, branches, only_leaders);
    }

    /*
     * Get job attributes from PMI.
     */

    ptag = get_ptag();
    cookie = get_cookie();

    if (use_short == 1) {

        switch (ce_command) {
        case GNI_FMA_CE_AND:
            ce_command = GNI_FMA_CE_AND_S;
            break;
        case GNI_FMA_CE_OR:
            ce_command = GNI_FMA_CE_OR_S;
            break;
        case GNI_FMA_CE_XOR:
            ce_command = GNI_FMA_CE_XOR_S;
            break;
        case GNI_FMA_CE_IMIN_LIDX:
            ce_command = GNI_FMA_CE_IMIN_LIDX_S;
            break;
        case GNI_FMA_CE_IMAX_LIDX:
            ce_command = GNI_FMA_CE_IMAX_LIDX_S;
            break;
        case GNI_FMA_CE_IADD:
            ce_command = GNI_FMA_CE_IADD_S;
            break;
        case GNI_FMA_CE_FPMIN_LIDX:
            ce_command = GNI_FMA_CE_FPMIN_LIDX_S;
            break;
        case GNI_FMA_CE_FPMAX_LIDX:
            ce_command = GNI_FMA_CE_FPMAX_LIDX_S;
            break;
        case GNI_FMA_CE_FPADD:
            ce_command = GNI_FMA_CE_FPADD_S;
            break;
        case GNI_FMA_CE_IMIN_GIDX:
            ce_command = GNI_FMA_CE_IMIN_GIDX_S;
            break;
        case GNI_FMA_CE_IMAX_GIDX:
            ce_command = GNI_FMA_CE_IMAX_GIDX_S;
            break;
        case GNI_FMA_CE_FPMIN_GIDX:
            ce_command = GNI_FMA_CE_FPMIN_GIDX_S;
            break;
        case GNI_FMA_CE_FPMAX_GIDX:
            ce_command = GNI_FMA_CE_FPMAX_GIDX_S;
            break;
        default:
            break;
        }
    }

    node_leaders = (uint32_t *)calloc(number_of_ranks, sizeof(uint32_t));
    assert(node_leaders != NULL);
    allgather(&node_leader, node_leaders, sizeof(uint32_t));

    for (i = number_of_ranks; i >= 0; i--) {
        if (node_leaders[i] == 1) {
            last_leader = i;
            break;
        }
    }

    /*
     * Determine the number of passes required for this test to be successful.
     */

    if (node_leader == 1) {
        if (branches == 1) {
            if (only_leaders == 0) {
                expected_passed = 7 + number_of_ranks_on_node;
            } else {
                expected_passed = 8;
            }
            if ((rank_id != 0) && (rank_id != last_leader)) {
                expected_passed++;
            }
        } else {
            expected_passed = 7;

            if (rank_id == 0) {
                previous_level_low_node = 0;
                previous_level_high_node = -1;
                nodes_on_previous_level = 1;
                cur_level = 0;
                node_count = number_of_ranks - 1;

                /*
                 * Determine how many branches are on each node.
                 */
                while (cur_level < branches) {
                    if (node_count < nodes_on_previous_level) {
                        break;
                    }

                    /*
                     * Add one for each child node.
                     */
                    expected_passed++;
                    node_count = node_count - nodes_on_previous_level;
                    cur_level++;
                }
            } else {
                /* This is for having a parent node. */
                expected_passed++;

                previous_level_low_node = 0;
                previous_level_high_node = -1;
                nodes_on_previous_level = 1;
                cur_level = number_of_ranks;

                /*
                 * Determine the low and high rank ids of the second to last level.
                 */
                while (cur_level > 0) {
                    cur_level = cur_level - nodes_on_previous_level;
                    if (cur_level > 0) {
                        previous_level_low_node = previous_level_high_node + 1;
                        previous_level_high_node = previous_level_high_node + nodes_on_previous_level;
                        nodes_on_previous_level = nodes_on_previous_level * branches;
                    }
                }

                nodes_on_previous_level = nodes_on_previous_level / branches;

                if (rank_id <= previous_level_high_node) {
                    if (rank_id < previous_level_low_node) {
                        /*
                         * The current rank is before the second to last level.
                         *
                         * This is for each child node.
                         */
                        expected_passed = expected_passed + branches;
                    } else {
                        cur_level = 0;
                        node_count = number_of_ranks - rank_id - 1;

                        /*
                         * Determine how many branches are on each node.
                         */
                        while (cur_level < branches) {
                            if (node_count < nodes_on_previous_level) {
                                break;
                            }

                            /*
                             * Add one for each child node.
                             */
                            expected_passed++;
                            node_count = node_count - nodes_on_previous_level;
                            cur_level++;
                        }
                    }
                }
            }
        }
    } else {
        if (only_leaders == 0) {
            expected_passed = 3;
        } else {
            expected_passed = 0;
        }
    }

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

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CdmCreate     inst_id: %i, ptag: %u, cookie: 0x%x, modes: 0x%x\n",
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

    if (v_option > 2) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CdmAttach     to NIC\n",
                uts_info.nodename, rank_id);
    }

    /*
     * Determine the minimum number of completion queue entries, which
     * is the number of outstanding transactions at one time.
     */

    number_of_cq_entries = number_of_ranks * 4;

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

    status = GNI_CqCreate(nic_handle, number_of_cq_entries, 0, cq_mode,
                     NULL, NULL, &cq_handle);
    if (status != GNI_RC_SUCCESS) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      local ERROR status: %s (%d)\n",
                uts_info.nodename, rank_id, gni_err_str[status], status);
        INCREMENT_ABORTED;
        goto EXIT_DOMAIN;
    }

    if (v_option > 2) {
        fprintf(stdout,
                "[%s] Rank: %4i GNI_CqCreate      local with %i entries\n",
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
        if ((only_leaders == 0) || ((node_leader == 1) && (node_leaders[i] == 1))) {
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
                        "[%s] Rank: %4i GNI_EpCreate      ERROR remote rank: %4i, status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_ENDPOINT;
            }

            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpCreate      remote rank: %4i, NIC: %p, CQ: %p, EP: %p\n",
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
                        "[%s] Rank: %4i GNI_EpBind        ERROR remote rank: %4i, status: %s (%d)\n",
                        uts_info.nodename, rank_id, i, gni_err_str[status], status);
                INCREMENT_ABORTED;
                goto EXIT_ENDPOINT;
            }

            if (((node_leader == 1) && v_option > 1) || (v_option > 2)) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpBind        remote rank: %4i, EP:  %p, remote_address: %u, remote_id: %u\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i], remote_address, bind_id);
            }
        }
    }

    /*
     * wait for all the processes to initialize their communication paths
     */
    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    if (node_leader == 1) {
        local_endpoint_handles_array = (gni_ep_handle_t *)calloc(number_of_ranks_on_node,
                                                                 sizeof(gni_ep_handle_t *));
        assert(local_endpoint_handles_array != NULL);

        /*
         * Get all of the local endpoints for this node.
         */
        for (i = 0; i < number_of_ranks_on_node; i++) {
            for (j = 0; j < number_of_ranks; j++) {
                if (j == ranks_on_node[i]) {
                    local_endpoint_handles_array[i] = endpoint_handles_array[j];

                    if (v_option) {
                        fprintf(stdout,
                                "[%s] Rank: %4i Local End Points         rank: %4i, EP: %p\n",
                                uts_info.nodename, rank_id, ranks_on_node[i],
                                local_endpoint_handles_array[i]);
                    }
                }
            }
        }
    }

    /*
     * Determine the number of node leaders.
     */
    for (i = 0; i < number_of_ranks; i++) {
        if (node_leaders[i] == 1) {
            number_of_leaders++;
        }
    }

    /*
     * If this is a node leader, then create the VCE channel.
     */
    if (node_leader == 1) {
        /*
         * Allocate a VCE Channel.
         *     nic_handle is the NIC handle that this completion queue will be
         *          associated with.
         *     ce_handle is the handle that is returned pointing to the VCE Channel.
         */
        status = GNI_CeCreate(nic_handle, &ce_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeCreate      ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_ENDPOINT;
        }

        INCREMENT_PASSED;

        if (v_option > 1) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeCreate      CE: %p\n",
                    uts_info.nodename, rank_id, ce_handle);
        }

        /*
         * Get the ID assocated with this VCE Channel.
         *     ce_handle is the handle of the VCE Channel.
         *     ce_id is ID assocated with the VCE Channel.
         */
        status = GNI_CeGetId(ce_handle, &ce_id);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeGetId       ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CE;
        }

        INCREMENT_PASSED;

        if (v_option) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeGetId             CE id: %4i, CE: %p\n",
                    uts_info.nodename, rank_id, ce_id, ce_handle);
        }

        /*
         * Create a completion queue to receive the VCE Channel events.
         *     nic_handle is the NIC handle that this completion queue will be
         *          associated with.
         *     number_of_cq_entries is the size of the completion queue.
         *     zero is the delay count is the number of allowed events before
         *          an interrupt is generated.
         *     cq_mode states that the operation mode is blocking.
         *     NULL states that no user supplied callback function is defined.
         *     NULL states that no user supplied pointer is passed to the
         *          callback function.
         *     vce_cq_handle is the handle that is returned pointing to this newly
         *          created completion queue.
         */

        status = GNI_CqCreate(nic_handle, number_of_cq_entries, 0,
                      cq_mode, NULL, NULL, &vce_cq_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqCreate      VCE ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CE;
        }

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqCreate      VCE with %i entries\n",
                    uts_info.nodename, rank_id, number_of_cq_entries);
        }
    }

    /*
     * Get all of the VCE IDs for all of the ranks.
     */

    ce_ids = (uint32_t *)calloc(number_of_ranks, sizeof(uint32_t));
    assert(ce_ids != NULL);
    allgather(&ce_id, ce_ids, sizeof(uint32_t));

    if (node_leader == 1) {
        /*
         * The leader rank configures the local VCE channels.
         */

        child_eps = (gni_ep_handle_t *)calloc(number_of_ranks, sizeof(gni_ep_handle_t));
        assert(child_eps != NULL);
        number_of_children_eps = 0;

        if (only_leaders == 0) {
            number_to_process = number_of_ranks_on_node;
        } else {
            number_to_process = 1;
        }

        /*
         * Configure the attributes for the VCE channel to all of the leafs.
         */
        for (i = 0; i < number_to_process; i++) {
            if ((only_leaders == 0) || (node_leader == 1)) {
                child_eps[number_of_children_eps] = local_endpoint_handles_array[i];

                /*
                 * Store the CE tree attributes into the child endpoint.
                 *    child_eps is the child endpoint to store the attributes into.
                 *    -1, is the CE_ID to store into the endpoint.
                 *    number_of_children_eps is the ID of this child.
                 *    GNI_CE_CHILD_PE is the child type to store in the endpoint.
                 *
                 * NOTE:  the ce_id is not required and therefore not used.
                 */

                status = GNI_EpSetCeAttr(child_eps[number_of_children_eps], -1,
                                         number_of_children_eps, GNI_CE_CHILD_PE);
                if (status != GNI_RC_SUCCESS) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_EpSetCeAttr  VCE->leaf ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_ABORTED;
                    goto EXIT_CE;
                }

                INCREMENT_PASSED;

                if (v_option) {
                    fprintf(stdout, "[%s] Rank: %4i GNI_EpSetCeAttr     VCE->leaf:       CE id: %4i, child_id: %4i,  local_rank: %4i, EP: %p\n",
                            uts_info.nodename, rank_id, -1, number_of_children_eps,
                            ranks_on_node[i], child_eps[number_of_children_eps]);
                }

                number_of_children_eps++;
            }
        }

        /*
         * Configure the attributes for the child VCE channel
         * to the parent VCE channel.
         */
        previous_leader = -1;
        nodes_on_previous_level = 0;
        node_count = 1;
        previous_level_low_node = 0;

        for (i = 0; i < number_of_ranks; i++) {
            if (node_leaders[i] == 1) {
                if (rank_id == i) {
                    if (previous_leader != -1) {
                        if (branches == 1) {
                            if (only_leaders == 0) {
                                parent_vce_child_id = number_of_ranks_on_node;
                            } else {
                                parent_vce_child_id = 1;
                            }
                        } else {
                            /*
                             * Determine the child_id from the parent for
                             * this VCE connection.
                             *
                             * NOTE: the child_id value must match for this
                             *       connection between the parent and child
                             *       or else the gather operation will not succeed.
                             */
                            previous_leader = i % nodes_on_previous_level;
                            if (previous_leader < previous_level_low_node) {
                                previous_leader = previous_leader + nodes_on_previous_level;
                            }
                            parent_vce_child_id = (rank_id - previous_leader) / nodes_on_previous_level;
                        }

                        /*
                         * Store the CE tree attributes into the parent endpoint.
                         *    parent_vce_ep is the endpoint to the previous VCE (parent).
                         *    parent_vce is the CE_ID to the previous VCE (parent).
                         *    parent_vce_child_id is the id of the previous VCE (parent)
                         *        connected to this node.
                         *    GNI_CE_CHILD_VCE is the parent type to store in the endpoint.
                         */
                        parent_vce_ep = endpoint_handles_array[previous_leader];
                        parent_vce = ce_ids[previous_leader];
                        status = GNI_EpSetCeAttr(parent_vce_ep, parent_vce,
                                                 parent_vce_child_id, GNI_CE_CHILD_VCE);
                        if (status != GNI_RC_SUCCESS) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_EpSetCeAttr   parent VCE ERROR status: %s (%d)\n",
                                    uts_info.nodename, rank_id, gni_err_str[status], status);
                            INCREMENT_ABORTED;
                            goto EXIT_CE;
                        }

                        INCREMENT_PASSED;

                        if (v_option) {
                            fprintf(stdout, "[%s] Rank: %4i GNI_EpSetCeAttr    parent VCE:       CE id: %4i, child_id: %4i, remote_rank: %4i, EP: %p\n",
                                    uts_info.nodename, rank_id, parent_vce,
                                    parent_vce_child_id, previous_leader, parent_vce_ep);
                        }
                    } else {
                        parent_vce_ep = NULL;
                    }

                    break;
                }

                if (branches == 1) {
                    previous_leader = i;
                } else {
                    if (i == 0) {
                        nodes_on_previous_level = 1;
                        previous_leader = i;
                    } else if (node_count >= (nodes_on_previous_level * branches)) {
                        previous_level_low_node = previous_level_low_node + nodes_on_previous_level;
                        nodes_on_previous_level = nodes_on_previous_level * branches;
                        node_count = 1;
                    } else {
                        node_count++;
                    }
                }
            }
        }

        if (branches == 1) {
            /*
             * Configure the attributes for the parent VCE channel
             * to the child VCE channel.
             */
            next_leader = -1;

            for (i = (number_of_ranks - 1); i >= 0; i--) {
                if (node_leaders[i] == 1) {
                    if (rank_id == i) {
                        if (next_leader != -1) {
                            /*
                             * Store the CE tree attributes into the child endpoint.
                             *    child_vce_ep is the endpoint to the next VCE (parent).
                             *    child_vce is the CE_ID to the next VCE (parent).
                             *    number_of_children_eps is the number of endpoints
                             *        connected to this endpoint.
                             *    GNI_CE_CHILD_VCE is the child type to store in the endpoint.
                             */
                            child_vce_ep = endpoint_handles_array[next_leader];
                            child_vce = ce_ids[next_leader];
                            status = GNI_EpSetCeAttr(child_vce_ep, child_vce,
                                                     number_of_children_eps, GNI_CE_CHILD_VCE);
                            if (status != GNI_RC_SUCCESS) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_EpSetCeAttr   child VCE ERROR status: %s (%d)\n",
                                        uts_info.nodename, rank_id, gni_err_str[status], status);
                                INCREMENT_ABORTED;
                                goto EXIT_CE;
                            }

                            INCREMENT_PASSED;

                            if (v_option) {
                                fprintf(stdout, "[%s] Rank: %4i GNI_EpSetCeAttr     child VCE:       CE id: %4i, child_id: %4i, remote_rank: %4i, EP; %p\n",
                                        uts_info.nodename, rank_id, child_vce,
                                        number_of_children_eps, next_leader, child_vce_ep);
                            }

                            child_eps[number_of_children_eps] = child_vce_ep;
                            number_of_children_eps++;
                        }

                        break;
                    }

                    next_leader = i;
                }
            }
        } else {
            nodes_on_current_level = 1;
            node_count = 1;

            for (i = 0; i < number_of_ranks; i++) {
                if (rank_id == i) {
                    for (j = 1; j <= branches; j++) {
                        next_leader = i + (nodes_on_current_level * j);

                        if (next_leader < number_of_ranks) {
                            /*
                             * Store the CE tree attributes into the child endpoint.
                             *    child_vce_ep is the endpoint to the next VCE (parent).
                             *    child_vce is the CE_ID to the next VCE (parent).
                             *    number_of_children_eps is the number of endpoints
                             *        connected to this endpoint.
                             *    GNI_CE_CHILD_VCE is the child type to store in the endpoint.
                             */
                            child_vce_ep = endpoint_handles_array[next_leader];
                            child_vce = ce_ids[next_leader];
                            status = GNI_EpSetCeAttr(child_vce_ep, child_vce,
                                                     number_of_children_eps, GNI_CE_CHILD_VCE);
                            if (status != GNI_RC_SUCCESS) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i GNI_EpSetCeAttr   child VCE ERROR status: %s (%d)\n",
                                        uts_info.nodename, rank_id, gni_err_str[status], status);
                                INCREMENT_ABORTED;
                                goto EXIT_CE;
                            }

                            INCREMENT_PASSED;

                            if (v_option) {
                                fprintf(stdout, "[%s] Rank: %4i GNI_EpSetCeAttr     child VCE:       CE id: %4i, child_id: %4i, remote_rank: %4i, EP; %p\n",
                                        uts_info.nodename, rank_id, child_vce,
                                        number_of_children_eps, next_leader, child_vce_ep);
                            }

                            child_eps[number_of_children_eps] = child_vce_ep;
                            number_of_children_eps++;
                        }
                    }

                    break;
                } else {
                    if (node_count >= nodes_on_current_level) {
                        nodes_on_current_level = nodes_on_current_level * branches;
                        node_count = 1;
                    } else {
                        node_count++;
                    }
                }
            }
        }

        /*
         * Configure the attributes for this VCE channel.
         *    ce_handle is the CE handle of the VCE channel to configure.
         *    child_pes is an array of child endponts to be configured for
         *        this VCE channel.
         *    number_of_children_eps is the number of child endpoints being
         *        configured for this VCE channel.
         *    parent_vce_ep is the parent endpoint for this VCE channel.
         *    vce_cq_handle is the completion queue handle that will be
         *        receiving CQ events for this VCE channel.
         *    GNI_CE_MOD_CQE_ONERR and GNI_CE_MODE_ROUND_ZERO describes the
         *        types of modes that are configured for this VCE channel.
         */
        status = GNI_CeConfigure(ce_handle, child_eps,
                     number_of_children_eps, parent_vce_ep, vce_cq_handle,
                     GNI_CE_MODE_CQE_ONERR | GNI_CE_MODE_ROUND_ZERO);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeConfigure  VCE ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CE;
        }

        INCREMENT_PASSED;

        if (v_option) {
            fprintf(stdout, "[%s] Rank: %4i GNI_CeConfigure           VCE:    children: %4i, CE: %p, parent EP: %p\n",
                    uts_info.nodename, rank_id, number_of_children_eps,
                    ce_handle, parent_vce_ep);
        }

        assert(!pthread_create(&thread_id, NULL, ce_err_handler, (void *)vce_cq_handle));
        if (v_option > 1) {
            fprintf(stdout, "[%s] Rank: %4i pthread_create   ce_err_handler thread_id: %p VCE CQ: %p\n",
                    uts_info.nodename, rank_id, (void *) thread_id, vce_cq_handle);
        }

        free(child_eps);
    }

    if ((only_leaders == 0) || (node_leader == 1)) {
        /*
         * Configure the attributes for the leaf to the VCE channel.
         */
        leaf_ep = endpoint_handles_array[ranks_on_node[0]];
        leaf_vce = ce_ids[ranks_on_node[0]];
        leaf_child = -1;

        for (i = 0; i < number_of_ranks_on_node; i++) {
            if (rank_id == ranks_on_node[i]) {
                leaf_child = i;
                break;
            }
        }

        /*
         * Store the CE tree attributes into the child endpoint.
         *    leaf_ep is the endpoint to store the attributes into.
         *    leaf_vce is the CE_ID to store into the endpoint.
         *    leaf_child is the ID of this child.
         *    GNI_CE_CHILD_PE is the child type to store in the endpoint.
         */
        status = GNI_EpSetCeAttr(leaf_ep, leaf_vce, leaf_child, GNI_CE_CHILD_PE);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_EpSetCeAttr  leaf->VCE ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CE;
        }

        INCREMENT_PASSED;

        if (v_option) {
            fprintf(stdout, "[%s] Rank: %4i GNI_EpSetCeAttr     leaf->VCE:       CE id: %4i, child_id: %4i,  local_rank: %4i, EP: %p\n",
                    uts_info.nodename, rank_id, leaf_vce, leaf_child,
                    ranks_on_node[0], leaf_ep);
        }

        /*
         * Allocate the buffer that will receive the data.  This allocation is
         * creating a buffer large enough to hold all of the received data.
         */

        rc = posix_memalign((void **) &result_buffer, 32, sizeof(gni_ce_result_t));
        assert(rc == 0);

        /*
         * Register the memory associated for the receive buffer with the NIC.
         * We are receiving the data into this buffer.
         *     nic_handle is our NIC handle.
         *     result_buffer is the memory location of the receive buffer.
         *     sizeof(*result_buffer) is the size of the memory allocated
         *         to the result buffer.
         *     NULL means that no destination completion queue handle is specified.
         *         We are sending the data from this buffer not receiving.
         *     GNI_MEM_READWRITE is the read/write attribute for the target buffer's
         *         memory region.
         *     vmdh_index specifies the index within the allocated memory region,
         *         a value of -1 means that the GNI library will determine this index.
         *     result_memory_handle is the handle for this memory region.
         */

        status = GNI_MemRegister(nic_handle, (uint64_t) result_buffer,
                                 sizeof(*result_buffer), NULL,
                                 0, vmdh_index,
                                 &result_memory_handle);

        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemRegister  result_buffer ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
            goto EXIT_CE;
        }

        if (v_option > 2) {
            fprintf(stdout,                                              
                    "[%s] Rank: %4i GNI_MemRegister  result_buffer  size: %u, address: %p\n",
                    uts_info.nodename, rank_id,
                    (unsigned int) (sizeof(*result_buffer)),
                    result_buffer);
        }
    }

    /*
     * wait for all the processes to sync up before sending requests
     */
    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    if ((only_leaders == 0) || (node_leader == 1)) {
        memset(result_buffer, 0, sizeof(*result_buffer));
        memset(&post_desc, 0, sizeof(gni_post_descriptor_t));

        /*
         * Setup the data request.
         *    type is CE.
         *    cq_mode states what type of events should be sent.
         *         GNI_CQMODE_GLOBAL_EVENT allows for the sending of an event
         *             to the local node after the receipt of the data.
         *         GNI_CQMODE_REMOTE_EVENT allows for the sending of an event
         *             to the remote node after the receipt of the data.
         *    local_addr is the address of the sending buffer.
         *    local_mem_hndl is the memory handle of the sending buffer.
         *    ce_cmd is the CE operation that is done on the data
         *        on the remote endpoint.
         *    ce_mode is the number of operands used by the CE command.
         *    ce_red_id is the Id associated with this specific CE operation.
         *    first_operand is used by the CE operation as one of
         *        the operands.
         *    second_operand is used by the CE operation as one of
         *        the operands.
         */
            
        post_desc.type = GNI_POST_CE;
        post_desc.cq_mode = GNI_CQMODE_GLOBAL_EVENT;
        post_desc.dlvr_mode = GNI_DLVMODE_PERFORMANCE;
        post_desc.local_addr = (uint64_t) result_buffer;
        post_desc.local_mem_hndl = result_memory_handle;
        post_desc.ce_cmd = ce_command;
        post_desc.ce_mode = ce_mode;
        post_desc.ce_red_id = CE_RED_ID;

        expected_result_1.u64 = (uint64_t) 0;
        expected_result_2.u64 = (uint64_t) 0;

        /*
         * Set up the operand values and expected operand results.
         */

        switch (ce_command) {
        case GNI_FMA_CE_AND_S:
            post_desc.first_operand = ~(((uint32_t) 1) << rank_id);
            post_desc.second_operand = ~((((uint32_t) 1) << rank_id) << SHORT_SHIFT_COUNT);

            expected_result_1.u32 = (uint32_t) ALL_ONES_DATA;
            expected_result_2.u32 = (uint32_t) ALL_ONES_DATA;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u32 = expected_result_1.u32 & ~(((uint64_t) 1) << i);
                    expected_result_2.u32 = expected_result_2.u32 & ~((((uint64_t) 1) << i) << SHORT_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_AND:
            post_desc.first_operand = ~(((uint64_t) 1) << rank_id);
            post_desc.second_operand = ~((((uint64_t) 1) << rank_id) << LONG_SHIFT_COUNT);

            expected_result_1.i64 = (uint64_t) ALL_ONES_DATA;
            expected_result_2.i64 = (uint64_t) ALL_ONES_DATA;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u64 = expected_result_1.u64 & ~(((uint64_t) 1) << i);
                    expected_result_2.u64 = expected_result_2.u64 & ~((((uint64_t) 1) << i) << LONG_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_OR_S:
            post_desc.first_operand = ((uint32_t) 1) << rank_id;
            post_desc.second_operand = (((uint32_t) 1) << rank_id) << SHORT_SHIFT_COUNT;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u32 = expected_result_1.u32 | (((uint64_t) 1) << i);
                    expected_result_2.u32 = expected_result_2.u32 | ((((uint64_t) 1) << i) << SHORT_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_OR:
            post_desc.first_operand = ((uint64_t) 1) << rank_id;
            post_desc.second_operand = (((uint64_t) 1) << rank_id) << LONG_SHIFT_COUNT;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u64 = expected_result_1.u64 | (((uint64_t) 1) << i);
                    expected_result_2.u64 = expected_result_2.u64 | ((((uint64_t) 1) << i) << LONG_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_XOR_S:
            post_desc.first_operand = ((uint32_t) 1) << rank_id;
            post_desc.second_operand = (((uint32_t) 1) << rank_id) << SHORT_SHIFT_COUNT;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u32 = expected_result_1.u32 ^ (((uint64_t) 1) << i);
                    expected_result_2.u32 = expected_result_2.u32 ^ ((((uint64_t) 1) << i) << SHORT_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_XOR:
            post_desc.first_operand = ((uint64_t) 1) << rank_id;
            post_desc.second_operand = (((uint64_t) 1) << rank_id) << LONG_SHIFT_COUNT;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.u64 = expected_result_1.u64 ^ (((uint64_t) 1) << i);
                    expected_result_2.u64 = expected_result_2.u64 ^ ((((uint64_t) 1) << i) << LONG_SHIFT_COUNT);
                }
            }

            break;

        case GNI_FMA_CE_IMIN_LIDX_S:
            post_desc.first_operand = ((int32_t) (1 + (rank_id % 2))) * INT_MULTIPLIER;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_result_1.i32 = (int32_t) MAXIMUM_SHORT_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int32 = ((int32_t) (1 + (i % 2))) * INT_MULTIPLIER;
                    if (temp_int32 <= expected_result_1.i32) {
                        expected_result_1.i32 = temp_int32;
                        expected_result_2.i32 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMIN_LIDX:
            post_desc.first_operand = ((int64_t) (1 + (rank_id % 2))) * LONG_MULTIPLIER;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_result_1.i64 = (int64_t) MAXIMUM_LONG_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int64 = ((int64_t) (1 + (i % 2))) * LONG_MULTIPLIER;
                    if (temp_int64 <= expected_result_1.i64) {
                        expected_result_1.i64 = temp_int64;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMAX_LIDX_S:
            post_desc.first_operand = ((int32_t) (1 + (rank_id % 2))) * INT_MULTIPLIER;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_result_1.i32 = (int32_t) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int32 = ((int32_t) (1 + (i % 2))) * INT_MULTIPLIER;
                    if (temp_int32 >= expected_result_1.i32) {
                        expected_result_1.i32 = temp_int32;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMAX_LIDX:
            post_desc.first_operand = ((int64_t) (1 + (rank_id % 2))) * LONG_MULTIPLIER;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_result_1.i64 = (int64_t) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int64 = ((int64_t) (1 + (i % 2))) * LONG_MULTIPLIER;
                    if (temp_int64 >= expected_result_1.i64) {
                        expected_result_1.i64 = temp_int64;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IADD_S:
            post_desc.first_operand = (int32_t) (1 + rank_id);
            post_desc.second_operand = (int32_t) ((1 + rank_id) * INT_MULTIPLIER);

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.i32 = expected_result_1.i32 + 1 + i;
                }
            }

            expected_result_2.i32 = expected_result_1.i32 * INT_MULTIPLIER;

            break;

        case GNI_FMA_CE_IADD:
            post_desc.first_operand = (int64_t) (1 + rank_id);
            post_desc.second_operand = (int64_t) ((1 + rank_id) * LONG_MULTIPLIER);

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_result_1.i64 = expected_result_1.i64 + 1 + i;
                }
            }

            expected_result_2.i64 = expected_result_1.i64 * LONG_MULTIPLIER;

            break;

        case GNI_FMA_CE_FPMIN_LIDX_S:
            operand_float_1 = ((float) (1.0 + (rank_id % 2))) * FLOAT_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_float_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_float_1 = (float) MAXIMUM_SHORT_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_sp = ((float) (1.0 + (i % 2))) * FLOAT_MULTIPLIER;
                    if (temp_sp <= expected_float_1) {
                        expected_float_1 = temp_sp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMIN_LIDX:
            operand_double_1 = ((double) (1.0 + (rank_id % 2))) * DOUBLE_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_double_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_double_1 = (double) MAXIMUM_LONG_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_dp = ((double) (1.0 + (i % 2))) * DOUBLE_MULTIPLIER;
                    if (temp_dp <= expected_double_1) {
                        expected_double_1 = temp_dp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMAX_LIDX_S:
            operand_float_1 = ((float) (1.0 + (rank_id % 2))) * FLOAT_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_float_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_float_1 = (float) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_sp = ((float) (1.0 + (i % 2))) * FLOAT_MULTIPLIER;
                    if (temp_sp >= expected_float_1) {
                        expected_float_1 = temp_sp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMAX_LIDX:
            operand_double_1 = ((double) (1.0 + (rank_id % 2))) * DOUBLE_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_double_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_double_1 = (double) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = number_of_ranks - 1; i >= 0; i--) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_dp = ((double) (1.0 + (i % 2))) * DOUBLE_MULTIPLIER;
                    if (temp_dp >= expected_double_1) {
                        expected_double_1 = temp_dp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPADD_S:
            operand_float_1 = (float) (1.0 + rank_id);
            operand_1_ptr = (uint64_t *)&operand_float_1;
            post_desc.first_operand = *operand_1_ptr;
            operand_float_2 = (float) ((1.0 + rank_id) * FLOAT_MULTIPLIER);
            operand_2_ptr = (uint64_t *)&operand_float_2;
            post_desc.second_operand = *operand_2_ptr;

            expected_float_1 = (float) 0.0;
            expected_float_2 = (float) 0.0;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_float_1 = expected_float_1 + ((float) (1.0 + i));
                }
            }

            expected_float_2 = expected_float_1 * FLOAT_MULTIPLIER;
            break;

        case GNI_FMA_CE_FPADD:
            operand_double_1 = (double) (1.0 + rank_id);
            operand_1_ptr = (uint64_t *)&operand_double_1;
            post_desc.first_operand = *operand_1_ptr;
            operand_double_2 = (double) ((1.0 + rank_id) * DOUBLE_MULTIPLIER);
            operand_2_ptr = (uint64_t *)&operand_double_2;
            post_desc.second_operand = *operand_2_ptr;

            expected_double_1 = (double) 0.0;
            expected_double_2 = (double) 0.0;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    expected_double_1 = expected_double_1 + ((double) (1.0 + i));
                }
            }

            expected_double_2 = expected_double_1 * DOUBLE_MULTIPLIER;
            break;

        case GNI_FMA_CE_IMIN_GIDX_S:
            post_desc.first_operand = ((int32_t) (1 + (rank_id % 2))) * INT_MULTIPLIER;
            post_desc.second_operand = (int32_t) rank_id;
            expected_result_1.i32 = (int32_t) MAXIMUM_SHORT_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int32 = ((int32_t) (1 + (i % 2))) * INT_MULTIPLIER;
                    if (temp_int32 <= expected_result_1.i32) {
                        expected_result_1.i32 = temp_int32;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMIN_GIDX:
            post_desc.first_operand = ((int64_t) (1 + (rank_id % 2))) * LONG_MULTIPLIER;
            post_desc.second_operand = (int64_t) rank_id;
            expected_result_1.i64 = (int64_t) MAXIMUM_LONG_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int64 = ((int64_t) (1 + (i % 2))) * LONG_MULTIPLIER;
                    if (temp_int64 <= expected_result_1.i64) {
                        expected_result_1.i64 = temp_int64;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMAX_GIDX_S:
            post_desc.first_operand = ((int32_t) (1 + (rank_id % 2))) * INT_MULTIPLIER;
            post_desc.second_operand = (int32_t) rank_id;
            expected_result_1.i32 = (int32_t) 0;
            expected_result_2.u64 = (uint32_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int32 = ((int32_t) (1 + (i % 2))) * INT_MULTIPLIER;
                    if (temp_int32 >= expected_result_1.i32) {
                        expected_result_1.i32 = temp_int32;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_IMAX_GIDX:
            post_desc.first_operand = ((int64_t) (1 + (rank_id % 2))) * LONG_MULTIPLIER;
            post_desc.second_operand = (int64_t) rank_id;
            expected_result_1.i64 = (int64_t) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_int64 = ((int64_t) (1 + (i % 2))) * LONG_MULTIPLIER;
                    if (temp_int64 >= expected_result_1.i64) {
                        expected_result_1.i64 = temp_int64;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMIN_GIDX_S:
            operand_float_1 = ((float) (1.0 + (rank_id % 2))) * FLOAT_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_float_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_float_1 = (float) MAXIMUM_SHORT_INT;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_sp = ((float) (1.0 + (i % 2))) * FLOAT_MULTIPLIER;
                    if (temp_sp <= expected_float_1) {
                        expected_float_1 = temp_sp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMIN_GIDX:
            operand_double_1 = ((double) (1.0 + (rank_id % 2))) * DOUBLE_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_double_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_double_1 = (double) MAXIMUM_LONG_INT;
            expected_result_2.u64 = (double) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_dp = ((double) (1.0 + (i % 2))) * DOUBLE_MULTIPLIER;
                    if (temp_dp <= expected_double_1) {
                        expected_double_1 = temp_dp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMAX_GIDX_S:
            operand_float_1 = ((float) (1.0 + (rank_id % 2))) * FLOAT_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_float_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_float_1 = (float) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_sp = ((float) (1.0 + (i % 2))) * FLOAT_MULTIPLIER;
                    if (temp_sp >= expected_float_1) {
                        expected_float_1 = temp_sp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        case GNI_FMA_CE_FPMAX_GIDX:
            operand_double_1 = ((double) (1.0 + (rank_id % 2))) * DOUBLE_MULTIPLIER;
            operand_1_ptr = (uint64_t *)&operand_double_1;
            post_desc.first_operand = *operand_1_ptr;
            post_desc.second_operand = (uint64_t) rank_id;

            expected_double_1 = (double) 0;
            expected_result_2.u64 = (uint64_t) number_of_ranks;

            for (i = 0; i < number_of_ranks; i++) {
                if ((only_leaders == 0) || (node_leaders[i] == 1)) {
                    temp_dp = ((double) (1.0 + (i % 2))) * DOUBLE_MULTIPLIER;
                    if (temp_dp >= expected_double_1) {
                        expected_double_1 = temp_dp;
                        expected_result_2.u64 = i;
                    }
                }
            }

            break;

        default:
            break;
        }

        if (v_option) {
            switch (post_desc.ce_cmd) {
            case GNI_FMA_CE_AND_S:
            case GNI_FMA_CE_AND:
            case GNI_FMA_CE_OR_S:
            case GNI_FMA_CE_OR:
            case GNI_FMA_CE_XOR_S:
            case GNI_FMA_CE_XOR:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: 0x%016lx, operand 2: 0x%016lx\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        post_desc.first_operand,
                        post_desc.second_operand);
                 break;

             case GNI_FMA_CE_IMIN_LIDX_S:
             case GNI_FMA_CE_IMAX_LIDX_S:
             case GNI_FMA_CE_IADD_S:
             case GNI_FMA_CE_IMIN_GIDX_S:
             case GNI_FMA_CE_IMAX_GIDX_S:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %i operand 2: %i\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        (int32_t) post_desc.first_operand,
                        (int32_t) post_desc.second_operand);
                 break;

             case GNI_FMA_CE_IMIN_LIDX:
             case GNI_FMA_CE_IMAX_LIDX:
             case GNI_FMA_CE_IADD:
             case GNI_FMA_CE_IMIN_GIDX:
             case GNI_FMA_CE_IMAX_GIDX:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %li operand 2: %li\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        (int64_t) post_desc.first_operand,
                        (int64_t) post_desc.second_operand);
                 break;

             case GNI_FMA_CE_FPMIN_LIDX_S:
             case GNI_FMA_CE_FPMAX_LIDX_S:
             case GNI_FMA_CE_FPMIN_GIDX_S:
             case GNI_FMA_CE_FPMAX_GIDX_S:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %.2f operand 2: %li\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        operand_float_1,
                        post_desc.second_operand);
                 break;

             case GNI_FMA_CE_FPADD_S:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %.2f operand 2: %.2f\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        operand_float_1,
                        operand_float_2);
                 break;

             case GNI_FMA_CE_FPMIN_LIDX:
             case GNI_FMA_CE_FPMAX_LIDX:
             case GNI_FMA_CE_FPMIN_GIDX:
             case GNI_FMA_CE_FPMAX_GIDX:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %.2lf operand 2: %li\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        operand_double_1,
                        post_desc.second_operand);
                 break;

             case GNI_FMA_CE_FPADD:
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma               cmd: 0x%04x, EP: %p, ce_red_id: %li, operand 1: %.2lf operand 2: %.2lf\n",
                        uts_info.nodename, rank_id,
                        post_desc.ce_cmd, leaf_ep,
                        post_desc.ce_red_id,
                        operand_double_1,
                        operand_double_2);
                 break;

             default:
                 break;
             }
        }

        /*
         * Post the request.
         */

        status = GNI_PostFma(leaf_ep, &post_desc);
        if (status != GNI_RC_SUCCESS) {
            if ((ce_command == GNI_FMA_CE_FPADD) && (status == GNI_RC_ILLEGAL_OP)) {
                fprintf(stdout, "[%s] Rank: %4i The GNI_FMA_CE_FPADD CE command is currently not supported.\n",
                        uts_info.nodename, rank_id);
            } else {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_PostFma       ce request ERROR status: %s (%d) cmd: 0x%04x\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status,
                        post_desc.ce_cmd);
            }

            INCREMENT_FAILED;
            goto BARRIER_WAIT;
        }

        INCREMENT_PASSED;

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_PostFma       ce request cmd: 0x%04x was successful\n",
                    uts_info.nodename, rank_id, post_desc.ce_cmd);
        }

        /*
         * Check the completion queue to verify that the data request has
         * been sent.  The local completion queue needs to be checked and
         * events to be removed so that it does not become full and cause
         * succeeding calls to PostFma to fail.
         */

        status = GNI_CqWaitEvent(cq_handle, timeout, &current_event);
        if (status == GNI_RC_SUCCESS) {
            /*
             * An event was received.
             *
             * Complete the event, which removes the current event's post
             * descriptor from the event queue.
             */

            status = GNI_GetCompleted(cq_handle, current_event, &completed_post_desc_ptr);
            if (status == GNI_RC_SUCCESS) {
                if (v_option > 1) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_GetCompleted  cmd: 0x%04x, event: 0x%016lx\n",
                            uts_info.nodename, rank_id, completed_post_desc_ptr->ce_cmd,
                            current_event);
                }

                /*
                 * Wait for the results to be gathered.
                 */
                while ((status = GNI_CeCheckResult(result_buffer, 1)) == GNI_RC_NOT_DONE) {
                    sched_yield();
                }

                if (status != GNI_RC_SUCCESS && status != GNI_RC_TRANSACTION_ERROR) {
                    fprintf(stdout,
                            "[%s] Rank: %4i GNI_CeCheckResult ERROR status: %s (%d)\n",
                            uts_info.nodename, rank_id, gni_err_str[status], status);
                    INCREMENT_FAILED;
                } else {
                    switch (completed_post_desc_ptr->ce_cmd) {
                    case GNI_FMA_CE_AND_S:
                    case GNI_FMA_CE_AND:
                    case GNI_FMA_CE_OR_S:
                    case GNI_FMA_CE_OR:
                    case GNI_FMA_CE_XOR_S:
                    case GNI_FMA_CE_XOR:
                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: 0x%016lx, result 2: 0x%016lx\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    result_buffer->result1,
                                    result_buffer->result2);
                        }
                        break;

                    case GNI_FMA_CE_IMIN_LIDX_S:
                    case GNI_FMA_CE_IMAX_LIDX_S:
                    case GNI_FMA_CE_IADD_S:
                    case GNI_FMA_CE_IMIN_GIDX_S:
                    case GNI_FMA_CE_IMAX_GIDX_S:
                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %i result 2: %i\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    (int32_t) result_buffer->result1,
                                    (int32_t) result_buffer->result2);
                        }
                        break;

                    case GNI_FMA_CE_IMIN_LIDX:
                    case GNI_FMA_CE_IMAX_LIDX:
                    case GNI_FMA_CE_IADD:
                    case GNI_FMA_CE_IMIN_GIDX:
                    case GNI_FMA_CE_IMAX_GIDX:
                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %li result 2: %li\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    (int64_t) result_buffer->result1,
                                    (int64_t) result_buffer->result2);
                        }
                        break;

                    case GNI_FMA_CE_FPMIN_LIDX_S:
                    case GNI_FMA_CE_FPMAX_LIDX_S:
                    case GNI_FMA_CE_FPMIN_GIDX_S:
                    case GNI_FMA_CE_FPMAX_GIDX_S:
                        result_1_ptr = &(result_buffer->result1);
                        result_float_1 = *((float *) result_1_ptr);

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %.2f result 2: %li\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    result_float_1,
                                    result_buffer->result2);
                        }
                        break;

                    case GNI_FMA_CE_FPADD_S:
                        result_1_ptr = &(result_buffer->result1);
                        result_float_1 = *((float *) result_1_ptr);
                        result_2_ptr = &(result_buffer->result2);
                        result_float_2 = *((float *) result_2_ptr);

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %.2f result 2: %.2f\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    result_float_1,
                                    result_float_2);
                        }
                        break;

                    case GNI_FMA_CE_FPMIN_LIDX:
                    case GNI_FMA_CE_FPMAX_LIDX:
                    case GNI_FMA_CE_FPMIN_GIDX:
                    case GNI_FMA_CE_FPMAX_GIDX:
                        result_1_ptr = &(result_buffer->result1);
                        result_double_1 = *((double *) result_1_ptr);

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %.2lf result 2: %li\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    result_double_1,
                                    result_buffer->result2);
                        }
                        break;

                    case GNI_FMA_CE_FPADD:
                        result_1_ptr = &(result_buffer->result1);
                        result_double_1 = *((double *) result_1_ptr);
                        result_2_ptr = &(result_buffer->result2);
                        result_double_2 = *((double *) result_2_ptr);

                        if (v_option) {
                            fprintf(stdout,
                                    "[%s] Rank: %4i GNI_CeCheckResult         cmd: 0x%04x, result 1: %.2lf result 2: %.2lf\n",
                                    uts_info.nodename, rank_id,
                                    completed_post_desc_ptr->ce_cmd,
                                    result_double_1,
                                    result_double_2);
                        }
                        break;

                    default:
                        break;
                    }

                    /*
                     * Verify that CE red id is for the CE request sent.
                     */
                    if (gni_ce_res_get_red_id(result_buffer) != post_desc.ce_red_id) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_CeCheckResult res_id ERROR expected: 0x%lx, received: 0x%lx\n",
                                uts_info.nodename, rank_id, post_desc.ce_red_id, gni_ce_res_get_red_id(result_buffer));
                        INCREMENT_FAILED;

                    /*
                     * Verify that CE status is for the CE request is successful.
                     */
                    } else if (!gni_ce_res_status_ok(result_buffer)) {
                        fprintf(stdout,
                                "[%s] Rank: %4i GNI_CeCheckResult ce_res_id: %li, status ERROR received: 0x%lx\n",
                                uts_info.nodename, rank_id, post_desc.ce_red_id, gni_ce_res_status_ok(result_buffer));
                        INCREMENT_FAILED;
                    } else if (gni_ce_res_get_fpe(result_buffer)) {
                        assert(status == GNI_RC_TRANSACTION_ERROR);
                    } else {
                        /*
                         * Verify that the gathered CE results are what was expected.
                         */

                        switch (completed_post_desc_ptr->ce_cmd) {
                        case GNI_FMA_CE_AND_S:
                        case GNI_FMA_CE_AND:
                        case GNI_FMA_CE_OR_S:
                        case GNI_FMA_CE_OR:
                        case GNI_FMA_CE_XOR_S:
                        case GNI_FMA_CE_XOR:
                            if ((result_buffer->result1 != expected_result_1.u64) ||
                                (result_buffer->result2 != expected_result_2.u64)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: 0x%016lx, result 2: 0x%016lx "
                                        "  received result 1: 0x%016lx, result 2: 0x%016lx\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_result_1.u64,
                                        expected_result_2.u64,
                                        result_buffer->result1,
                                        result_buffer->result2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_IMIN_LIDX_S:
                        case GNI_FMA_CE_IMAX_LIDX_S:
                        case GNI_FMA_CE_IADD_S:
                        case GNI_FMA_CE_IMIN_GIDX_S:
                        case GNI_FMA_CE_IMAX_GIDX_S:
                            if ((result_buffer->result1 != expected_result_1.u64) ||
                                (result_buffer->result2 != expected_result_2.u64)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %i (0x%016lx), result 2: %i\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_result_1.i32,
                                        expected_result_1.u64,
                                        expected_result_2.i32);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %i (0x%016lx), result 2: %i\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        (int32_t) result_buffer->result1,
                                        result_buffer->result1,
                                        (int32_t) result_buffer->result2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_IMIN_LIDX:
                        case GNI_FMA_CE_IMAX_LIDX:
                        case GNI_FMA_CE_IADD:
                        case GNI_FMA_CE_IMIN_GIDX:
                        case GNI_FMA_CE_IMAX_GIDX:
                            if ((result_buffer->result1 != expected_result_1.u64) ||
                                (result_buffer->result2 != expected_result_2.u64)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %li (0x%016lx), result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_result_1.i64,
                                        expected_result_1.u64,
                                        expected_result_2.i64);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %li (0x%016lx), result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        (int64_t) result_buffer->result1,
                                        result_buffer->result1,
                                        result_buffer->result2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_FPMIN_LIDX_S:
                        case GNI_FMA_CE_FPMAX_LIDX_S:
                        case GNI_FMA_CE_FPMIN_GIDX_S:
                        case GNI_FMA_CE_FPMAX_GIDX_S:
                            if ((result_float_1 != expected_float_1) ||
                                (result_buffer->result2 != expected_result_2.u64)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %.2f, result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_float_1,
                                        expected_result_2.u64);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %.2f, result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        result_float_1,
                                        result_buffer->result2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_FPADD_S:
                            if ((result_float_1 != expected_float_1) ||
                                (result_float_2 != expected_float_2)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %.2f, result 2: %.2f\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_float_1,
                                        expected_float_2);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %.2f, result 2: %.2f\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        result_float_1,
                                        result_float_2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_FPMIN_LIDX:
                        case GNI_FMA_CE_FPMAX_LIDX:
                        case GNI_FMA_CE_FPMIN_GIDX:
                        case GNI_FMA_CE_FPMAX_GIDX:
                            if ((result_double_1 != expected_double_1) ||
                                (result_buffer->result2 != expected_result_2.u64)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %.2lf, result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_double_1,
                                        expected_result_2.u64);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %.2lf, result 2: %li\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        result_double_1,
                                        result_buffer->result2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        case GNI_FMA_CE_FPADD:
                            if ((result_double_1 != expected_double_1) ||
                                (result_double_2 != expected_double_2)) {
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: expected result 1: %.2lf, result 2: %.2lf\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        expected_double_1,
                                        expected_double_2);
                                fprintf(stdout,
                                        "[%s] Rank: %4i ERROR cmd: 0x%04x, CE data: received result 1: %.2lf, result 2: %.2lf\n",
                                        uts_info.nodename, rank_id,
                                        completed_post_desc_ptr->ce_cmd,
                                        result_double_1,
                                        result_double_2);
                                INCREMENT_FAILED;
                            } else {
                                INCREMENT_PASSED;
                            }
                            break;

                        default:
                             break;
                        }
                    }
                }
            } else {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_GetCompleted  ERROR status: %s (%d)\n",
                        uts_info.nodename, rank_id, gni_err_str[status], status);

                INCREMENT_FAILED;
            }
        } else {
            /*
             * An error occurred while receiving the event.
             *
             * NOTE: if you receive a GNI_RC_TRANSACTION_ERROR error,
             *       in the console log you could see a message with
             *       A_STATUS_CE_JOIN_CHILD_INV or A_STATUS_CE_REDUCTION_ID_MISMATCH
             *       error.
             *
             *       If A_STATUS_CE_JOIN_CHILD_INV error appears, then verify
             *       that you have used the correct child ids on the GNI_EpSetCeAttr()
             *       GNI_CeConfigure() calls.
             *
             *       If A_STATUS_CE_REDUCTION_ID_MISMATCH error appears, then verify
             *       that all of the processes are using the identical ce_red_id in
             *       the post descriptor for the GNI_PostFma() call.
             */

            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CqWaitEvent   ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_FAILED;
        }
    }

    BARRIER_WAIT:
    /*
     * Wait for all the processes to finish before we clean up and exit.
     */

    rc = PMI_Barrier();
    assert(rc == PMI_SUCCESS);

    sleep(1);

    if (node_leader == 1) {
        pthread_cancel(thread_id);
        pthread_join(thread_id, NULL);

        if (v_option > 1) {
            fprintf(stdout, "[%s] Rank: %4i pthread_cancel   ce_err_handler thread_id: %p\n",
                    uts_info.nodename, rank_id, (void *) thread_id);
        }
    }

    if ((only_leaders == 0) || (node_leader == 1)) {
        /*
         * Deregister the memory associated for the receive buffer with the NIC.
         *     nic_handle is our NIC handle.
         *     result_memory_handle is the handle for this memory region.
         */

        status = GNI_MemDeregister(nic_handle, &result_memory_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_MemDeregister result_buffer ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
        } else {
            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_MemDeregister result_buffer     NIC: %p\n",
                        uts_info.nodename, rank_id, nic_handle);
            }

            /*
             * Free allocated memory.
             */

            free(result_buffer);
        }
    }

  EXIT_CE:

    /*
     * Destroy the VCE Channel.
     */

    if (node_leader == 1) {
        status = GNI_CeDestroy(ce_handle);
        if (status != GNI_RC_SUCCESS) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeDestroy     ERROR status: %s (%d)\n",
                    uts_info.nodename, rank_id, gni_err_str[status], status);
            INCREMENT_ABORTED;
        }

        if (v_option > 2) {
            fprintf(stdout,
                    "[%s] Rank: %4i GNI_CeDestroy     CE: %p\n",
                    uts_info.nodename, rank_id, ce_handle);
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
        if ((only_leaders == 0) || ((node_leader == 1) && (node_leaders[i] == 1))) {
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

            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpUnbind      remote rank: %4i EP: %p\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i]);
            }

            /*
             * You must do an EpDestroy for each endpoint pair.
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

            if (v_option > 2) {
                fprintf(stdout,
                        "[%s] Rank: %4i GNI_EpDestroy     remote rank: %4i EP: %p\n",
                        uts_info.nodename, rank_id, i,
                        endpoint_handles_array[i]);
            }
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
    } else if (v_option > 2) {
        fprintf(stdout, "[%s] Rank: %4i GNI_CqDestroy     local\n",
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
    } else if (v_option > 2) {
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
