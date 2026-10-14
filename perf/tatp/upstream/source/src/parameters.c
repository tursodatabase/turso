/**********************************************************************\
**  source       * parameters.c
**  description  * 
**                 
**
**  Copyright (c) Solid Information Technology Ltd. 2004, 2010
**
**  This program is made available under the terms of the Common Public
**  License 1.0 as published by the Open Source Initiative (OSI).
**  http://www.opensource.org/licenses/cpl1.0.php
**
\**********************************************************************/

#include <string.h>
#include <stdlib.h>
#ifdef WIN32
#else
#include <unistd.h>
#endif
#include "parameters.h"
#include "remcontrol.h"

/*##********************************************************************** \
 *
 *      readTDF
 *
 * Reads one TDF file and stores the content to dedicated data structure.
 *
 * Parameters:
 *      tdffilename
 *          The name of the tdf file
 *
 *      tdf
 *          The TDF data structure
 *
 *      bmrs
 *          The benchmark data structure
 *
 *      num_of_bmr
 *          The number of benchmarks defined in the TDF.
 *			Note that a populaton directive is counted as
 *			one benchmark in this case.
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int readTDF(char *tdffilename, struct tdfs *tdf, struct bmr *bmrs[],
            int *num_of_bmr)
{
        FILE *fTDF = NULL;
        char line[256], msg[256];
        /* flag to indicate the first line in the TDF */
        int firstline;
        int retCode, transaction_num, client_distr_num;
        /* Flag type variables */
        int parsing_transaction_mix, parsing_client_distribution;
        enum tdf_file_sections tdf_file_section;
        int error, err;
        int repeats;

        parsing_transaction_mix = 0;
        parsing_client_distribution = 0;
        transaction_num = 0;
        client_distr_num = 0;
        error = err = 0;

        if (openFile(&fTDF, tdffilename) != 0) {
            sprintf(msg, "Cannot open TDF %s", tdffilename);
            writeLog('E', msg);
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        sprintf(msg, "Processing Test Definition File '%s'", tdffilename);
        writeLog('I', msg);

        initTDFDataStruct(tdf);

        /* Save host name to the tdf struct */
        gethostname(tdf->control_host, W_L);
        /* Statistics and Main Control are in the same host */
        strncpy(tdf->statistics_host, tdf->control_host, W_L);

        firstline = 1;
        tdf_file_section = NONE;
        /* Iterate through the lines in TDF */
        while (readFileLine(fTDF, line, 256) != -1) {
            if (err == E_ERROR) {
                /* Handle the rest of the TDF and then exit with error */
                error = 1;
            }
            if (firstline) {
                firstline = 0;
                if (strncmp(line, "//tatp_tdf", 10) != 0) {
                    writeLog('E', "TDF has wrong or no identification line");
                    fclose(fTDF);
                    /* Not fatal, because we can handle the next TDF */
                    return E_ERROR;
                }
                continue; /* to next line */
            }
            /* Remove potential comments from the line (everything
             * after //) */
            removeComment(line);

            /* Skip blank lines */
            if (*line == '\0') {
                continue; /* to next line */
            }
            writeLog('D', line);

            /* Check if we hit the section change tag */
            retCode = isTDFSectionMarker(line, &tdf_file_section);
            if (retCode == 1) {
                /* Section change -> move to next line of TDF */
                if (parsing_transaction_mix) {
                    message('E',
                            "TDF section marker encountered while parsing "
                            "a transaction mix");
                    fclose(fTDF);
                    return E_ERROR;
                }
                if (parsing_client_distribution) {
                    message('E',
                            "TDF section marker encountered while parsing "
                            "a client distribution");
                    fclose(fTDF);
                    return E_ERROR;
                }
                continue;
            }
            else if (retCode == -1) {
                message('E', "TDF section marker error");
                fclose(fTDF);
                /* Not fatal, because we can handle the next TDF */
                return E_ERROR;
            }
            switch (tdf_file_section) {
                case SESSION_PARAMETERS:
                    err = parseTDFSessionParameter(line, tdf);
                    continue;
                case POPULATION_PARAMETERS:
                    err = parseTDFPopulationParameter(line, tdf);
                    continue;
                case TEST_PARAMETERS:
                    err = parseTDFTestParameter(line, tdf);
                    continue;
                case TRANSACTION_MIXES:
                    err = parseTDFTransactionMixes(line, tdf,
                                                   &parsing_transaction_mix,
                                                   &transaction_num);
                    continue;
                case DATABASE_CLIENT_DISTRIBUTIONS:
                    err = parseTDFLoadDistributions(
                            line, tdf,
                            &parsing_client_distribution, &client_distr_num);
                    continue;
                case TEST_SEQUENCE:
		    repeats = 0;
		    do {
		      err = parseTDFTestSequence(line, tdf, bmrs, num_of_bmr, &repeats);
		    } while (repeats > 0);
		    continue;
                case NONE:
                    message('E', "A TDF directive before any section "
                            "marker in TDF");
                    err = E_ERROR;
                    continue;
            } /* switch (tdf_file_section) */

        } /* For the next line in TDF */

        fclose(fTDF);

        if (error == 1 || err == E_ERROR ||
			checkTDFData(tdf, bmrs, *num_of_bmr, tdffilename) == E_ERROR) {
            sprintf(msg, "Errors in TDF (%s)", tdffilename);
            writeLog('E', msg);
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      initTDFDataStruct
 *
 * Initializes the TDF data structure. Use default values when appropriate.
 *
 * Parameters:
 *      tdf
 *          TDF data structure
 *
 * Return value:
 *      none
 */
void initTDFDataStruct(struct tdfs *tdf)
{
        int i, j;
        /*  Session parameters */
        tdf->session_name[0] = '\0';
        tdf->author[0] = '\0';
        tdf->comments[0] = '\0';
        /* Population parameters */
        tdf->subscribers = DEFAULT_NUM_OF_SUBSCRIBERS;
        tdf->serial_keys = DEFAULT_SERIAL_KEY_MODE;
        tdf->commitblock_size = DEFAULT_COMMIT_BLOCK_SIZE;
        tdf->post_population_delay = DEFAULT_POST_POPULATION_DELAY;
        /* Test parameters */
        tdf->warm_up_duration = DEFAULT_WARM_UP_DURATION;
        tdf->run_duration = DEFAULT_RUN_DURATION;
        tdf->uniform = DEFAULT_UNIFORM;
        tdf->throughput_resolution = DEFAULT_THROUGHPUT_RESOLUTION;
	tdf->repeats = 1;

        /* Transaction mixes */
        for (i = 0; i < MAX_NUM_OF_TRANSACTION_MIXES; i++) {
            tdf->tr_mixes[i].name[0] = '\0';
            for (j = 0; j < MAX_NUM_OF_TRANSACTIONS; j++) {
                tdf->tr_mixes[i].tr_props[j].transact[0] = '\0';
                tdf->tr_mixes[i].tr_props[j].prob = 0;
            }
        }
        tdf->num_of_tr_mixes = 0;
        /* Load distributions */
        for (i = 0; i < MAX_NUM_OF_CLIENT_DISTRIBUTIONS; i++) {
            tdf->client_distributions[i].name[0] = '\0';
            for (j = 0; j < MAX_NUM_OF_REMOTE_COMPUTERS; j++) {
                tdf->client_distributions[i].
                    rem_loads[j].remControls_index = 0;
                tdf->client_distributions[i].
                    rem_loads[j].remLoad = 0;
                tdf->client_distributions[i].
                    rem_loads[j].remLoadProcesses = DEFAULT_CLIENT_PROCESSES;
            }
        }
        tdf->num_of_client_distributions = 0;
}

/*##**********************************************************************\
 *
 *      checkTDFData
 *
 * Validates the TDF data that was read from the TDF file.
 *
 * Parameters:
 *      tdf 
 *          TDF data structure
 *
 *      bmrs
 *			Benchmark run time data structure
 *
 *      num_of_bmr
 *			Number of bmrs in the structure
 *
 *      tdffilename
 *			TDF file name
 *
 * Return value:
 *       0 - success
 *      !0 - data not ok
 */
int checkTDFData(struct tdfs *tdf, struct bmr *bmrs[], int num_of_bmr,
                 char *tdffilename)
{
        int i, j, err;
        char msg[256];
        err = 0;

        /* Population parameters */
        if (tdf->subscribers < 1) {
            sprintf(msg, "TDF (%s) Population parameter: 'subscribers' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->commitblock_size < 0) {
            sprintf(msg, "TDF (%s) Population parameter: 'commit_block_rows' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->post_population_delay < 0) {
            sprintf(msg, "TDF (%s) Population parameter: "
                    "'post_population_delay' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        /* Test parameters */
        if (tdf->warm_up_duration < 0) {
            sprintf(msg, "TDF (%s) Test parameter: 'warm_up_duration' "
                    "has to be >=0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->run_duration < 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'run_duration' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->repeats < 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'repeats' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
	}
        if (tdf->uniform != 0
            && tdf->uniform != 1) {
            sprintf(msg, "TDF (%s) Test parameter: 'uniform' "
                    "has to be one of [0, 1]",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        if (tdf->throughput_resolution < 1) {
            sprintf(msg, "TDF (%s) Test parameter: "
                    "'throughput_resolution' "
                    "has to be >0",
                    tdffilename);
            writeLog('E', msg);
            err = 1;
        }
        /* Check the run time parameters also */
        for (i = 0; i < num_of_bmr; i++) {
            if (bmrs[i]->cmd_type == POPULATE
                || bmrs[i]->cmd_type == POPULATE_INCREMENTALLY)
                || bmrs[i]->cmd_type == POPULATE_CONDITIONALLY) {
                if (bmrs[i]->subscribers < 1) {
                    sprintf(msg, "TDF (%s) 'populate' command: "
                            "'subscribers' "
                            "has to be >0",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
                if (bmrs[i]->commitblock_size < 0) {
                    sprintf(msg, "TDF (%s) 'populate' command: "
                            "'commit_block_rows' "
                            "has to be >=0",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
                if (bmrs[i]->min_subscriber_id < 1) {
                    sprintf(msg, "TDF (%s) 'populate' command: "
                            "'min_subscriber_id' "
                            "has to be >=1",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
                if (bmrs[i]->post_population_delay < 0) {
                    sprintf(msg, "TDF (%s) 'populate' command: "
                            "'post_population_delay' has to be >=0",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
            }
            else if (bmrs[i]->cmd_type == RUN) {
                if (bmrs[i]->run_duration < 1) {
                    sprintf(msg, "TDF (%s) 'run' command: 'run_duration' "
                            "has to be >0",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
		if (bmrs[i]->repeats < 1) {
		    sprintf(msg, "TDF (%s) 'run' command: 'repeats' "
			    "has to be >0",
			    tdffilename);
		    writeLog('E', msg);
		    err = 1;
		}
                if (bmrs[i]->warm_up_duration < 0) {
                    sprintf(msg, "TDF (%s) 'run' command: 'warm_up_duration' "
                            "has to be >=0",
                            tdffilename);
                    writeLog('E', msg);
                    err = 1;
                }
                /* Check that transaction mix reference exist */
                for (j = 0; j < tdf->num_of_tr_mixes; j++) {
                    if (strncmp(bmrs[i]->transaction_mix_str,
                                tdf->tr_mixes[j].name, W) == 0) {
                        /* Transaction mix with the given name was found  */
                        bmrs[i]->transaction_mix_ind = j;
                        break;
                    }
                }
                if (j == tdf->num_of_tr_mixes) {
                    /* A transaction mix with a given name is not defined */
                    sprintf(msg, "TDF (%s) 'run' command: unresolved "
                            "transaction mix name '%s'",
                            tdffilename, bmrs[i]->transaction_mix_str);
                    writeLog('E', msg);
                    err = 1;
                }
                /* Check that load distribution reference exist */
                for (j = 0; j < tdf->num_of_client_distributions; j++) {
                    if (strncmp(bmrs[i]->client_distribution_str,
                                tdf->client_distributions[j].name, W) == 0) {
                        /* Client load distribution with the given name was
                           found  */
                        bmrs[i]->client_distribution_ind = j;
                        break;
                    }
                }
                if (j == tdf->num_of_client_distributions) {
                    /* A client load distribution with a given name
                       is not defined */
                    sprintf(msg, "TDF (%s) 'run' command: "
                            "unresolved db client "
                            "distribution name '%s'",
                            tdffilename, bmrs[i]->client_distribution_str);
                    writeLog('E', msg);
                    err = 1;
                }
            }
        }
        if (err == 1) {
            /* Not fatal, because we can handle the next TDF */
            return E_ERROR;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      isTDFSectionMarker
 *
 * Checks if a character string given as an argument
 * has a TDF section starter tag.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf_file_section
 *          the name of the tdf section
 *
 * Return value:
 *      1  - tag found
 *      0  - tag not found
 *     <0  - error (section tag is not the only thing in the string)
 */
int isTDFSectionMarker(const char *line,
                       enum tdf_file_sections *tdf_file_section)
{
        if (strstr(line, "[Session parameters]")) {
            if (strlen(line) > 20) return -1;
            *tdf_file_section = SESSION_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Population parameters]")) {
            if (strlen(line) > 23) return -1;
            *tdf_file_section = POPULATION_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Test parameters]")) {
            if (strlen(line) > 17) return -1;
            *tdf_file_section = TEST_PARAMETERS;
            return 1;
        }
        else if (strstr(line, "[Transaction mixes]")) {
            if (strlen(line) > 19) return -1;
            *tdf_file_section = TRANSACTION_MIXES;
            return 1;
        }
        else if (strstr(line, "[Database client distributions]")) {
            if (strlen(line) > 31) return -1;
            *tdf_file_section = DATABASE_CLIENT_DISTRIBUTIONS;
            return 1;
        }
        else if (strstr(line, "[Test sequence]")) {
            if (strlen(line) > 15) return -1;
            *tdf_file_section = TEST_SEQUENCE;
            return 1;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      parseTDFSessionParameter
 *
 * Parses one line from the TDF file that is within the
 * SESSION_PARAMETERS section.
 *
 * Parameters:
 *      line 
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFSessionParameter(const char *line, struct tdfs *tdf)
{
        char msg[256];
        if ((extractStringKeyword(line, "session_name",
                                  tdf->session_name, W_L))
            != E_NO_KEYWORD) {
			return 0;
        }
        if ((extractStringKeyword(line, "author", tdf->author, W))
            != E_NO_KEYWORD) {
			return 0;
        }
        if ((extractIntKeyword(line, "throughput_resolution",
                               &tdf->throughput_resolution)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractStringKeyword(line, "comments", tdf->comments, W_L))
            != E_NO_KEYWORD) {
			return 0;
        }
        sprintf(msg,
                "Unknown directive in [Session parameters] "
                "section in TDF (%s).",
                line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFPopulationParameter
 *
 * Parses one line from the TDF file that is within the
 * POPULATION_PARAMETERS section.
 *
 * Parameters:
 *      line 
 *          the character string
 *
 *      tdf 
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFPopulationParameter(const char *line, struct tdfs *tdf)
{
        char msg[256];
        char value[W];
        if ((extractIntKeyword(line, "subscribers", &tdf->subscribers))
            != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractStringKeyword(line, "serial_keys", value, W))
            != E_NO_KEYWORD) {
			if (strncmp(value, "yes", 3) == 0) {
				tdf->serial_keys = 1;
				return 0;
			}
			else if (strncmp(value, "no", 2) == 0) {
				tdf->serial_keys = 0;
				return 0;
			}
			else {
				sprintf(msg, "Wrong value for 'serial_keys' in "
                        "[Population parameters] section in TDF.");
				message('E', msg);
				return E_ERROR;
			}
        }
        if ((extractStringKeyword(line, "check_targetdb", value, W))
            != E_NO_KEYWORD) {
			if (strncmp(value, "yes", 3) == 0) {
				tdf->check_targetdb = 1;
				return 0;
			}
			else if (strncmp(value, "no", 2) == 0) {
				tdf->check_targetdb = 0;
				return 0;
			}
			else {
				sprintf(msg, "Wrong value for 'check_targetdb' in "
                        "[Population parameters] section in TDF.");
				message('E', msg);
				return E_ERROR;
			}
        }
        if ((extractIntKeyword(line, "commit_block_rows",
                               &tdf->commitblock_size)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "post_population_delay",
                               &tdf->post_population_delay)) != E_NO_KEYWORD) {
            return 0;
        }
        sprintf(msg,
                "Unknown directive (or value) in "
                "[Population parameters] section in TDF (%s).",
                line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFTestParameter
 *
 * Parses one line from the TDF file that is within the
 * TEST_PARAMETERS section.
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 * Return value:
 *      0 - success
 *     !0 - error 
 */
int parseTDFTestParameter(const char *line, struct tdfs *tdf)
{
        char msg[256];
        char buf[W];
        if ((extractIntKeyword(line, "warm_up_duration",
                               &tdf->warm_up_duration)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "run_duration",
                               &tdf->run_duration)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractIntKeyword(line, "repeats",
                               &tdf->repeats)) != E_NO_KEYWORD) {
            return 0;
        }
        if ((extractStringKeyword(line, "check_targetdb", buf, W))
            != E_NO_KEYWORD) {
            if (strncmp(buf, "yes", 3) == 0) {
                tdf->check_targetdb = 1;
                return 0;
            }
            else if (strncmp(buf, "no", 2) == 0) {
                tdf->check_targetdb = 0;
                return 0;
            }
            else {
                sprintf(msg, "Wrong value for 'check_targetdb' in "
                        "[Test parameters] section in TDF, "
                        "must be \"yes\"/\"no\".");
                message('E', msg);
                return E_ERROR;
            }
        }
        if ((extractStringKeyword(line, "uniform",
                                  buf, W)) != E_NO_KEYWORD) {

            if (strcmp(buf, "yes") == 0) {
                tdf->uniform = 1;
            }
            else if (strcmp(buf, "no") == 0) {
                tdf->uniform = 0;
            }
            else {
                sprintf(msg, "Wrong value for 'uniform' in "
                        "[Test parameters] section in TDF, must be "
                        "\"yes\"/\"no\".");
                message('E', msg);
                return E_ERROR;
            }

            return 0;
        }
        sprintf(msg,
                "Unknown directive in [Test parameters] section in TDF (%s).",
                line);
        message('E', msg);
        return E_ERROR;
}

/*##**********************************************************************\
 *
 *      parseTDFTransactionMixes
 *
 * Parses one line from the TDF file that is within the
 * TRANSACTION_MIXES section.
 *
 * Parameters:
 *      line 
 *          the character string
 *
 *      tdf 
 *          the test definition data structure
 *
 *      parsing_transaction_mix
 *          a flag indicating the parsing status (whether
 *          we are currently parsing a transaction mix or not)
 *
 *      transaction_num
 *          The transaction number within a transaction mix
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFTransactionMixes(const char *line, struct tdfs *tdf,
							 int *parsing_transaction_mix,
							 int *transaction_num) {
        char msg[256];
        int i, propability_count;
        /* regular expressions */
        regex_t *r;
        /* used with regex to locate the string hits */
        int count, starts[100], lengths[100];

        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        if (!(*parsing_transaction_mix)) {
            /* We are not currently parsing a mix */
            /* Try to match for the transaction mix start "name = {" */
            if (!multiMatch(r, (char*)line, "^ *(.*) *= *\\{ *$", &count,
                            starts, lengths)) {
                if (tdf->num_of_tr_mixes >= MAX_NUM_OF_TRANSACTION_MIXES) {
                    sprintf(msg,
                            "More than %d transaction mixes "
                            "defined in TDF (%s).",
                            MAX_NUM_OF_TRANSACTION_MIXES, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Extract the name of the transaction mix */
                strncat(tdf->tr_mixes[tdf->num_of_tr_mixes].name,
                        &line[starts[1]], minimum(lengths[1]-1,W));
                *parsing_transaction_mix = 1;
                *transaction_num = 0;
            }
        }
        else if (!multiMatch(r, (char*)line, "^ *\\} *$", &count,
                             starts, lengths)) {
            /* Leaving the transaction mix block */
            *parsing_transaction_mix = 0;
            /* Check that the propabilities count to 100% */
            propability_count = 0;
            for (i = 0; i < *transaction_num; i++) {
                propability_count +=
                    tdf->tr_mixes[tdf->num_of_tr_mixes].tr_props[i].prob;
            }
            if (propability_count != 100) {
				sprintf(msg,
                        "Probability in the transaction mix %s "
                        "does not sum to 100.",
                        tdf->tr_mixes[tdf->num_of_tr_mixes].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            (tdf->num_of_tr_mixes)++;
        }
        else {
            /* Currently parsing a mix (we are within the { } block) */
            /* Try to match for a transaction name and its probability */
            if (!multiMatch(r, (char*)line, "^ *(.*) +([0-9]+) *$",
                            &count, starts, lengths)) {
                if (*transaction_num >= MAX_NUM_OF_TRANSACTIONS) {
                    sprintf(msg,
                            "More than %d transactions defined "
                            "in a transaction "
                            "mix in TDF (%s).",
                            MAX_NUM_OF_TRANSACTIONS, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                if (count != 3) {
                    sprintf(msg, "Transaction / propability -pair error in "
                            "TDF file (%s)",
                            line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assign the transaction name and its probability */
                strncat(tdf->tr_mixes[tdf->num_of_tr_mixes].
                        tr_props[*transaction_num].transact,
                        &line[starts[1]], minimum(lengths[1],W_L));
                tdf->tr_mixes[tdf->num_of_tr_mixes].
                    tr_props[*transaction_num].prob = atoi(&line[starts[2]]);
                (*transaction_num)++;
            }
        }
        free(r);
        return 0;
}

/*##**********************************************************************\
 *
 *      parseTDFLoadDistributions
 *
 * Parses one line from the TDF file that is within the
 * LOAD_DISTRIBUTIONS section.
 *
 * Parameters:
 *      line 
 *          the character string
 *
 *      tdf
 *          the test definition data structure
 *
 *      parsing_client_distribution 
 *          flag indicating whether we are currently parsing
 *          a client distribution
 *
 *      client_num 
 *          number of the current client distribution
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFLoadDistributions(const char *line, struct tdfs *tdf,
                              int *parsing_client_distribution,
                              int *client_num) {
        char msg[256], client_name[W_L];
        int i, client_count;
        /* regular expressions */
        regex_t *r;
        /* used with regex to locate the string hits */
        int count, starts[100], lengths[100];
        char *strptr = NULL;
        char strClients[W];
        
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        if (!(*parsing_client_distribution)) {
            /* We are not currently parsing a load distribution */
            /* Try to match for the load distribution start "name = {" */
            if (!multiMatch(r, (char*)line, "^ *(.*) *= *\\{ *$",
                            &count, starts, lengths)) {
                if (tdf->num_of_client_distributions
                    >= MAX_NUM_OF_CLIENT_DISTRIBUTIONS) {
                    sprintf(msg,
                            "More than %d load distributions defined "
                            "in TDF (%s).",
                            MAX_NUM_OF_CLIENT_DISTRIBUTIONS, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Extract the name of the load distribution */
                strncat(tdf->client_distributions[
                                tdf->num_of_client_distributions].name,
                        &line[starts[1]], minimum(lengths[1]-1,W));
                *parsing_client_distribution = 1;
                *client_num = 0;
                tdf->client_distributions[tdf->num_of_client_distributions].
                    localLoad = 0;
                tdf->client_distributions[tdf->num_of_client_distributions].
                    localLoadProcesses = DEFAULT_CLIENT_PROCESSES;
            }
        }
        else if (!multiMatch(r, (char*)line, "^ *\\} *$", &count,
                             starts, lengths)) {
            /* Leaving the load distribution block */
            *parsing_client_distribution = 0;
            /* Check that the number of db clients does not exceed
             * MAX_CLIENTS */
            client_count = tdf->client_distributions[
                    tdf->num_of_client_distributions].localLoad;
            if (tdf->client_distributions[
                        tdf->num_of_client_distributions].localLoadProcesses
                > client_count) {
                sprintf(msg,
                        "More client processes (%d) than clients (%d) defined for "
                        "'localhost' in client distribution '%s'",
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].localLoadProcesses,
                        client_count,
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            for (i = 0; i < *client_num; i++) {
                client_count +=
                    tdf->client_distributions[
                            tdf->num_of_client_distributions].
                            rem_loads[i].remLoad;
                if (tdf->client_distributions[
                            tdf->num_of_client_distributions].
                            rem_loads[i].remLoad <
                    tdf->client_distributions[
                            tdf->num_of_client_distributions].
                    rem_loads[i].remLoadProcesses) {
                    sprintf(msg,
                            "More client processes (%d) than clients (%d) "
                            "defined for a Remote Node in client "
                            "distribution '%s'",
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                            rem_loads[i].remLoadProcesses,
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                            rem_loads[i].remLoad,
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].name);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;                    
                }
            }
            if (client_count > MAX_CLIENTS) {
				sprintf(msg,
                        "More than %d database clients defined in %s.",
                        MAX_CLIENTS, tdf->client_distributions[
                                tdf->num_of_client_distributions].name);
				writeLog('E', msg);
				free(r);
				return E_ERROR;
            }
            (tdf->num_of_client_distributions)++;
        }
        else {
            /* Currently parsing a distribution (we are within the { }
             * block) */
            /* Try to match for a client name and its load */
            if ((!multiMatch(r, (char*)line,
                             "^ *([-a-zA-Z0-9\\.]*) +([0-9\\/]+) *$",
                             &count, starts, lengths))
                || (!multiMatch(r, (char*)line,
                                "^ *([-a-zA-Z0-9\\.]*) +([0-9\\/]+) +"
                                "([0-9]+) +([0-9]+) *$",
                                &count, starts, lengths))) {

                if (*client_num >= MAX_NUM_OF_REMOTE_COMPUTERS) {
                    sprintf(msg,
                            "More than %d client computers defined "
                            "in a db client "
                            "distribution in TDF (%s).",
                            MAX_NUM_OF_REMOTE_COMPUTERS, line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                if ((count != 3) && (count != 5)) {
                    sprintf(msg, "Client / load -pair error in TDF file (%s)",
                            line);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assign the client name and its load */
                /* First find appropriate client from client machine
                 * structure */
                client_name[0] = '\0';
                strncat(client_name, &line[starts[1]],
                        minimum(lengths[1],W_L));
                if (strncmp(client_name, "localhost", 9) == 0) {
                    /* Special case when we use the local computer */
                    strClients[0] = '\0';
                    strncat(strClients, &line[starts[2]],
                            minimum(lengths[2], W));
                    strptr = strstr (strClients, "/");

#ifdef ACCELERATOR
		    tdf->client_distributions[tdf->num_of_client_distributions].
		      localLoadProcesses = 1;
#else
                    if (strptr != NULL) {
                        /* specified amount of processes */
                        tdf->client_distributions[tdf->num_of_client_distributions].
                            localLoadProcesses = atoi(strptr+1);
                        *strptr = '\0';
                    }
#endif

                    tdf->client_distributions[tdf->num_of_client_distributions].
                        localLoad = atoi(strClients);
                    
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        min_subs_id = 0;
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        max_subs_id = 0;
                    /* S_ID range */
                    if (count == 5) {
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].
                                    min_subs_id = atoi(&line[starts[3]]);
                        tdf->client_distributions[
                                tdf->num_of_client_distributions].
                                    max_subs_id = atoi(&line[starts[4]]);
                    }
                    free(r);
                    return 0;
                }
                else {
                    for (i = 1; i < MAX_NUM_OF_REMOTE_COMPUTERS; i++) {
                        if (strncmp(client_name,
                                    remControls[i].name, W_L) == 0) {
                            /* then store the index of the machine in
                               the client machine structure */
                            tdf->client_distributions[
                                    tdf->num_of_client_distributions].
                                rem_loads[*client_num].remControls_index = i;
                            break;
                        }
                    }
                }
                if (i == MAX_NUM_OF_REMOTE_COMPUTERS) {
                    printf(msg, "Client computer '%s' used in TDF file is "
                            "not defined",
                            client_name);
                    writeLog('E', msg);
                    free(r);
                    return E_ERROR;
                }
                /* Assign the client load */
                strClients[0] = '\0';
                strncat(strClients, &line[starts[2]],
                        minimum(lengths[2], W));
                strptr = strstr (strClients, "/");

#ifdef ACCELERATOR
		tdf->client_distributions[tdf->num_of_client_distributions].
		  rem_loads[*client_num].remLoadProcesses = 1;
#else
                if (strptr != NULL) {
                    /* specified amount of processes */
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].remLoadProcesses = atoi(strptr+1);
                    *strptr = '\0';
                }                    
#endif
                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].remLoad = atoi(strClients);
                                
                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].min_subs_id=0;               
                tdf->client_distributions[tdf->num_of_client_distributions].
                    rem_loads[*client_num].max_subs_id=0;
                /* S_ID range */
                if (count == 5) {
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].min_subs_id = atoi(
                                                             &line[starts[3]]);
                    tdf->client_distributions[tdf->num_of_client_distributions].
                        rem_loads[*client_num].max_subs_id = atoi(
                                                             &line[starts[4]]);
                }
                (*client_num)++;   
            } else {
                sprintf(msg,
                        "Unknown row '%s' in database client distribution",
                        line);
                writeLog('E', msg);
                free(r);
                return E_ERROR;
            }
        }
        free(r);
        return 0;
}

/*##********************************************************************** \
 *
 *      parseTDFTestSequence
 *
 * Parses one line from the TDF file that is within the TEST_SEQUENCE
 * section. Two parameters are expected: transaction_mix and
 * database_client_distribution.  The 'test_parameters' may also exist
 * (optional). Each 'test_parameter' not given is assigned the value
 * given in the [Test parameters] section (or default value if the
 * parameters is not defined at all).
 *
 * Parameters:
 *      line
 *          the character string
 *
 *      tdf 
 *          the test definition data structure
 *
 *      bm_run 
 *          benchmark run(s) data structure
 *
 *      num_of_bmr
 *          number of benchmark runs defined
 *    
 *      repeats
 *          number of repeats counter
 *
 * Return value:
 *      0 - success
 *     !0 - error
 */
int parseTDFTestSequence(char *line, struct tdfs *tdf,
			 struct bmr *bm_run[], int *num_of_bmr, int *repeats)
{
        char msg[256], *c;
        char strValue[W];
        int tmp_num_of_bmr;
        enum test_sequence_cmd_type operationType;
        if (*num_of_bmr >= MAX_BM) {
            sprintf(msg, "More than %d commands in the [Test sequence] "
                    "section in TDF (%s).", MAX_BM, line);
            message('E', msg);
            return E_ERROR;
        }
        /* Reserve memory for a benchmark run structure */
        bm_run[*num_of_bmr] = (struct bmr*)malloc(sizeof(struct bmr));
        if (!bm_run[*num_of_bmr]) {
            message('E', "Could not reserve memory for benchmark data "
                    "structure");
            return E_ERROR;
        }
        /* Set inital values for some of the run parameters */
        initBMRunParameters(tdf, bm_run[*num_of_bmr]);

        for (c = line; *c == ' '; c++) ;
        digestBasicOperationType(c, &operationType);
        switch(operationType) {
            case POPULATE:
            case POPULATE_CONDITIONALLY:
            case POPULATE_INCREMENTALLY:
                bm_run[*num_of_bmr]->cmd_type = operationType;
                bm_run[*num_of_bmr]->test_run_name[0] = '\0';

                if (!strstr(line, "database_client_distribution")) {
                    sprintf(msg, "Database client distribution not defined "
                            "for 'populate' command in TDF");
                    message('E', msg);
                    return E_ERROR;
                }
                /* Extract the keywords from the command */
                extractIntKeyword(line, "subscribers",
                                  &(bm_run[*num_of_bmr]->subscribers));
                if (extractStringKeyword(line, "serial_keys",
                                         strValue, W) == 0) {
                    if (strncmp(strValue, "yes", 3) == 0) {
                        bm_run[*num_of_bmr]->serial_keys = 1;
                    }
                    else if (strncmp(strValue, "no", 2) == 0) {
                        bm_run[*num_of_bmr]->serial_keys = 0;
                    }
                    else {
                        sprintf(msg, "Unknown value in table populate command "
                                "in TDF.");
                        message('E', msg);
                        return E_ERROR;
                    }
                }
                extractStringKeyword(line, "name",
                                     bm_run[*num_of_bmr]->test_run_name, W_L); 
                extractIntKeyword(line, "min_subscriber_id",
                                  &(bm_run[*num_of_bmr]->min_subscriber_id));
                extractIntKeyword(line, "commit_block_rows",
                                  &(bm_run[*num_of_bmr]->commitblock_size));
                extractIntKeyword(line, "post_population_delay",
                                  &(bm_run[*num_of_bmr]->
                                    post_population_delay));
                extractStringKeyword(line, "database_client_distribution",
                                     bm_run[*num_of_bmr]->
                                     client_distribution_str, W_L);
                if (!isEmptyBuf(line, strlen(line))) {
                    sprintf(msg, "Unknown content (%s) in table populate "
                            "command in TDF.",
                            line);
                    message('E', msg);
                    return E_ERROR;
                }
                break;
            case RUN:
                if (!strstr(line, "transaction_mix")) {
                    sprintf(msg, "Transaction mix not defined for 'run' "
                            "command in TDF");
                    message('E', msg);
                    return E_ERROR;
                }
                if (!strstr(line, "database_client_distribution")) {
                    sprintf(msg, "Database client distribution not "
                            "defined for 'run' command in TDF");
                    message('E', msg);
                    return E_ERROR;
                }

                bm_run[*num_of_bmr]->test_run_name[0] = '\0';   
                bm_run[*num_of_bmr]->cmd_type = RUN;

                /* Verify the 'subscribers' value. If we had one or many
                   'populate' command(s) in the test sequence before this RUN
                   command, then use the 'subscribers' from the latest
                   'populate' command. */
                tmp_num_of_bmr = (*num_of_bmr) - 1;
                while (tmp_num_of_bmr >= 0) {
                    if ( (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE)
                         || (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE_CONDITIONALLY)
                         || (bm_run[tmp_num_of_bmr]->cmd_type == POPULATE_INCREMENTALLY))
                        bm_run[*num_of_bmr]->subscribers =
                            bm_run[tmp_num_of_bmr]->subscribers;
                        break;
                    }
                    tmp_num_of_bmr--;
                }
                /* Extract the keywords from the command */
                extractStringKeyword(line, "name",
                                     bm_run[*num_of_bmr]->test_run_name, W_L);
                extractIntKeyword(line, "warm_up_duration",
                                  &(bm_run[*num_of_bmr]->warm_up_duration));
                extractIntKeyword(line, "run_duration",
                                  &(bm_run[*num_of_bmr]->run_duration));
                extractIntKeyword(line, "repeats",
                                  &(bm_run[*num_of_bmr]->repeats));
                extractStringKeyword(line, "transaction_mix",
                                     bm_run[*num_of_bmr]->transaction_mix_str,
                                     W_L);
                extractStringKeyword(line, "database_client_distribution",
                                     bm_run[*num_of_bmr]->
                                     client_distribution_str, W_L);
                if (!isEmptyBuf(line, strlen(line))) {
                    sprintf(msg, "Unknown content (%s) in run benchmark "
                            "command in TDF.",
                            line);
                    message('E', msg);
                    return E_ERROR;
                }
                break;
            case EXECUTESQL:
            case EXECUTESQLFILE:
		        bm_run[*num_of_bmr]->cmd_type = EXECUTESQLFILE;
		        if (extractStringKeyword(line,
                                         "file",
                                         bm_run[*num_of_bmr]->sql_file,
                                         W_L)
                    == E_NO_KEYWORD) {
                    extractStringKeyword(line, "sql",
                                         bm_run[*num_of_bmr]->sql_file, W_L);
                    bm_run[*num_of_bmr]->cmd_type = EXECUTESQL;
                }
		        break;                
            case SLEEP:
                bm_run[*num_of_bmr]->cmd_type = SLEEP;
                extractIntKeyword(line, "duration",
                                  &(bm_run[*num_of_bmr]->run_duration));
                break;
            case NOP:
                message('E', "Unknown operation type in TDF [Test sequence] "
                        "section");
                /* Not fatal, because we can handle the next TDF */
                return E_ERROR;
        }
	/* update repeat counter */
	if (*repeats == 0) {
	  *repeats = bm_run[*num_of_bmr]->repeats - 1;
	} else {
	  (*repeats)--;
	}

        (*num_of_bmr)++;

        return 0;
}

/*##**********************************************************************\
 *
 *      digestBasicOperationType
 *
 * Checks if a character string given as an argument starts with a
 * basic operation type tag, that is, either 'populate' or 'run'.  As a
 * side effect, abovementioned reserved words are cleaned from the line
 * (filled with spaces)
 *
 * Parameters:
 *      line 
 *          the character string
 *
 *      operationType
 *          the basic operation type
 *
 * Return value:
 *      0 - success (always)
 */
int digestBasicOperationType(char *line,
                             enum test_sequence_cmd_type *operationType) {
        char *c;
        if (line == strstr(line, "populate")) {
            /* Note. The above checks that the line starts with
               "populate " */
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            if ((c = strstr(line, "conditional")) != NULL) {
                *operationType = POPULATE_CONDITIONALLY;
                for ( ; (*c != ' ' && *c != '\0'); c++) {
                    *c = ' ';
                }
            } else if ((c = strstr(line, "incremental")) != NULL) {
                *operationType = POPULATE_INCREMENTALLY;
                for ( ; (*c != ' ' && *c != '\0'); c++) {
                    *c = ' ';
                }
            } else {
                /* 'incremental' or 'conditional'
                   was not found after 'populate' */
                *operationType = POPULATE;
            }
        }
        else if (line == strstr(line, "run")) {
            /* Note. The above checks that the line starts with
               "run " */
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = RUN;
        }
        else if (line == strstr(line, "execute")) {
	        for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = EXECUTESQL;
        }
        else if (line == strstr(line, "sleep")) {
            for (c = line; (*c != ' ' && *c != '\0'); c++) {
                *c = ' ';
            }
            *operationType = SLEEP;
        }
        else {
            *operationType = NOP;
        }
        return 0;
}

/*##**********************************************************************\
 *
 *      initBMRunParameters
 *
 * Initializes the parameter values for a benchmark run.
 *
 * Parameters:
 *      tdf
 *          the test definition data structure
 *
 *      bm_run
 *			benchmark run data structure
 *
 * Return value:
 *      none
 */
void initBMRunParameters(struct tdfs *tdf, struct bmr *bm_run)
{
        bm_run->subscribers = tdf->subscribers;
        bm_run->min_subscriber_id = 1;
        bm_run->serial_keys = tdf->serial_keys;
        bm_run->commitblock_size = tdf->commitblock_size;
        bm_run->post_population_delay = tdf->post_population_delay;
        bm_run->warm_up_duration = tdf->warm_up_duration;
        bm_run->run_duration = tdf->run_duration;
        bm_run->repeats = tdf->repeats;
}

/*##**********************************************************************\
 *
 *      readRemNodsFile
 *
 * Read the Remote Nodes file and fills up the 'remControls'
 * structure accordingly.
 *
 * Parameters:
 *      RemNodsFileName 
 *          remote nodes file name
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int readRemNodsFile(char *RemNodsFileName)
{
        FILE *fRN = NULL;
        /* The first line indicator */
        int firstline;
        int err;
        int remContrInd;
        int remControlId;
        char line[256];
        char msg[300];
        
        char *strptr = NULL;
        
        /* the regular expression strucure */
        regex_t *r;
        /* used with regex */
        int count, starts[100], lengths[100];
        
        /* Open and handle the fiel */
        if (openFile(&fRN, RemNodsFileName) != 0) {
            /* This should not actually happen, because the file was checked
               to exist in parseOptions */
            sprintf(msg, "Cannot open Remote Nodes file '%s'",
                    RemNodsFileName);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }
        sprintf(msg, "Processing Remote Nodes File '%s'", RemNodsFileName);
        message('I', msg);
        
        firstline = 1;
        err = 0;
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        /* The information is stored to 'remControls' starting from the
           index 1 (and not 0) */
        remContrInd = 1;
        /* Remote control IDs start from -3 and go downwards
           (Main control id is -2) */
        remControlId = REMOTE_CONTROL_ID_BASE;
        /* Iterate through the Remote Nodes file */
        while (readFileLine(fRN, line, 256) != -1) {
            /* First remove potential comments from the line just read */
            removeComment(line);
            if (*line == '\0') {
                /* the line is empty */
                continue;
            }
            message('D', line);
            
            /* Extract the information about the remote nodes from
               the file lines */
            if (!multiMatch(r, line, "^ *(.*) *= *\"(.*)\" +\"(.*)\" *$",
                            &count, starts, lengths)) {
                if (count != 4) {
                    sprintf(msg, "Wrong number of parameters for a Remote "
                            "Node (%s)", line);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }            
                strncpy(remControls[remContrInd].name, &line[starts[1]],
                        minimum(lengths[1], W_L));
                remControls[remContrInd].name[minimum(lengths[1], W_L)] = '\0';
                strncpy(remControls[remContrInd].ip, &line[starts[2]],
                        minimum(lengths[2], W));
                remControls[remContrInd].ip[minimum(lengths[2], W)] = '\0';
                
                remControls[remContrInd].port = CONTROL_PORT;
                strptr = strstr (remControls[remContrInd].ip, ":");
                if (strptr != NULL) {
                    /* specified port */
                    remControls[remContrInd].port = atoi(strptr+1);
                    *strptr = '\0';
                }
                strncpy(remControls[remContrInd].targetDBdsn, &line[starts[3]],
                        minimum(lengths[3], W_L));
                remControls[remContrInd].
                    targetDBdsn[minimum(lengths[3], W_L)] = '\0';
                
                if ((trim(remControls[remContrInd].name) != 0) ||
                    (trim(remControls[remContrInd].ip) != 0) ||
                    (trim(remControls[remContrInd].targetDBdsn) != 0)) {
					sprintf(msg, "Remote node file handling error "
                            "(memory allocation error)");
					message('F', msg);
					return E_FATAL; /* Fatal error */
                }

                /* Check that the information was actually given */
                if (strlen(remControls[remContrInd].name) == 0) {
                    sprintf(msg, "No name given for a Remote Node");
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
                if (strlen(remControls[remContrInd].ip) == 0) {
                    sprintf(msg, "No IP given for a Remote Node %s",
                            remControls[remContrInd].name);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
                if (strlen(remControls[remContrInd].targetDBdsn) == 0) {
                    sprintf(msg, "No target DB DSN given for a Remote Node %s",
                            remControls[remContrInd].name);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }

                remControls[remContrInd].defined = 1;
                remControls[remContrInd].remoteControlId = remControlId;

                remContrInd++;
                remControlId--;
                if (remContrInd > (MAX_CONNECTIONS)) {
                    sprintf(msg, "Too many Remote Nodes (>%d) defined in (%s)",
                            MAX_CONNECTIONS-1, RemNodsFileName);
                    message('F', msg);
                    return E_FATAL; /* Fatal error */
                }
            }
        } /* Next line */

        /* regex is not needed any more */
        free(r);
        fclose(fRN);
        return 0;
}

/*##**********************************************************************\
 *
 *      readDDF
 *
 * Reads the DDF file and fills up the ddf data structure.  Reads the
 * database configuration file (defined in DDF) and stores it to
 * TIRDB.
 *
 * Parameters:
 *      ddffilename
 *		    DDF file name
 *
 *      ddf
 *          pointer to ddf struct
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int readDDF(char *ddffilename, struct ddfs *ddf)
{
        FILE *fDDF = NULL;
        FILE *fTest = NULL;
        /* The first line indicator */
        int firstline;
        int err;
        char line[256];
        char msg[300];
        /* the regular expression strucure */
        regex_t *r;
        /* start and length (substring) used with simple regex */
        int start, length;

        /* Open and handle the DDF */
        if (openFile(&fDDF, ddffilename) != 0) {
            /* This should not actually happen, because the file was checked
               to exist in parseOptions */
            sprintf(msg, "Cannot open DDF %s", ddffilename);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        }
        sprintf(msg, "Processing Data Definition File '%s'", ddffilename);
        message('I', msg);
        /* Clear ddf data */
        *ddf->db_name = '\0';
        *ddf->db_version = '\0';
        *ddf->db_connect = '\0';
        *ddf->os_name = '\0';
        *ddf->os_version = '\0';
        *ddf->hardware_id = '\0';
        *ddf->configuration_file_name = '\0';
        *ddf->configuration_code = '\0';
        *ddf->configuration_comments = '\0';
        *ddf->configuration_file_contents = '\0';

        *ddf->db_schemaname = '\0';
        *ddf->db_schemafile = '\0';
        *ddf->db_transactionfile = '\0';
        *ddf->db_initfile = '\0';
        *ddf->db_connect_initfile = '\0';

        firstline = 1;
        err = 0;
        r = (regex_t *) malloc(sizeof(regex_t));
        if (r == NULL) {
            writeLog('E', "Cannot reserve memory for regex_t");
            return E_ERROR;
        }
        /* Iterate through the DDF file */
        while (readFileLine(fDDF, line, 256) != -1) {
            /* First remove potential comments from the line just read */
            removeComment(line);

            if (*line == '\0') {
                /* the line is empty */
                continue;
            }
            message('D', line);

            /* Try to find the acceptable keywords from the line.
               If found, copy the keyword value to a variable */

            SIMPLEMATCH_FILL_STR(ddf->db_name, r, line, start, length,
                                 "^db_name *= *\"(.*)\" *$", W);
            
            /*if (!simpleMatch(r, line, "^db_name *= *\"(.*)\" *$",
                             &start, &length)) {
                             strncpy(ddf->db_name, &line[start], minimum(W, length));
                             ddf->db_name[length] = '\0';
                             continue;
                             }*/

            SIMPLEMATCH_FILL_STR(ddf->db_version, r, line, start, length,
                                 "^db_version *= *\"(.*)\" *$", W);

            /*if (!simpleMatch(r, line, "^db_version *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_version, &line[start], minimum(W, length));
                ddf->db_version[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->db_connect, r, line, start, length,
                                 "^db_connect *= *\"(.*)\" *$", W_L);

            /*if (!simpleMatch(r, line, "^db_connect *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_connect, &line[start], minimum(W_L, length));
                ddf->db_connect[length] = '\0';
                continue;
                } */


            SIMPLEMATCH_FILL_STR(ddf->os_name, r, line, start, length,
                                 "^os_name *= *\"(.*)\" *$", W);

            /*if (!simpleMatch(r, line, "^os_name *= *\"(.*)\" *$",
                             &start, &length)) {
                             strncpy(ddf->os_name, &line[start], minimum(W, length));
                             ddf->os_name[length] = '\0';
                             continue;
                             }*/

            SIMPLEMATCH_FILL_STR(ddf->os_version, r, line, start, length,
                                  "^os_version *= *\"(.*)\" *$", W);
            
            /*if (!simpleMatch(r, line, "^os_version *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->os_version, &line[start], minimum(W, length));
                ddf->os_version[length] = '\0';
                continue;
                }*/


            SIMPLEMATCH_FILL_STR(ddf->hardware_id, r, line, start, length,
                                 "^hardware_id *= *\"(.*)\" *$", W);

            /*if (!simpleMatch(r, line, "^hardware_id *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->hardware_id, &line[start], minimum(W, length));
                ddf->hardware_id[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->configuration_code, r, line, start, length,
                                 "^configuration_code *= *\"(.*)\" *$", W);
            
            /*if (!simpleMatch(r, line, "^configuration_code *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_code, &line[start],
                        minimum(W, length));
                ddf->configuration_code[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->configuration_file_name, r, line, start,
                                 length, "^configuration_file *= *\"(.*)\" *$",
                                 W_L);
                        
            /*if (!simpleMatch(r, line, "^configuration_file *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_file_name, &line[start],
                        minimum(W_L, length));
                ddf->configuration_file_name[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->configuration_comments, r, line, start,
                                 length,
                                 "^configuration_comments *= *\"(.*)\" *$",
                                 W_EL);
                        
            /*if (!simpleMatch(r, line, "^configuration_comments *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->configuration_comments, &line[start],
                        minimum(W_EL, length));
                ddf->configuration_comments[length] = '\0';
                continue;
                }*/
            
            SIMPLEMATCH_FILL_STR(ddf->db_initfile, r, line, start,
                                 length, "^targetdbinit *= *\"(.*)\" *$",
                                 FILENAME_LENGTH);
            
            /*if (!simpleMatch(r, line, "^targetdbinit *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_initfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_initfile[length] = '\0';
                continue;
                }*/
            
            SIMPLEMATCH_FILL_STR(ddf->db_connect_initfile, r, line, start,
                                 length, "^connectioninit *= *\"(.*)\" *$",
                                 FILENAME_LENGTH);
            
            /*if (!simpleMatch(r, line, "^connectioninit *= *\"(.*)\" *$",
                             &start, &length)) {
                strncpy(ddf->db_connect_initfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_connect_initfile[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->db_schemafile, r, line, start,
                                 length, "^targetdbschema *= *\"(.*)\" *$",
                                 FILENAME_LENGTH);
            
            /*if (!simpleMatch(r, line, "^targetdbschema *= *\"(.*)\" *$",
                             &start, &length)) {
		        strncpy(ddf->db_schemafile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_schemafile[length] = '\0';
                continue;
                }*/

            SIMPLEMATCH_FILL_STR(ddf->db_transactionfile, r, line, start,
                                 length,  "^transaction_file *= *\"(.*)\" *$",
                                 FILENAME_LENGTH);
            
            /*if (!simpleMatch(r, line, "^transaction_file *= *\"(.*)\" *$",
                             &start, &length)) {
		        strncpy(ddf->db_transactionfile, &line[start],
                        minimum(FILENAME_LENGTH, length));
                ddf->db_transactionfile[length] = '\0';
                continue;
                }*/
            
            /* transaction_file is the only mandatory parameter
               (even if TIRDB is not used) */
            if (*ddf->db_transactionfile == '\0') {
	          	sprintf(msg, "DDF parameter: no 'transaction_file' defined");
                message('F', msg);
                return E_FATAL;
            }

            if (!simpleMatch(r, line, "^targetdbschemaname *= *\"(.*)\" *$",
                             &start, &length)) {
                if (length == 0) {
                    sprintf(msg,
                            "Fatal error: empty DBSchemaName");
                    message('F', msg);
                    return E_FATAL;
                }
                strncpy(ddf->db_schemaname, &line[start],
                        minimum(W-1, length));
                strcat(ddf->db_schemaname, ".");
                length++;
                ddf->db_schemaname[length] = '\0';
                continue;
            }

            sprintf(msg, "Unknown keyword in DDF: %s", line);
            message('F', msg);
            return E_FATAL; /* Fatal error */
        } /* Next line */

        /* regex is not needed any more */
        free(r);
        fclose(fDDF);

        /* Check that TargetDBInit file exists
           and that it has the correct file tag (starting the file) */
        if (*(ddf->db_initfile) != '\0') {
            if (openFile(&fTest, ddf->db_initfile) != 0) {
                sprintf(msg,
                        "Fatal error: Cannot open DB init file '%s'",
                        ddf->db_initfile);
                message('F', msg);
                return E_FATAL;
            } else {
                if (readFileLine(fTest, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", ddf->db_initfile);
                    message('F', msg);
                    message('F',
                            "The file has to start with the line //tatp_sql");
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if ((!(strncmp(line, "//tatp_sql", 10) == 0))
                    && (!(strncmp(line, "--tatp_sql", 10) == 0))) {
                    sprintf(msg,
                            "File '%s' is not a DB initialization file",
                            ddf->db_initfile);
                    message('F', msg);
                    message('F', "Check first line (//tatp_sql)");
                    return E_FATAL;
                }
                fclose(fTest);
                sprintf(msg,
                        "Using DB initialization file '%s'.",
                        ddf->db_initfile);
                message('D', msg);
            }
        }
    
        /* Check that TargetSchemaInit file exists (if it was defined
           in tatp.ini) and that it has the correct file tag (starting
           the file) */
        if (*(ddf->db_schemafile) != '\0') {
            if (openFile(&fTest, ddf->db_schemafile) != 0) {
                sprintf(msg,
                        "Fatal error: Cannot open DB schema file '%s'",
                        ddf->db_schemafile);
                message('F', msg);
                return E_FATAL;
            } else {
                if (readFileLine(fTest, line, 256) == -1) {
                    sprintf(msg, "File '%s' is empty", ddf->db_schemafile);
                    message('F', msg);
                    message('F', "The file has to start with "
                            "the line '//tatp_sql'");
                    return E_FATAL;
                }
                /* check the file type from the first line of the file */
                if ((!(strncmp(line, "//tatp_sql", 10) == 0))
                    && (!(strncmp(line, "--tatp_sql", 10) == 0))) {
                    sprintf(msg,
                            "File '%s' is not a DB schema file",
                            ddf->db_schemafile);
                    message('F', msg);
                    message('F', "Check first line (//tatp_sql)");
                    return E_FATAL;
                }
                fclose(fTest);
                sprintf(msg, "Using DB schema file '%s'.", ddf->db_schemafile);
                message('D', msg);
            }
        }
    
        if (*ddf->db_connect == '\0') {
            message('F', "Missing 'db_connect' in DDF");
            return E_FATAL; /* Fatal error */
        }
    
        return 0;
}

/*##**********************************************************************\
 *
 *      checkDDFparameters
 *
 * Checks if the parameters for TIRDB access are present.*
 *
 * Parameters:
 *      ddf 
 *          pointer to ddf struct
 *
 * Return value:
 *      0  - success
 *      E_ERROR  - error
 */
int checkDDFparameters(const struct ddfs *ddf)
{       
        /* Check the data */
        if (*ddf->db_name == '\0') {
            message('F', "Missing 'db_name' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->db_version == '\0') {
            message('F', "Missing 'db_version' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->os_name == '\0') {
            message('F', "Missing 'os_name' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->os_version == '\0') {
            message('F', "Missing 'os_version' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->hardware_id == '\0') {
            message('F', "Missing 'hardware_id' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->configuration_file_name == '\0') {
            message('F', "Missing 'configuration_file' in DDF");
            return E_FATAL; /* Fatal error */
        }
        if (*ddf->configuration_code == '\0') {
            message('F', "Missing 'configuration_code' in DDF");
            return E_FATAL; /* Fatal error */
        }
        /* Configuration_comments is not mandatory so its
           not checked */

        return 0;
}
