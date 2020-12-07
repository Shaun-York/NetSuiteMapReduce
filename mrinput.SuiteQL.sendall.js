/**
 * @NApiVersion 2.0
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/query', 'N/search', 'N/log', 'N/https', 'N/error'], function (Q, S, Log, HTTPS, E) {

    //TODO Set these with script parameters
    const OUTPUT_URLS = [
        ''
    ]
    // TODO Client certificates and JWT signing
    const OUTPUT_AUTH_HEADERS = [{
        "Content-Type": "application/json",
        "Authorization": ""
    }]

    // Object.assign polyfill
    if (typeof Object.assign != 'function') {
        // Must be writable: true, enumerable: false, configurable: true
        Object.defineProperty(Object, 'assign', {
            value: function assign(target, varArgs) {
                // .length of function is 2
                'use strict';
                if (target == null) {
                    // TypeError if undefined or null
                    throw new TypeError(
                        'Cannot convert undefined or null to object'
                    );
                }
                var to = Object(target);
                for (var index = 1; index < arguments.length; index++) {
                    var nextSource = arguments[index];
                    if (nextSource != null) {
                        // Skip over if undefined or null
                        for (var nextKey in nextSource) {
                            // Avoid bugs when hasOwnProperty is shadowed
                            if (
                                Object.prototype.hasOwnProperty.call(nextSource, nextKey)
                            ) {
                                to[nextKey] = nextSource[nextKey];
                            }
                        }
                    }
                }
                return to;
            },
            writable: true,
            configurable: true
        })
    }
    // Array.prototypeisArray Polyfill
    if (!Array.isArray) {
        Array.isArray = function (arg) {
            return Object.prototype.toString.call(arg) === '[object Array]';
        };
    }
    // String.startsWith.prototype Polyfill
    if (!String.prototype.startsWith) {
        Object.defineProperty(String.prototype, 'startsWith', {
            value: function (search, rawPos) {
                var pos = rawPos > 0 ? rawPos | 0 : 0;
                return this.substring(pos, pos + search.length) === search;
            }
        });
    }

    function log_ss_debug(func, msg, context) {
        const debug_options = {
            title: func,
            details: 'MSG: [ ' + msg + ' ] \n\n' + JSON.stringify(context)
        }
        Log.debug(debug_options)
    }

    function post_parameters(index) {
        // index is 0 unless total failure 
        return [OUTPUT_URLS[index], OUTPUT_AUTH_HEADERS[index]]
    }

    function https_options(tasks, index) {
        const func = 'https_options'
        const [url, headers] = post_parameters(index)
        const options = {
            url: url,
            headers: headers
        }
        try {
            return [Object.assign(options, {
                body: JSON.stringify(tasks)
            }), index]
        } catch (error) {
            log_ss_debug(func, error.message, tasks)
        }
    }

    function post_to_endpoint(https_options, retry_count) {
        const func = 'post_to_endpoint'
        const [options, endpoint_index] = https_options

        try {
            if (retry_count >= 1 && https_options.hasOwnProperty('url')) {
                return H.post(https_options)
            }
        } catch (error) {
            log_ss_error(func, error.message)
        }

        try {
            // try again!
            if (retry_count >= 1) {
                return HTTPS.post(options)
                // tried 5 times; switching endpoints
            } else if (retry_count === 0 && endpoint_index === 0) {
                log_ss_debug(func, 'Failed to deliver Payload', options)
                throw E.create({
                    name: 'BAD_ENDPOINT',
                    message: 'Trying another datacenter url.'
                })
            } else {
                // TODO: send email to tech support
                log_ss_debug(func, 'Failed to deliver Payload', options)
            }
        } catch (error) {
            const err_msg = error.message,
                err_code = error.name,
                closed_connection = 'SSS_CONNECTION_CLOSED',
                url_failed = 'BAD_ENDPOINT',
                missing_args = 'SSS_MISSING_REQD_ARGUMENT',
                bad_loop = 'SSS_REQUEST_LOOP_DETECTED',
                bad_headers = 'SSS_INVALID_HEADER',
                request_timedout = 'SSS_REQUEST_TIME_EXCEEDED'

            switch (err_code) {
                case bad_loop: {
                    log_ss_debug(func, bad_loop, options)
                    break
                }
                case closed_connection: {
                    const try_again_closed = (retry_count - 1)
                    post_to_endpoint(options, try_again_closed)
                    break
                }
                case request_timedout: {
                    const try_again_timedout = (retry_count - 1)
                    post_to_endpoint(options, try_again_timedout)
                    break
                }
                case url_failed: {
                    const tasks = JSON.parse(options.body)
                    const [new_https_options] = https_options(tasks, endpoint_index + 1)
                    post_to_endpoint(new_https_options, 5)
                    break
                }
                case missing_args: {
                    log_ss_error(func, err_msg)
                    log_ss_debug(func, err_code, https_options)
                    break
                }
                case bad_headers: {
                    log_ss_debug(func, bad_headers, https_options)
                    break
                }
                default: {
                    log_ss_debug(func, err_msg, https_options)
                    return
                }
            }
        }
    }

    function post_payload(payload) {
        log_ss_debug('post_payload', 'Records in payload', payload.length)
        const options = https_options(payload, 0)
        const response = post_to_endpoint(options, 5)
        log_ss_debug('post_payload', 'response', response)
    }

    function src_labor_times(sequence, workcenter_internalid, workorder_internalid) {
        // customrecord_workcentertime
        //custrecord_hr ": "HR ",
        //"custrecord_quantity": ".46",
        //"custrecord_used_in_build": ".483",
        //"custrecord_workcentertime_item": "83034",
        //"custrecord_workcentertime_operation_item": "10", <-
        //"custrecord_workcentertime_workcenter": "16488",
        // "custrecord_workcentertime_workorder": "15484012", <-
        try {
            const times = {
                    setupTime: 0
                },
                wrkcnttime = 'customrecord_workcentertime',
                wrkcntt_nwo = 'custrecord_workcentertime_workorder',
                wrkcntt_cnt = 'custrecord_workcentertime_workcenter',
                op_item = 'custrecord_workcentertime_operation_item',
                op_qty = 'custrecord_quantity'
            const wrkord_id = {
                    name: wrkcntt_nwo,
                    operator: 'is',
                    values: workorder_internalid
                },
                wrkord_wc = {
                    name: wrkcntt_cnt,
                    operator: 'is',
                    values: workcenter_internalid
                },
                wrkord_seq = {
                    name: op_item,
                    operator: 'is',
                    values: sequence
                }
            const filters = [wrkord_id, wrkord_wc, wrkord_seq],
                columns = [op_qty, wrkcntt_nwo, op_item]

            const T = S.create({
                type: wrkcnttime,
                filters: filters,
                columns: columns
            })
            T.run().each(function (x) {

                const laborTime = x.getValue({ name: op_qty })
                Object.assign(times, { laborTime: (laborTime) ? Math.round(Math.ceil(laborTime * 60)) : 0 })

                return true
            })
            return (times.hasOwnProperty('laborTime')) ? times : Object.assign(times, { laborTime: 0 })
        } catch (error) {
            return {
                ...times,
                laborTime: 0
            }
        }
    }

    const AWS_SQS_LIMIT = 256000
    const max_payload_size = AWS_SQS_LIMIT
    var payload_size = 0
    var post_requests = []
    var payload = []

    return {
        getInputData: function () {
            //Query for work
            /*Work Order:Planning	        WorkOrd:A
             *Work Order:Pending Build	    WorkOrd:B
             *Work Order:Cancelled	        WorkOrd:C
             *Work Order:In Process	        WorkOrd:D
             *Work Order:Built	            WorkOrd:G
             *Work Order:Closed	            WorkOrd:H
             */
            try {
                const WOmfgOps = "SELECT " +
                    "T.id || O.operationsequence AS internalid, " +
                    "T.id AS workorder, " +
                    "REPLACE(T.trandisplayname, 'Work Order #', '') AS workorder_display, " +
                    "O.manufacturingworkcenter AS workcenter, " +
                    "BUILTIN.DF(O.manufacturingworkcenter) AS workcenter_display, " +
                    "MR.item AS item, " +
                    "BUILTIN.DF(MR.item) AS item_display, " +
                    "T.custbody_prodenddate AS ped, " +
                    "O.inputquantity AS inputQty, " +
                    "NVL(O.completedquantity, 0) AS completedQty, " +
                    "O.id AS task, " +
                    "O.id AS task_rec, " +
                    "O.operationsequence AS sequence, " +
                    "O.status AS status, " +
                    "FROM transaction AS T " +
                    "INNER JOIN manufacturingoperationtask AS O ON T.id = O.workorder " +
                    "INNER JOIN manufacturingrouting AS MR ON T.manufacturingrouting = MR.id " +
                    "WHERE UPPER(T.recordtype) IN ('WORKORDER') AND UPPER(T.status) IN ('WORKORD:A') " +
                    "AND O.status NOT IN ('COMPLETE') " +
                    "AND BUILTIN.DF(O.manufacturingworkcenter) IN ('CNPR','CNPS','XFPR','MIMK','RICO','BIND','BVPR','BBPR','TXPR')"

                const mfgOpQuery = Q.runSuiteQL({ query: WOmfgOps })
                const records = mfgOpQuery.asMappedResults()
                log_ss_debug('getInputData', 'Sync:input mfgOps count', records.length)
                return records
            } catch (error) {
                log_ss_debug(error.type, error.name, error.message)
            }
        },
        map: function (context) {
            // build up final object
            try {
                var mfgOp = JSON.parse(context.value)

                // computed columns/fields

                var mfg_operation = Object.assign(mfgOp,
                    src_labor_times(mfgOp.sequence, mfgOp.workcenter, mfgOp.workorder),
                )
                
                // char length
                const size = JSON.stringify(mfg_operation).length
                const chunk_value = {
                    size: size,
                    payload: mfg_operation
                }
                const chunk = {
                    key: context.key,
                    value: chunk_value
                }
                context.write(chunk)
            } catch (error) {
                log_ss_debug(error.type, error.name, error.message)
            }
        },
        reduce: function (context) {
            try {
                const x = {
                    key: context.key,
                    value: context.values[0]
                }
                context.write(x);
                log_ss_debug('reduce', 'rec', x);
            } catch (error) {
                log_ss_debug(error.type, error.name, error.message)
            }
        },
        summarize: function (context) {
            try {
                context.output.iterator().each(function (key, value) {
                    const x = JSON.parse(value)
                    log_ss_debug('summarize', 'summarize', x)
                    if (x.size + payload_size < max_payload_size) {
                        payload_size = payload_size + x.size
                        payload.push(x.payload)
                    } else {
                        post_requests.push(payload)
                        payload_size = 0
                        payload = []
                    }
                    return true
                })
                // POST sets of requests
                if (post_requests.length !== 0) {
                    post_requests.forEach(function (req) {
                        log_ss_debug('post_payload', 'payload', req)
                        post_payload(req)
                    })
                }
                // POST leftovers
                if (payload.length !== 0) {
                    log_ss_debug('post_payload', 'payload modulo count', payload.length)
                    post_payload(payload)
                }
            } catch (error) {
                log_ss_debug(error.type, error.name, error.message)
            }
        }
    }
})