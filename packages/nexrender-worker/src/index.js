const { createClient } = require('@nexrender/api');
const { init, render } = require('@nexrender/core');
const { getRenderingStatus } = require('@nexrender/types/job');

const instance = createWorker()

/* TODO: possibly add support for graceful shutdown */
let active = true;

const delay = amount => (
    new Promise(resolve => setTimeout(resolve, amount))
);

const nextJob = async (client, settings) => {
    do {
        try {
            let job = await (settings.tagSelector ?
                await client.pickupJob(settings.tagSelector) :
                await client.pickupJob()
            );

            if (job && job.uid) {
                emptyReturns = 0;
                return job;
            } else {
                // no job was returned by the server. If enough checks have passed, and the exit option is set, deactivate the worker
                emptyReturns++;
                if (settings.exitOnEmptyQueue && emptyReturns > settings.tolerateEmptyQueues) active = false;
            }

        } catch (err) {
            if (settings.stopOnError) {
                throw err;
            } else {
                console.error(err);
                console.error("render process stopped with error...");
                console.error("continue listening next job...");
            }
        }

        if (active) await delay(settings.polling || NEXRENDER_API_POLLING);
    } while (active);
};

/**
 * Starts worker "thread" of continuous loop
 * of fetching queued projects and rendering them
 * @param  {String} host
 * @param  {String} secret
 * @param  {Object} settings
 * @return {Promise}
 */
const start = async (host, secret, settings, headers) => {
    settings = init(Object.assign({ process: 'nexrender-worker' }, settings, {
        logger: console,
    }));

    settings.logger.log('starting nexrender-worker with following settings:');
    Object.keys(settings).forEach(key => {
        settings.logger.log(` - ${key}: ${settings[key]}`);
    });

    if (typeof settings.tagSelector == 'string') {
        settings.tagSelector = settings.tagSelector.replace(/[^a-z0-9, ]/gi, '');
    }
    // if there is no setting for how many empty queues to tolerate, make one from the
    // environment variable, or the default (which is zero)
    if (!(typeof settings.tolerateEmptyQueues == 'number')) {
        settings.tolerateEmptyQueues = NEXRENDER_TOLERATE_EMPTY_QUEUES;
    }

    const client = createClient({ host, secret, headers });

    settings.track('Worker Started', {
        worker_tags_set: !!settings.tagSelector,
        worker_setting_tolerate_empty_queues: settings.tolerateEmptyQueues,
        worker_setting_exit_on_empty_queue: settings.exitOnEmptyQueue,
        worker_setting_polling: settings.polling,
        worker_setting_stop_on_error: settings.stopOnError,
    });

    do {
        let job = await nextJob(client, settings);

        // if the worker has been deactivated, exit this loop
        if (!active) break;

        settings.track('Worker Job Started', {
            job_id: job.uid, // anonymized internally
        });

        job.state = 'started';
        job.startedAt = new Date();

        try {
            await client.updateJob(job.uid, job);
        } catch (err) {
            console.log(`[${job.uid}] error while updating job state to ${job.state}. Job abandoned.`);
            console.log(`[${job.uid}] error stack: ${err.stack}`);
            continue;
        }

        let retryCount = 0;
        let numberOfRetries = settings.numOfRetries || 1; 

        do {
            try {
                // Attempt to render the job
                job = await render(job, settings);
                job.state = 'finished';
                job.finishedAt = new Date();
                if (settings.onFinished) {
                    settings.onFinished(job);
                }

                settings.track('Worker Job Finished', { job_id: job.uid });

                await client.updateJob(job.uid, getRenderingStatus(job));
                break; // Exit the retry loop if rendering succeeds
            } catch (err) {
                // Increment retry count
                retryCount++;

                // Check if retries are allowed and the maximum retry count is not reached
                if (settings.retryOnError && retryCount <= numberOfRetries) {
                    console.log(`[${job.uid}] error occurred during rendering: ${err.toString()}`);
                    console.log(`[${job.uid}] Retrying rendering (Attempt ${retryCount} of ${settings.numOfRetries})...`);

                    //update state to inform user job is retrying
                    job.state = 'retrying'

                    // Retry rendering after a delay
                    await delay(settings.retryDelay || 0);
                } else {
                    // If retries are not allowed or maximum retry count is reached, handle the error
                    job.error = [].concat(job.error || [], [err.toString()]);
                    job.errorAt = new Date();
                    job.state = 'error';

                    settings.track('Worker Job Error', { job_id: job.uid });

                    if (settings.onError) {
                        settings.onError(job, err);
                    }

                    await client.updateJob(job.uid, getRenderingStatus(job)).catch((err) => {
                        if (settings.stopOnError) {
                            throw err;
                        } else {
                            console.log(`[${job.uid}] error occurred: ${err.stack}`);
                            console.log(`[${job.uid}] render process stopped with error...`);
                            console.log(`[${job.uid}] continue listening next job...`);
                        }
                    });

                    if (settings.stopOnError) {
                        throw err;
                    } else {
                        console.log(`[${job.uid}] error occurred: ${err.stack}`);
                        console.log(`[${job.uid}] render process stopped with error...`);
                        console.log(`[${job.uid}] continue listening next job...`);
                    }
                }
            }
        } while (active && retryCount <= settings.numOfRetries);

    } while (active);
};

module.exports = {
    start,
};
