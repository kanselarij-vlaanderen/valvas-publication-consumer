import { app, errorHandler } from 'mu';
import fetch from 'node-fetch';
import { INGEST_INTERVAL } from './config';
import { scheduleSyncTask, getRunningSyncTask, getNextSyncTask } from './lib/sync-task';
import { getUnconsumedDatasets } from './lib/dataset';

/**
 * Core assumption of the microservice that must be respected at all times:
 *
 * 1. At any moment we know that the latest ext:syncUntil timestamp
 *    on a task, either in failed/ongoing/success state, reflects
 *    the timestamp of the latest sync that has been
 *    completely and successfully consumed
 * 2. Maximum 1 sync task is running at any moment in time
*/

async function triggerIngest() {
  if (INGEST_INTERVAL > 0) {
    console.log(`Executing scheduled function at ${new Date().toISOString()}`);
    fetch('http://localhost/ingest/', {
      method: 'POST'
    });
    setTimeout(triggerIngest, INGEST_INTERVAL);
  }
}

app.post('/ingest', async function (req, res, next) {
  await scheduleSyncTask();

  const isRunning = await getRunningSyncTask();

  if (!isRunning) {
    const task = await getNextSyncTask();
    if (task) {
      console.log(`Start ingesting new published data since ${task.since.toISOString()}`);
      try {
        const datasets = await getUnconsumedDatasets(task.since);
        task.datasets = datasets;
        task.execute();  // errors are handled inside task.execute()
        return res.status(202).end();
      } catch (e) {
        console.log(`Something went wrong while ingesting. Closing sync task with failure state.`);
        console.trace(e);
        await task.closeWithFailure();
        return next(new Error(e));
      }
    } else {
      console.log(`No scheduled sync task found. Did the insertion of a new task just fail?`);
      return res.status(200).end();
    }
  } else {
    console.log('A sync task is already running. A new task is scheduled and will start when the previous task finishes');
    return res.status(409).end();
  }
});

app.use(errorHandler);