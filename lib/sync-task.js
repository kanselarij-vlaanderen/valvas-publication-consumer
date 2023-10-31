import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeDateTime, uuid } from 'mu';

import { PUBLIC_GRAPH } from '../config';

const TASK_NOT_STARTED_STATUS = 'http://kanselarij.vo.data.gift/services/valvas-publication-consumer/sync-task-statuses/not-started';
const TASK_ONGOING_STATUS = 'http://kanselarij.vo.data.gift/services/valvas-publication-consumer/sync-task-statuses/ongoing';
const TASK_SUCCESS_STATUS = 'http://kanselarij.vo.data.gift/services/valvas-publication-consumer/sync-task-statuses/success';
const TASK_FAILED_STATUS = 'http://kanselarij.vo.data.gift/services/valvas-publication-consumer/sync-task-statuses/failed';

class SyncTask {
  constructor({ uri, since, until, created, status }) {
    /** Uri of the sync task */
    this.uri = uri;

    /**
     * Datetime as Data object since when datasets should be retrievd from the producer service
    */
    this.since = since;

    /**
     * Datetime as Data object when the task was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the sync task as stored in the triplestore
    */
    this.status = status;

    /**
     * Time in ms of the latest successfully ingested dataset.
     * Will be updated while datasets are being consumed.
    */
    this.latestDatasetMs = Date.parse(since.toISOString());

    /**
     * List of datasets to be ingested for this task
     * I.e. datasets generated since timestamp {since}
     * retrieved from the producer server just before
     * the start of the task execution
     *
     * @type Array of Dataset
    */
    this.datasets = [];

    /**
     * Number of already successfull ingested datasets for this task
    */
    this.handledDatasets = 0;

    /**
     * Progress status of the handling of datasets.
     * This status is only used during execution and never persisted in the store.
     * Possible values: [ 'notStarted', 'progressing', 'failed' ]
    */
    this.progressStatus = 'notStarted';
  }

  /**
   * Get datetime as Date object of the latest successfully ingested dataset
  */
  get latestDataset() {
    return new Date(this.latestDatasetMs);
  }

  /**
   * Get the total number of files to be ingested for this task
  */
  get totalDatasets() {
    return this.datasets.length;
  }

  /**
   * Execute the sync task
   * I.e. consume the datasets one-by-one as long as there are datasets
   * or until ingestion of a file fails
   *
   * @public
  */
  async execute() {
    try {
      await this.persistStatus(TASK_ONGOING_STATUS);
      console.log(`Found ${this.totalDatasets} new datasets to be consumed`);
      if (this.totalDatasets) {
        await this.consumeNext();
      } else {
        console.log(`No datasets to consume. Finished sync task successfully.`);
        console.log(`Most recent dataset consumed is created at ${this.latestDataset.toISOString()}.`);
        await this.persistLatestDataset(this.latestDatasetMs);
        await this.persistStatus(TASK_SUCCESS_STATUS);
      }
    } catch (e) {
     this.closeWithFailure();
      console.log(`Something went wrong while consuming the datasets.`);
      console.log(e);
    }
  }

  /**
   * Close the sync task with a failure status
   *
   * @public
  */
  async closeWithFailure() {
    await this.persistLatestDataset(this.latestDatasetMs);
    await this.persistStatus(TASK_FAILED_STATUS);
  }

  /**
   * Recursive function to consume the next dataset in the datasets array
   *
   * @private
  */
  async consumeNext() {
    const dataset = this.datasets[this.handledDatasets];
    await dataset.consume(async (dataset, isSuccess) => {
      if (isSuccess) {
        this.handledDatasets++;

        console.log(`Consumed ${this.handledDatasets}/${this.totalDatasets} datasets`);
      }
      // update latestDatasetMS in triple store on success
      await this.updateProgressStatus(dataset, isSuccess);

      if (this.progressStatus == 'progressing' && this.handledDatasets < this.totalDatasets) {
        await this.consumeNext();
      } else {
        if (this.progressStatus == 'failed') {
          await this.persistStatus(TASK_FAILED_STATUS);
          console.log(`Failed to finish sync task. Skipping the remaining datasets. Most recent datas successfully consumed is created at ${this.latestDataset.toISOString()}.`);
        } else {
          await this.persistStatus(TASK_SUCCESS_STATUS);
          console.log(`Finished sync task successfully. Ingested ${this.totalDatasets} files. Most recent dataset consumed is created at ${this.latestDataset.toISOString()}.`);
        }
      }
    });
  };

  /**
   * Update the progress status of the dataset handling and write the latest ingested dataset timestamp to the store.
   * I.e. update the progress status to 'progressing' and update the latest dataset timestamp on success.
   * Update the progress status to 'failed' on failure
   *
   * @param dataset {Dataset} Ingested dataset
   * @param isSuccess {boolean} Flag to indicate success of ingestion of the given dataset
   * @private
  */
  async updateProgressStatus(dataset, isSuccess) {
    if (isSuccess && this.progressStatus != 'failed') {
      this.progressStatus = 'progressing';

      const datasetMs = Date.parse(dataset.releaseDate);
      if (datasetMs > this.latestDatasetMs) {
        await this.persistLatestDataset(datasetMs);
      }
    } else if (!isSuccess) {
      this.progressStatus = 'failed';
    }
  }

  /**
   * Perists the given timestamp as timestamp of the latest consumed dataset in the triple store.
   *
   * At any moment the latest ext:datasetsUntil timestamp on a task, either in failed/ongoing/success state,
   * should reflect the timestamp of the latest dataset that has been completly and successfully consumed.
   * Therefore, the ext:datasetsUntil needs to be updated immediately after every dataset consumption.
   *
   * @param datasetMs {int} Timestamp in milliseconds of the latest successfully consumed dataset
   * @private
  */
  async persistLatestDataset(datasetMs) {
    this.latestDatasetMs = datasetMs;

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> ext:datasetsUntil ?latestDataset .
        }
      }
    `);

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

      INSERT {
        GRAPH ?g {
          <${this.uri}> ext:datasetsUntil ${sparqlEscapeDateTime(this.latestDataset)} .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);

  }

  /**
   * Persists the given status as task status in the triple store
   *
   * @param status {string} URI of the task status
   * @private
  */
  async persistStatus(status) {
    this.status = status;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      DELETE WHERE {
        GRAPH ?g {
          <${this.uri}> adms:status ?status .
        }
      }
    `);

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      INSERT {
        GRAPH ?g {
          <${this.uri}> adms:status <${this.status}> .
        }
      } WHERE {
        GRAPH ?g {
          <${this.uri}> a ext:SyncTask .
        }
      }
    `);
  }
}

/**
 * Insert a new sync task in the store to consume datasets if there isn't one scheduled yet.
 * The timestamp from which datasets need to be consumed is determined at the start of the task execution.
 *
 * @public
*/
async function scheduleSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?s WHERE {
      ?s a ext:SyncTask ;
         adms:status <${TASK_NOT_STARTED_STATUS}> .
    } LIMIT 1
  `);

  if (result.results.bindings.length) {
    console.log(`There is already a sync task scheduled to ingest datasets. No need to create a new task.`);
  } else {
    const uuid = mu.uuid();
    const uri = `http://kanselarij.vo.data.gift/services/valvas-publication-consumer/sync-tasks/${uuid}`;
    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      INSERT DATA {
        GRAPH <${PUBLIC_GRAPH}> {
          <${uri}> a ext:SyncTask ;
             mu:uuid "${uuid}" ;
             adms:status <${TASK_NOT_STARTED_STATUS}> ;
             dct:created ${sparqlEscapeDateTime(new Date())} .
        }
      }
    `);
    console.log(`Scheduled new sync task <${uri}> to ingest datasets`);
  }
}

/**
 * Get the next sync task with the earliest creation date that has not started yet
 *
 * @public
*/
async function getNextSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?s ?created WHERE {
      ?s a ext:SyncTask ;
         adms:status <${TASK_NOT_STARTED_STATUS}> ;
         dct:created ?created .
    } ORDER BY ?created LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];

    console.log('Getting the timestamp of the latest successfully ingested dataset. This will be used as starting point for consumption.');
    let latestDatasetTimestamp = await getLatestDatasetTimestamp();

    if (!latestDatasetTimestamp) {
      console.log(`It seems to be the first time we will consume datasets. No datasets have been consumed before.`);
      if (process.env.START_FROM_DATASET_TIMESTAMP) {
        console.log(`Service is configured to start consuming datasets since ${process.env.START_FROM_DATASET_TIMESTAMP}`);
        latestDatasetTimestamp = new Date(Date.parse(process.env.START_FROM_DATASET_TIMESTAMP));
      } else {
        console.log(`No configuration as of when datasets should be consumed. Starting consuming from sync task creation time ${b['created'].value}.`);
        latestDatasetTimestamp = new Date(Date.parse(b['created'].value));
      }
    }

    return new SyncTask({
      uri: b['s'].value,
      status: TASK_NOT_STARTED_STATUS,
      since: latestDatasetTimestamp,
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

/**
 * Get the URI of the currently running sync task.
 * Null if no task is running.
 *
 * @public
*/
async function getRunningSyncTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?s WHERE {
      GRAPH ?g {
        ?s a ext:SyncTask ;
          dct:created ?created ;
          adms:status <${TASK_ONGOING_STATUS}> .
      }
    } ORDER BY ?created LIMIT 1
  `);

  return result.results.bindings.length ? { uri: result.results.bindings[0]['s'] } : null;
}

/**
 * Update the status of a given task to "failed".
 *
 * @public
*/
async function setTaskFailedStatus(uri) {
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    DELETE WHERE {
      GRAPH ?g {
        <${uri}> adms:status ?status .
      }
    }
  `);

  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT {
      GRAPH ?g {
        <${uri}> adms:status <${TASK_FAILED_STATUS}> .
      }
    } WHERE {
      GRAPH ?g {
        <${uri}> a ext:SyncTask .
      }
    }
  `);
}

/**
 * Get the latest timestamp of a successfully ingested dataset.
 * Even on failed tasks, we're sure ext:datasetsUntil reflects the latest
 * successfully ingested dataset.
 *
 * @private
*/
async function getLatestDatasetTimestamp() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?latestDataset WHERE {
      ?s a ext:SyncTask ;
         ext:datasetsUntil ?latestDataset .
    } ORDER BY DESC(?latestDataset) LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    return new Date(Date.parse(b['latestDataset'].value));
  } else {
    return null;
  }
}

export default SyncTask;
export {
  scheduleSyncTask,
  getNextSyncTask,
  getRunningSyncTask,
  setTaskFailedStatus
};
