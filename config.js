const INGEST_INTERVAL = process.env.INGEST_INTERVAL || -1;
const SYNC_BASE_URL = process.env.SYNC_BASE_URL || 'https://themis.vlaanderen.be';
const SYNC_ENDPOINT = `${SYNC_BASE_URL}/datasets`;
const DISTRIBUTION_NEWSITEM_TYPE = 'http://themis.vlaanderen.be/id/concept/distribution-type/dd5bfc23-8f88-4df5-80f6-a9f72e08d7c4';
const DISTRIBUTION_ATTACHMENT_TYPE = 'http://themis.vlaanderen.be/id/concept/distribution-type/c4d99dde-3df9-4da1-8136-9a3b2de82de4';
const DATASET_NEWSITEM_TYPE = 'http://themis.vlaanderen.be/id/concept/dataset-type/9119805f-9ee6-4ef1-9ef7-ad8dccc2bf2d';
const DOCUMENT_NEWSITEM_TYPE = 'http://themis.vlaanderen.be/id/concept/document-type/63d628cb-a594-4166-8b4e-880b4214fc5b';
const PUBLIC_GRAPH = 'http://mu.semte.ch/graphs/public';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
const UPDATE_BATCH_SIZE = parseInt(process.env.UPDATE_BATCH_SIZE) || 100;

export {
  INGEST_INTERVAL,
  SYNC_ENDPOINT,
  SYNC_BASE_URL,
  DISTRIBUTION_NEWSITEM_TYPE,
  DISTRIBUTION_ATTACHMENT_TYPE,
  DOCUMENT_NEWSITEM_TYPE,
  DATASET_NEWSITEM_TYPE,
  PUBLIC_GRAPH,
  BATCH_SIZE,
  UPDATE_BATCH_SIZE
}
