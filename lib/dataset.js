import fetch from 'node-fetch';
import { SYNC_ENDPOINT, SYNC_BASE_URL, DISTRIBUTION_NEWSITEM_TYPE, DOCUMENT_NEWSITEM_TYPE, PUBLIC_GRAPH, DATASET_NEWSITEM_TYPE } from '../config';
import { deleteTriplesFromTtl, insertTriplesAndGenerateTtlFile, parseTtl } from './ttl-helpers';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { DataFactory } from 'n3';
import { syncDocuments } from './document';
const { namedNode, defaultGraph, quad } = DataFactory;
import path from 'path';
import fs from 'fs-extra';
import mu, { sparqlEscapeDateTime, sparqlEscapeInt, sparqlEscapeString, sparqlEscapeUri } from 'mu';

class Dataset {

  constructor(data) {
    this.id = data.id;
    this.uri = data.attributes.uri;
    this.releaseDate = data.attributes['release-date'];
    this.distributionsPath = data.relationships.distributions.links.related;
    this.previousVersion = data.relationships['previous-version'].links.related;

    this.filePath = `/share/${this.id}.ttl`;
    this.distributions = [];
   }

  async consume(onFinishCallback) {
    console.log(`Consuming dataset ${this.id}`);
    await this.deletePrevious();
    await this.fetchDistributions(this.distributionsPath);
    await this.insertTtl();
    await syncDocuments(this.distributions);
    onFinishCallback(this, true);
  }

  async deletePrevious() {
    const previousVersionUri = await fetchPreviousVersion(this.previousVersion);
    if (previousVersionUri) {
      await removeDeprecatedTriples(previousVersionUri);
      console.log('Previous version succesfully deleted.');
    } else {
      console.log(`No previous version found for dataset ${this.uri}. Nothing to remove.`);
    }
  }

  async insertTtl() {
    console.log(`Inserting ttl triples for dataset ${this.uri}...`);
    const ttlDistribution =  this.distributions.find(d => d.attributes['type'] === DISTRIBUTION_NEWSITEM_TYPE && d.attributes['download-url']);
    if (ttlDistribution) {
      const ttlDownloadUrl = ttlDistribution.attributes['download-url'];

      let triples;
      const result = await fetch(ttlDownloadUrl);
      if (result.ok) {
        triples = await parseTtl(result.body);
      } else {
        throw new Error(`Request to fetch ttl-file returned status code ${result.status} (download url: ${ttlDownloadUrl})`);
      }

      const newsItemUris = triples
      .filter(triple => triple.predicate.value === 'http://purl.org/dc/terms/type' && triple.object.value === DOCUMENT_NEWSITEM_TYPE)
      .map(triple => triple.subject.value);

      // adding additional type for search index
      newsItemUris.forEach(newsItemUri => {
        const newsItemQuad = quad(
          namedNode(newsItemUri),
          namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
          namedNode('http://mu.semte.ch/vocabularies/ext/Nieuwsbericht'),
          defaultGraph(),
        );
        triples.push(newsItemQuad);
      });

      await insertTriplesAndGenerateTtlFile(triples, PUBLIC_GRAPH, this.filePath);
      await this.insertFileDataObjectForTtl();
      console.log("Insert ttl triples finished succesfully");
    } else {
      console.log(`Distribution TTL download not found for dataset ${this.id}. It may already be unpublished.`);
    }
  }

  async fetchDistributions(path) {
    console.log(`Fetching distributions for dataset ${this.uri}.`);
    let page = 0;
    let size = 20;

    const fetchPage = async (page) => {
      const endpoint = new URL(path, SYNC_BASE_URL);
      const params = new URLSearchParams(Object.entries({
        'page[size]': size,
        'page[number]': page
      }));
      endpoint.search = params.toString();

      const result = await fetch(endpoint, {
        method: 'GET',
        headers: {
          'Accept': 'application/vnd.api+json'
        },
      });

      if (result.ok) {
        return await result.json();
      } else {
        throw new Error(`Request to fetch distributions returned status code ${result.status} (download url: ${endpoint})`);
      }
    }

    try {
      let count;
      do {
        const jsonResult = await fetchPage(page);
        this.distributions.push(...jsonResult.data);
        count = jsonResult.meta.count;
        page++;
      } while (page*size < count);
    } catch (e) {
      console.log(`Unable to retrieve distributions from ${SYNC_BASE_URL}${path}`);
      throw e;
    }
  }

   /**
   * Create a FileDataObject for the ttl file and link to dataset
   *
   * @private
   */
  async insertFileDataObjectForTtl() {
    const now = Date.now();
    const fileName = path.basename(this.filePath);
    const extension = path.extname(this.filePath);
    const format = 'text/turtle';
    const fileStats = fs.statSync(this.filePath);
    const created = new Date(fileStats.birthtime);
    const size = fileStats.size;

    const logicalFileUuid = mu.uuid();
    const logicalFileUri = `http://themis.vlaanderen.be/id/file/${logicalFileUuid}`;

    const physicalFileUuid = mu.uuid();
    const physicalFileUri = this.filePath.replace('/share/', 'share://');

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      INSERT DATA {
        GRAPH <${PUBLIC_GRAPH}> {
          ${sparqlEscapeUri(logicalFileUri)} a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(logicalFileUuid)} ;
            nfo:fileName ${sparqlEscapeString(fileName)} ;
            dct:format ${sparqlEscapeString(format)} ;
            nfo:fileSize ${sparqlEscapeInt(size)} ;
            dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
            dct:creator <http://themis.vlaanderen.be/id/service/valvas-publication-consumer> ;
            dct:created ${sparqlEscapeDateTime(created)} .
          ${sparqlEscapeUri(physicalFileUri)} a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(physicalFileUuid)} ;
            nfo:fileName ${sparqlEscapeString(fileName)} ;
            dct:format ${sparqlEscapeString(format)} ;
            nfo:fileSize ${sparqlEscapeInt(size)} ;
            dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
            dct:created ${sparqlEscapeDateTime(created)} ;
            nie:dataSource ${sparqlEscapeUri(logicalFileUri)} .

          ${sparqlEscapeUri(this.uri)} prov:value ${sparqlEscapeUri(logicalFileUri)} .
        }
      }
    `);
  }
}

async function getUnconsumedDatasets(since) {
  let page = 0;
  let size = 20;
  let datasets = [];

  const fetchPage = async (page) => {
    const endpoint = new URL(SYNC_ENDPOINT);
    const params = new URLSearchParams(Object.entries({
      'page[size]': size,
      'page[number]': page,
      'filter[:gt:release-date]': since.toISOString(),
      'filter[type]': DATASET_NEWSITEM_TYPE,
      sort: '-release-date',
      'filter[distributions][format]': 'text/turtle'
    }));
    endpoint.search = params.toString();
    console.log('********************************************');
    console.log('Sync URL: ');
    console.log(endpoint);
    console.log('********************************************');
    const result = await fetch(endpoint, {
      method: 'GET',
      headers: {
        'Accept': 'application/vnd.api+json'
      },
    });
    if (result.ok) {
      let resultJson = await result.json();
      console.log(JSON.stringify(resultJson, null, ' '));
      console.log('********************************************');
      return resultJson;
    } else {
      throw new Error(`Request to fetch unconsumed datasets returned status code ${result.status} (download url: ${endpoint})`);
    }
  };

  try {
    let count;
    do {
      const jsonResult = await fetchPage(page);
      datasets.push(...jsonResult.data.map(f => new Dataset(f)));
      count = jsonResult.meta.count;
      page++;
    } while (page*size < count);
  } catch (e) {
    console.log(`Unable to retrieve  unconsumed datasets from ${SYNC_ENDPOINT}`);
    throw e;
  }

  datasets.reverse();
  return datasets;
}


async function fetchPreviousVersion(path) {
  try {
    const url = `${SYNC_BASE_URL}${path}`;
    const result = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/vnd.api+json'
      },
    });

    if (result.ok) {
      const jsonResult = await result.json();
      if (jsonResult.data) {
        return jsonResult.data.attributes['uri'];
      } else {
        console.log(`Cannot retrieve dataset URI from previous version response fetched from ${url}`);
        console.log(`Retrieved response: ${JSON.stringify(jsonResult)}`);
      }
    } else {
      throw new Error(`Request to fetch previous version returned status code ${result.status} (download url: ${url})`);
    }
  } catch (e) {
    console.log(`Unable to retrieve previous version from ${SYNC_BASE_URL}${path}`);
    throw e;
  }

  return null;
}

async function removeDeprecatedTriples(previousDataset) {
  console.log(`Removing all triples belonging to previous dataset <${previousDataset}>`);

  const result = await query(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    SELECT ?physicalFileUri
    WHERE {
      GRAPH <${PUBLIC_GRAPH}> {
        <${previousDataset}> prov:value ?logicalFileUri .
        ?physicalFileUri a nfo:FileDataObject ;
          nie:dataSource ?logicalFileUri .
      }
    }
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    const physicalFileUri = b['physicalFileUri'].value;
    const ttlFile = physicalFileUri.replace('share://', '/share/');
    await deleteTriplesFromTtl(ttlFile, PUBLIC_GRAPH);
  } else {
    console.log(`No file found in triplestore for previous dataset <${previousDataset}>. The contents of this dataset were never inserted in the triplestore (probably because it was already unpublished at consumption time).`);
  }
}

export {
  getUnconsumedDatasets
};
