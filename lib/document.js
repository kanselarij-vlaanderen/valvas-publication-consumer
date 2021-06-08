import fs from 'fs-extra';
import path from 'path';
import fetch from 'node-fetch';
import { DISTRIBUTION_ATTACHMENT_TYPE } from '../config';
import { querySudo as query } from '@lblod/mu-auth-sudo';

class Document {
  constructor({ downloadUrl, file }) {
    this.downloadUrl = downloadUrl;
    this.file = file;
  }

  get filepath() {
    return this.locationUri.replace('share://', '/share/');
  }

  async download() {
    const directory = path.dirname(this.filepath);
    await fs.mkdir(directory, { recursive: true });
    const writeStream = fs.createWriteStream(this.filepath);

    try {
      const result = await fetch(this.downloadUrl);
      if (result.ok) {
        return new Promise((resolve, reject) =>
                           result.body
                           .pipe(writeStream)
                           .on('error', function(err) {
                             console.log(`Something went wrong while handling response of downloading file from ${this.downloadUrl}`);
                             console.log(err);
                             reject(err);
                           })
                           .on('finish', function() { resolve(); })
                          );
      } else {
        throw new Error(`Request to download file returned status code ${result.status}`);
      }
    } catch (e) {
      console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
      console.log(e);
      throw new Error(`Something went wrong while downloading file from ${this.downloadUrl}`);
    }
  }
}

async function syncDocuments(distributions) {
  const documents =  distributions.filter(d => d.attributes['type'] === DISTRIBUTION_ATTACHMENT_TYPE)
  .map(d => new Document({ downloadUrl: d.attributes['download-url'], file: d.attributes.subject }));

  let downloadCount = 0;
  for (let document of documents) {
    const result = await query(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      SELECT  ?file WHERE {
        <${document.file}> a nfo:FileDataObject .
        ?file nie:dataSource <${document.file}> .
      } LIMIT 1
    `);
    if (result.results.bindings.length) {
      const binding = result.results.bindings[0];
      document.locationUri = binding['file'].value;
      console.log(`Copying document <${document.downloadUrl}> from Themis to Valvas`);
      try {
        await document.download();
      } catch (e) {
        console.log(`Failed to download document <${document.downloadUrl}> from Themis to Valvas. This failure will not block the sync flow.`);
        console.log(e);
      }
      downloadCount++;
    }
  }
  console.log(`Copied ${downloadCount} files from Themis to Valvas`);
}

export { syncDocuments }
