import { updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs-extra';
import { Parser } from 'n3';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import {
  BATCH_SIZE
} from '../config';

async function insertTriplesAndGenerateTtlFile(triples, graph, path) {
  const statements = triples.map(triple => toTripleStatement(triple));
  await insertStatements(statements, graph);
  await fs.writeFile(path, statements.join('\n'));
}

async function deleteTriplesFromTtl(ttlFile, graph) {
  console.log(`Removing all triples from TTL ${ttlFile} in graph <${graph}>`);
  const ttl = fs.readFileSync(ttlFile, { encoding: 'utf-8' });
  const triples = await parseTtl(ttl);
  await deleteTriples(triples, graph);
}

async function parseTtl(file) {
  return (new Promise((resolve, reject) => {
    const parser = new Parser();
    const triples = [];
    parser.parse(file, (error, triple) => {
      if (error) {
        reject(error);
      } else if (triple) {
        triples.push(triple);
      } else {
        resolve(triples);
      }
    });
  }));
}

async function insertStatements(statements, graph) {
  for (let i = 0; i < statements.length; i += BATCH_SIZE) {
    console.log(`Inserting statements in batch: ${i}-${i + BATCH_SIZE}`);
    const batch = statements.slice(i, i + BATCH_SIZE).join('\n');
    await update(`
      INSERT DATA {
        GRAPH <${graph}> {
            ${batch}
        }
      }
    `);
  }
}

async function deleteTriples(triples, graph) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Deleting triples in batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = batch.map(b => toTripleStatement(b)).join('\n');
    await update(`
      DELETE DATA {
        GRAPH <${graph}> {
          ${statements}
        }
      }
    `);
  }
}

function toTripleStatement(triple) {
  const escape = function (node) {
    const { termType, value, datatype, "xml:lang": lang } = node;
    if (termType == "NamedNode") {
      return sparqlEscapeUri(value);
    } else if (termType == "Literal") {
      // We ignore xsd:string datatypes because Virtuoso doesn't treat those as default datatype
      // Eg. SELECT * WHERE { ?s mu:uuid "4983948" } will not return any value if the uuid is a typed literal
      // Since the n3 npm library used by the producer explicitely adds xsd:string on non-typed literals
      // we ignore the xsd:string on ingest
      if (datatype && datatype.value && datatype.value != 'http://www.w3.org/2001/XMLSchema#string')
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype.value)}`;
      else if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    } else
      console.log(`Don't know how to escape type ${termType}. Will escape as a string.`);
    return sparqlEscapeString(value);
  };

  const subject = escape(triple['subject']);
  const predicate = escape(triple['predicate']);
  const object = escape(triple['object']);

  return `${subject} ${predicate} ${object} .`;
}

export {
  deleteTriplesFromTtl, insertTriplesAndGenerateTtlFile, parseTtl
};