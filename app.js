const fs = require('fs');
const CsvReadableStream = require('csv-reader');
const csvWriter = require('csv-write-stream');
const _ = require('lodash');

let fileName = process.argv.length > 2 && process.argv[process.argv.length - 1] || 'SampleCSVFile_11kb.csv';
const inputStream = fs.createReadStream(fileName, 'utf8');
let writer;
let headers = [];

inputStream
    .pipe(CsvReadableStream({ parseNumbers: true, parseBooleans: true, trim: true }))
    .on('data', (row) => {
        // init headers from the first row
        if(headers.length === 0) {
            writer = csvWriter({ headers: row});
            writer.pipe(fs.createWriteStream('out.csv'));
        }

        // create hash map to the words with indexing 
        let mappedRow = row.reduce((acc, curr, index) => {
            acc[curr]
                ? acc[curr].push(index)
                : acc[curr] = [index];
            return acc;
        }, {});

        let outRowArr = [];
        _.forEach(mappedRow, (indexes, key) => {
            // in case we have duplicated words
            indexes.length > 1
                ? indexes.forEach((duplicateIndex, index) => {
                    index === 0
                        ? outRowArr[duplicateIndex] = key
                        : outRowArr[duplicateIndex] = indexes[0] // first index
                })
                : outRowArr[indexes[0]] = key;
        });

        writer.write(outRowArr);
    })
    .on('end', (data) => {
        console.log('No more rows!');
        writer.end();
    });