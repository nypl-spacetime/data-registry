var fs = require('fs');
var path = require('path');
var H = require('highland');
var JSONStream = require('JSONStream');

var writeLine = function(writer, obj, callback) {
  writer.writeObject(obj, function(err) {
    callback(err);
  });
};

function ndjson(obj) {
  return {
    type: 'relation',
    obj: {
      from: 'http://data.nypl.org/terms/' + obj.registry,
      to: 'http://id.worldcat.org/fast/'+ obj.fast,
      type: 'hg:sameAs'
    }
  }
}

// function download(config, dir, writer, callback) {
// }

function convert(config, dir, writer, callback) {
  var inputStream = fs.createReadStream(path.join(__dirname, 'terms_geographic.json'))
    .pipe(JSONStream.parse());

  H(inputStream)
    .filter(o => {
      return o.fast === parseInt(o.fast, 10);
    })
    .map(ndjson)
    .flatten()
    .map(H.curry(writeLine, writer))
    .nfcall([])
    .series()
    .errors(function(err){
      console.error(err);
    })
    .done(function() {
      callback();
    });
}

// ==================================== API ====================================

module.exports.steps = [
  // download,
  convert
];
