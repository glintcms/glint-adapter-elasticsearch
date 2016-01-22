/**
 * Module dependencies.
 */
var debug = require('debug')('glint-adapter-elasticsearch');
var merge = require('utils-merge');
var request = require('superagent');
var config = require('./config');

/**
 * superagent consistency fix
 *
 * @link https://github.com/visionmedia/superagent/issues/19
 *
 */
request.Request.prototype.endWithoutErr = request.Request.prototype.end;
request.Request.prototype.end = function(fn) {
  this.endWithoutErr(function(res) {
    if (fn.length < 2) return fn(res);
    if (res.ok) {
      fn(null, res);
    } else {
      fn(res.text);
    }
  });
};

/**
 * Initialize a new `ElasticsearchAdapter` element.
 */
function ElasticsearchAdapter(options) {
  if (!(this instanceof ElasticsearchAdapter)) return new ElasticsearchAdapter(options);
  merge(this, config);
  merge(this, options);
}

/**
 * API functions.
 */
ElasticsearchAdapter.prototype.api = ElasticsearchAdapter.api = 'adapter-provider';

ElasticsearchAdapter.prototype.provider = ElasticsearchAdapter.provider = 'elasticsearch';

ElasticsearchAdapter.prototype.load = function(db, type, id, fn) {
  var path = this.getPath(db, type, id);
  fn = fn || noop();
  debug('es load', path);
  request
    .get(path)
    .set('Accept', 'application/json')
    .end(function(err, res) {
      if (err) return fn(err);
      if (res && res.body && res.body._source) return fn(null, res.body._source);
      return fn();
    });
};

ElasticsearchAdapter.prototype.find = function(db, type, query, fn) {
  var path = this.getPath(db, type, '_search');
  fn = fn || noop();
  debug('es find', path);
  request
    .get(path)
    .send(query)
    .set('Accept', 'application/json')
    .end(function(err, res) {
      if (err) return fn(err);
      if (res && res.body && res.body.hits && res.body.hits.hits) {
        var hits = res.body.hits.hits;
        var result = hits.map(function(hit) {
          return hit._source;
        });
        return fn(null, result);
      } else {
        return fn(null, []);
      }
    });
};

ElasticsearchAdapter.prototype.save = function(db, type, id, content, fn) {
  var path = this.getPath(db, type, id);
  fn = fn || noop();
  debug('es save', path);
  request
    .post(path)
    .send(content)
    .set('Accept', 'application/json')
    .end(function(err, res) {
      if (err) return fn(err);
      if (res && res.body && res.body._source) return fn(null, res.body._source);
      return fn();
    });
};

ElasticsearchAdapter.prototype.delete = function(db, type, id, fn) {
  var path = this.getPath(db, type, id);
  fn = fn || noop();
  debug('es delete', path);
  request
    .del(path)
    .set('Accept', 'application/json')
    .end(function(err, res) {
      if (err) return fn(err);
      if (res && res.ok && res.ok == true) return fn(null, true);
      return fn(null, false);
    });
};

/**
 * Helper functions.
 */
ElasticsearchAdapter.prototype.getPath = function(db, type, id) {
  debug('es getPath', db, type, id);

  var path = [this.address, db, type, id];
  path = path.map(function(val) {
    return val.toLowerCase();
  });
  path = path.join('/');
  return path;
};

function noop() {
}

/**
 * Expose ElasticsearchAdapter element.
 */
exports = module.exports = ElasticsearchAdapter;