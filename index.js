/* jshint esnext: true */

var run = function (schema, options) {
	var r = (options.rethink || require('rethinkdbdash'))({ host: options.host, port: options.port, authKey: options.auth }),
		_ = require('lodash'),
		B = require('bluebird');
	if (typeof schema == 'function') schema = schema(r);

	function uncursorify(l) { if (Array.isArray(l)) { return l; } return l.toArray(); }

	return r.dbList().run().then(function(dbs) {	
		// Create databases
		var commands = _.map(_.difference(_.keys(schema), dbs), function(db) {
			return r.dbCreate(db).run();
		});
		if (options.clean) {
			commands = commands.concat(_.map(_.difference(dbs, _.keys(schema)), function(db) {
				return r.dbDrop(db).run();
			}));
		}
		return B.all(commands);
	}).then(function() {
		console.log('Created databases');
		return B.all(_.map(_.keys(schema), function(db) {
			var tables = r.db(db).tableList().run().then(uncursorify).then(function(tables) {
				var cmds = _.map(_.difference(_.keys(schema[db]), tables), function(table) {
					return r.db(db).tableCreate(table).run();
				});
				if (options.clean) {
					cmds = cmds.concat(_.map(_.difference(tables, _.keys(schema[db])), function(table) {
						return r.db(db).tableDrop(table).run();
					}));
				}
				return B.all(cmds);
			});
		}));
	}).then(function() {
		console.log('Created tables');
		// Go through each table.
		var commands = [];
		_.each(_.keys(schema), function(db) { _.each(_.keys(schema[db]), function(table) {
			// Create rows
			if (!_.isEmpty(schema[db][table].rows)) {
				var keys = _.pluck(schema[db][table].rows, 'id');
				// Find existing rows.
				commands.push(r.db(db).table(table).getAll.apply(r.db(db).table(table), keys).run().then(uncursorify).then(function(existing) {
					existing = _.pluck(existing, 'id');
					// Create uncreated rows, inserting in batch.
					if (_.difference(keys, existing).length) {
						return r.db(db).table(table).insert(_.map(_.difference(keys, existing), function(id) {
							return _.find(schema[db][table].rows, { id: id });
						})).run();
					}
					// Do not delete old rows.
				}));
			}

			// Create indexes.
			if (!_.isEmpty(schema[db][table].indexes)) {
				// Find existing indexes.
				var names = _.pluck(schema[db][table].indexes, 'name');
				commands.push(r.db(db).table(table).indexList().run().then(uncursorify).then(function(existing) {
					// Insert uncreated indexes.
					if (_.difference(names, existing).length) {
						return B.all(_.map(_.difference(names, existing), function(name) {
							var index = _.find(schema[db][table].indexes, { name: name });
							// Account for different syntaxes.
							if (index.options && index.func) return r.db(db).table(table).indexCreate(index.name, index.func, index.options).run();
							else if (index.options) return r.db(db).table(table).indexCreate(index.name, index.options).run();
							else if (index.func) return r.db(db).table(table).indexCreate(index.name, index.func).run();
							else return r.db(db).table(table).indexCreate(index.name).run();
						}));
					}
				}));
			}
		}); });
		return B.all(commands);
	}).then(function() {
		console.log('Inserted rows and created indexes');
	}).catch(function(e) {
		console.log(e, e.stack)
	}).return(r);
};

if (require.main == module) {
	var args = require('optimist')
		.boolean('help')
		.boolean('clean')
		.default({ host: 'localhost', port: 28015, auth: '' })
		.describe('host', 'RethinkDB host')
		.describe('port', 'RethinkDB port')
		.describe('auth', 'RethinkDB auth key')
		.describe('clean', 'Delete invalid databases/tables/indexes');

	if (args.argv.help) {
		args.showHelp();
		process.exit();
	}
	var fs = require('fs'), schema;
	if (fs.existsSync(process.cwd() + '/rethinkdb-schema.json'))
		schema = JSON.parse(fs.readFileSync(process.cwd() + '/rethinkdb-schema.json'));
	else if (fs.existsSync(process.cwd() + '/rethinkdb-schema.js') || fs.existsSync(process.cwd() + '/rethinkdb-schema.coffee'))
		schema = require(process.cwd() + '/rethinkdb-schema');
	else throw "No schema file found";

	run(schema, args.argv);
}
else {
	module.exports = run;
}

