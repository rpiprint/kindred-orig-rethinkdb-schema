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

var fs = require('fs'), r = require('rethinkdbdash')({ host: args.argv.host, port: args.argv.port, authKey: args.argv.auth }), co = require('co'), _ = require('lodash');

var schema;
if (fs.existsSync(process.cwd() + '/rethinkdb-schema.json'))
	schema = JSON.parse(fs.readFileSync(process.cwd() + '/rethinkdb-schema.json'));
else if (fs.existsSync(process.cwd() + '/rethinkdb-schema.js') || fs.existsSync(process.cwd() + '/rethinkdb-schema.coffee'))
	schema = require(process.cwd() + '/rethinkdb-schema');
else throw "No schema file found";

var clean = args.argv.clean;

co(function* () {
	try {

	var dbs = yield (yield r.dbList().run()).toArray();

	// Create databases
	var commands = _.map(_.difference(_.keys(schema), dbs), function(db) { return r.dbCreate(db).run(); });
	if (clean) commands = commands.concat(_.map(_.difference(dbs, _.keys(schema)), function(db) { return r.dbDrop(db).run(); }));
	yield commands;
	console.log('Created databases');

	// Create tables
	yield _.map(_.keys(schema), function(db) {
		return function* () {
			var tables = yield (yield r.db(db).tableList().run()).toArray();

			var cmds = _.map(_.difference(_.keys(schema[db]), tables), function(table) { return r.db(db).tableCreate(table).run(); });
			if (clean) cmds = cmds.concat(_.map(_.difference(tables, _.keys(schema[db])), function(table) { return r.db(db).tableDrop(table).run(); }));
			yield cmds;
		};
	});
	console.log('Created tables');

	// Go through each table.
	commands = [];
	_.each(_.keys(schema), function(db) {
		_.each(_.keys(schema[db]), function(table) {
			// Create rows
			if (!_.isEmpty(schema[db][table].rows)) commands.push(function* () {
				// Find existing rows.
				var keys = _.pluck(schema[db][table].rows, 'id');
				var existing = _.pluck(yield (yield r.db(db).table(table).getAll(keys).run()).toArray(), 'id');

				// Create uncreated rows, inserting in batch.
				if (_.difference(keys, existing).length)
					yield r.db(db).table(table).insert(_.map(_.difference(keys, existing), function(id) {
						return _.find(schema[db][table].rows, { id: id });
					})).run();
			});

			// Create indexes
			if (!_.isEmpty(schema[db][table].indexes)) commands.push(function* () {
				// Find existing indexes.
				var names = _.pluck(schema[db][table].indexes, 'name');
				var existing = yield (yield r.db(db).table(table).indexList().run()).toArray();
				// Insert uncreated indexes.
				if (_.difference(names, existing).length)
					yield _.map(_.difference(names, existing), function(name) {
						var index = _.find(schema[db][table].indexes, { name: name });
						// Account for different syntaxes.
						if (index.options && index.func) return r.db(db).table(table).indexCreate(index.name, index.func, index.options).run();
						else if (index.options) return r.db(db).table(table).indexCreate(index.name, index.options).run();
						else if (index.func) return r.db(db).table(table).indexCreate(index.name, index.func).run();
						else return r.db(db).table(table).indexCreate(index.name).run();
					});
			});
		})
	});
	yield commands;
	console.log('Inserted rows and created indexes');

	r.getPool().drain();

	} catch(e) { console.warn(e) }
})();
