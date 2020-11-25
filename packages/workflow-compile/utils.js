const debug = require("debug")("workflow-compile:utils");
const Config = require("@truffle/config");
const expect = require("@truffle/expect");
const Resolver = require("@truffle/resolver");
const Artifactor = require("@truffle/artifactor");

const {connect, Project} = require("@truffle/db");

async function prepareConfig(options) {
  expect.options(options, ["contracts_build_directory"]);

  expect.one(options, ["contracts_directory", "files"]);

  // Use a config object to ensure we get the default sources.
  const config = Config.default().merge(options);

  config.compilersInfo = {};

  if (config.db && !config.db.connect && config.db.enabled) {
    debug("enabling db resolver");
    config.db.connect = () => connect(config);

    const { id } = await Project.initialize({
      db: connect(config),
      project: {
        directory: config.working_directory
      }
    });

    config.db.project = { id };
  }

  if (!config.resolver) config.resolver = new Resolver(config);

  if (!config.artifactor) {
    config.artifactor = new Artifactor(config.contracts_build_directory);
  }

  return config;
}

function multiPromisify(func) {
  return (...args) =>
    new Promise((accept, reject) => {
      const callback = (err, ...results) => {
        if (err) reject(err);

        accept(results);
      };

      func(...args, callback);
    });
}

module.exports = {
  prepareConfig,
  multiPromisify,
};
