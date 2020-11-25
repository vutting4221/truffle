import debugModule from "debug";
const debug = debugModule("resolver:sources:db");

import path from "path";

import gql from "graphql-tag";
import { DocumentNode, ExecutionResult } from "graphql";
import { ContractObject } from "@truffle/contract-schema/spec";
import { Shims } from "@truffle/compile-common";

import { ResolverSource, SourceResolution } from "../source";

export class TruffleDB implements ResolverSource {
  workingDirectory: string;
  connect: () => Db;
  project: {
    id: string;
  };
  cache: {
    [contractName: string]: ContractObject
  };

  constructor(
    workingDirectory: string,
    connect: () => Db,
    project: { id: string }
  ) {
    debug("instantiating db resolver");
    this.workingDirectory = workingDirectory;
    this.connect = connect;
    this.project = project;

    this.cache = {};
    setImmediate(() => this.loadCache());
  }

  require(importPath: string): ContractObject | null {
    const { regex } = matchers.contract;

    const match = importPath.match(regex);
    if (!match) {
      return null;
    }

    const {
      groups: {
        contractName
      }
    } = match;

    const contract = this.cache[contractName];
    setImmediate(() => this.loadCache());

    debug("contract %O", contract);

    return contract;
  }

  private async loadCache() {
    const db = this.connect();

    await this.loadContractCache(db);
    await this.loadContractInstanceCache(db);
  }


  private async loadContractCache(db: Db) {
    const result = await db.execute(
      gql`
        ${contractFragment}

        query GetContractAndSources($projectId: ID!) {
          project(id: $projectId) {
            contracts {
              ...Contract
            }
          }
        }
      `,
      { projectId: this.project.id }
    );

    if (result.errors) {
      return;
    }

    const {
      data: {
        project: {
          contracts
        }
      }
    } = result;

    for (const {
      name: contractName,
      abi,
      callBytecode,
      createBytecode,
    } of contracts) {
      this.cache[contractName] = {
        contractName,
        abi: JSON.parse(abi.json),
        bytecode: Shims.NewToLegacy.forBytecode(createBytecode),
        deployedBytecode: Shims.NewToLegacy.forBytecode(callBytecode),
        networks: {}
      };
    }
  }


  private async loadContractInstanceCache(db: Db) {
    const result = await db.execute(
      gql`
        ${contractFragment}

        query GetContractAndSources($projectId: ID!) {
          project(id: $projectId) {
            contractInstances {
              address
              network {
                networkId
              }
              contract {
                name
                abi {
                  json
                }
              }
              creation {
                transactionHash
                constructor {
                  # don't bother getting the callBytecode, since
                  # network-object.spec.json doesn't distinguish links in
                  # separate bytecodes
                  createBytecode {
                    linkValues {
                      linkReference {
                        name
                      }
                      value
                    }
                  }
                }
              }
            }
          }
        }
      `,
      { projectId: this.project.id }
    );

    if (result.errors) {
      return;
    }

    const {
      data: {
        project: {
          contractInstances
        }
      }
    } = result;

    for (const {
      address,
      network: {
        networkId
      },
      creation: {
        transactionHash,
        constructor: {
          createBytecode: {
            linkValues
          }
        }
      },
      contract: {
        name: contractName,
        abi
      }
    } of contractInstances) {
      debug("loading contractInstance %o", contractName);
      const contract = this.cache[contractName];
      this.cache[contractName] = {
        ...contract,
        abi: contract.abi || JSON.parse(abi.json),
        networks: {
          ...contract.networks,
          [networkId]: {
            address,
            transactionHash,
            links: linkValues
              .map(({
                linkReference: {
                  name
                },
                value
              }: any) => ({
                [name]: value
              }))
              .reduce((a: any, b: any) => ({ ...a, ...b }), {})
          }
        }
      }
    }
  }

  async resolve(importPath: string, importedFrom: string): Promise<SourceResolution> {
    debug("importPath %s", importPath);
    debug("importedFrom %s", importedFrom);

    const checkMatch = (path: string) => Object.entries(matchers)
      .map(([matcher, { regex }]) => ([matcher, path.match(regex)]))
      .filter(([, match]) => match !== null)
      .map(([matcher, match]) => ({ matcher, groups: (match as any).groups }))
      [0];

    const { matcher, groups } = checkMatch(importPath) || {};

    debug("matcher %o", matcher);
    if (!matcher) {
      if (importedFrom && checkMatch(importedFrom)) {
        const sourcePath = path.join(path.dirname(importedFrom), importPath);
        debug("sourcePath %o", sourcePath);
        return this.resolve(sourcePath, importedFrom);
      }

      return {
        body: undefined,
        filePath: undefined
      };
    }

    switch (matcher) {
      case "contract": {
        const { contractName } = groups;

        const match = await this.findContract({ contractName });

        return match || {
          body: undefined,
          filePath: undefined
        };
      }
      case "contractSource": {
        const { contractName, sourcePath } = groups;
        const match = await this.findContract({ contractName, sourcePath });

        return match || {
          body: undefined,
          filePath: undefined
        };
      }
    }
  }

  // Here we're resolving from local files to local files, all absolute.
  resolveDependencyPath(importPath: string, dependencyPath: string) {
    return dependencyPath;
  }

  private async findContract(options: {
    contractName: string;
    sourcePath?: string;
  }) {
    const { contractName } = options;
    debug("matching contractName %s", contractName);
    debug("this.project.id %o", this.project.id);

    const db = this.connect();
    const result = await db.execute(
      gql`
        ${contractFragment}

        query GetContractAndSources(
          $projectId: ID!
          $contractName: String!
        ) {
          project(id: $projectId) {
            contract(name: $contractName) {
              ...Contract
            }
          }
        }
      `,
      { projectId: this.project.id, contractName }
    );
    debug("result %o", result);

    if (result.errors) {
      debug("result.errors %o", result.errors);
      return;
    }

    const {
      data: {
        project: {
          contract
        }
      }
    } = result;

    return contract;
  }

  private async resolveSource(options: {
    contract: {
      processedSource: {
        source: {
          contents: string;
          sourcePath: string;
        }
      };
      compilation: {
        sources: {
          contents: string;
          sourcePath: string;
        }[]
      };
    };
    contractName: string;
    sourcePath?: string;
  }) {
    const { contract, contractName } = options;
    debug("contract %O", contract);

    const filePathFor = (sourcePath: string) => `contract:${contractName}/${
      path.isAbsolute(sourcePath)
        ? path.relative(this.workingDirectory, sourcePath)
        : sourcePath
    }`;

    if (!options.sourcePath) {
      const {
        processedSource: {
          source: {
            contents: body,
            sourcePath
          }
        }
      } = contract;

      return {
        body,
        filePath: filePathFor(sourcePath)
      }
    }

    if (options.sourcePath) {
      const {
        compilation: {
          sources
        }
      } = contract;

      debug("sources %o", sources.map(
        (source?: { sourcePath: string }) =>
          source && filePathFor(source.sourcePath)
      ));

      const source = sources.find(
        (source?: { sourcePath: string }) =>
          source && filePathFor(source.sourcePath) === filePathFor(options.sourcePath)
      );

      if (source) {
        const {
          contents: body,
          sourcePath
        } = source;

        return {
          body,
          filePath: filePathFor(sourcePath)
        }
      }
    }

  }

}

const matchers = {
  contract: {
    regex: /^contract:(?<contractName>[^/]+)$/
  },
  contractSource: {
    regex: /^contract:(?<contractName>[^/]+)\/(?<sourcePath>.+)$/
  }
};

interface Db {
  execute: (
    request: DocumentNode | string,
    variables: any
  ) => Promise<ExecutionResult>;
}

const contractFragment = gql`
fragment Contract on Contract {
  name

  abi {
    json
  }

  processedSource {
    source {
      contents
      sourcePath
    }
    ast {
      json
    }
  }

  compilation {
    sources {
      contents
      sourcePath
    }
  }

  createBytecode {
    bytes
    linkReferences {
      name
      offsets
      length
    }
  }
  callBytecode {
    bytes
    linkReferences {
      name
      offsets
      length
    }
  }
}
`;


