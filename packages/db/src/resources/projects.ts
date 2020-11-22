import {logger} from "@truffle/db/logger";
const debug = logger("db:resources:projects");

import gql from "graphql-tag";
import graphql from "graphql";
import { delegateToSchema } from "graphql-tools";

import { Definition, IdObject, Workspace } from "./types";

export const projects: Definition<"projects"> = {
  createIndexes: [],
  idFields: ["directory"],
  typeDefs: gql`
    type Project implements Resource {
      id: ID!

      directory: String!

      contract(name: String!): Contract
      contracts: [Contract]!

      network(name: String!): Network
      networks: [Network]!

      contractInstance(
        contract: NameInput!
        network: NameInput!
      ): ContractInstance
      contractInstances(network: NameInput!): [ContractInstance]

      resolve(type: String, name: String): [NameRecord] # null means unknown type
    }

    input ProjectInput {
      directory: String!
    }

    input NameInput {
      name: String!
    }
  `,
  resolvers: {
    Project: {
      resolve: {
        resolve: async ({id}, {name, type}, {workspace}) => {
          debug("Resolving Project.resolve...");

          const results = await workspace.find("projectNames", {
            selector: {"project.id": id, name, type}
          });

          const nameRecordIds = results.map(({nameRecord: {id}}) => id);

          const result = await workspace.find("nameRecords", {
            selector: {
              id: {$in: nameRecordIds}
            }
          });

          debug("Resolved Project.resolve.");
          return result;
        }
      },
      network: {
        resolve: async (project, { name }, { workspace }) => {
          debug("Resolving Project.network...");

          const [nameRecord] = await resolve({
            project,
            name,
            type: "Network",
            workspace
          });

          if (!nameRecord) {
            return;
          }

          const { resource } = nameRecord;

          const result = await workspace.get("networks", resource.id);

          debug("Resolved Project.network.");
          return result;
        }
      },
      networks: {
        resolve: async (project, _, { workspace }) => {
          debug("Resolving Project.networks...");

          const nameRecords = await resolve({
            project,
            type: "Network",
            workspace
          });

          const resourceIds = nameRecords.map(({ resource }) => resource.id);

          const result = await workspace.find("networks", {
            selector: { id: { $in: resourceIds } }
          });

          debug("Resolved Project.networks.");
          return result;
        }
      },
      contract: {
        resolve: async (project, { name }, { workspace }) => {
          debug("Resolving Project.contract...");

          const [nameRecord] = await resolve({
            project,
            name,
            type: "Contract",
            workspace
          });

          if (!nameRecord) {
            return;
          }

          const { resource } = nameRecord;

          const result = await workspace.get("contracts", resource.id);

          debug("Resolved Project.contract.");
          return result;
        }
      },
      contracts: {
        resolve: async (project, _, { workspace }) => {
          debug("Resolving Project.contracts...");

          const nameRecords = await resolve({
            project,
            type: "Contract",
            workspace
          });

          const resourceIds = nameRecords.map(({ resource }) => resource.id);

          const result = await workspace.find("contracts", {
            selector: { id: { $in: resourceIds } }
          });

          debug("Resolved Project.contracts.");
          return result;
        }
      },
      contractInstance: {
        async resolve({ id }, args, { workspace }, info) {
          debug("Resolving Project.contractInstance...");

          const project = await delegateToSchema({
            schema: info.schema,
            operation: "query",
            fieldName: "project",
            returnType: info.schema.getType(
              "Project"
            ) as graphql.GraphQLOutputType,
            args: { id },
            selectionSet: extractSelectionSet(gql`{
              network(name: "${args.network.name}") {
                contractInstances {
                  id
                  contract {
                    id
                  }
                  network {
                    historicBlock {
                      height
                    }
                  }
                }
              }

              contractsNameRecords: resolve(
                type: "Contract"
                name: "${args.contract.name}"
              ) {
                history(includeSelf: true) {
                  resource {
                    id
                  }
                }
              }
            }`),
            context: { workspace },
            info
          });

          const {
            network,
            contractsNameRecords: [contractNameRecord]
          } = project;

          if (!network || !contractNameRecord) {
            return;
          }

          const { history: contractHistory } = contractNameRecord;

          const contractInstanceIdsByContractId = network.contractInstances
            .sort(
              (a, b) =>
                a.network.historicBlock.height - b.network.historicBlock.height
            )
            .map(({ id, contract }) => ({
              [contract.id]: id
            }))
            .reduce((a, b) => ({ ...a, ...b }), {});

          for (const {
            resource: { id }
          } of contractHistory) {
            const contractInstanceId = contractInstanceIdsByContractId[id];

            if (contractInstanceId) {
              debug("Resolved Project.contractInstance.");
              return await workspace.get(
                "contractInstances",
                contractInstanceId
              );
            }
          }
        }
      },
      contractInstances: {
        async resolve({ id }, args, { workspace }, info) {
          debug("Resolving Project.contractInstance...");

          const project = await delegateToSchema({
            schema: info.schema,
            operation: "query",
            fieldName: "project",
            returnType: info.schema.getType(
              "Project"
            ) as graphql.GraphQLOutputType,
            args: { id },
            selectionSet: extractSelectionSet(gql`{
              network(name: "${args.network.name}") {
                contractInstances {
                  id
                  contract {
                    id
                  }
                  network {
                    historicBlock {
                      height
                    }
                  }
                }
              }

              contractsNameRecords: resolve(type: "Contract") {
                history(includeSelf: true) {
                  resource {
                    id
                  }
                }
              }
            }`),
            context: { workspace },
            info
          });
          debug("project %o", project);

          const { network, contractsNameRecords } = project;

          const contractsHistory = contractsNameRecords.map(
            ({ history }) => history
          );

          if (!network) {
            return;
          }

          const contractInstanceIdsByContractId = network.contractInstances
            .sort(
              (a, b) =>
                a.network.historicBlock.height - b.network.historicBlock.height
            )
            .map(({ id, contract }) => ({
              [contract.id]: id
            }))
            .reduce((a, b) => ({ ...a, ...b }), {});

          const contractInstanceIds = [];
          for (const contractHistory of contractsHistory) {
            for (const {
              resource: { id }
            } of contractHistory) {
              const contractInstanceId = contractInstanceIdsByContractId[id];

              if (contractInstanceId) {
                contractInstanceIds.push(contractInstanceId);
                break;
              }
            }
          }

          const contractInstances = await workspace.find("contractInstances", {
            selector: { id: { $in: contractInstanceIds } }
          });

          debug("Resolved Project.contractInstance.");
          return contractInstances;
        }
      }
    }
  }
};

async function resolve(options: {
  project: IdObject<DataModel.Project>;
  name?: string;
  type?: string;
  workspace: Workspace;
}) {
  const {
    project: { id },
    name,
    type,
    workspace
  } = options;

  const results = await workspace.find("projectNames", {
    selector: { "project.id": id, name, type }
  });
  const nameRecordIds = results.map(({ nameRecord: { id } }) => id);
  const nameRecords = await workspace.find("nameRecords", {
    selector: {
      id: { $in: nameRecordIds }
    }
  });

  return nameRecords;
}

function extractSelectionSet(document) {
  return document.definitions
    .map(({ selectionSet }) => selectionSet)
    .find(selectionSet => selectionSet);
}
