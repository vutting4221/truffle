import { logger } from "@truffle/db/logger";
const debug = logger("db:project:migrate:networkGenealogies");

import gql from "graphql-tag";
import {
  IdObject,
  toIdObject,
  resources,
  Process
} from "@truffle/db/project/process";

/*
 * We must find nearest ancestor and/or nearest descendant for each Network.
 *
 * To do this, we first rely on the !precondition! that all given networks are
 * in the same genealogy tree.
 */

export function* generateNetworkGenealogiesLoad<
  N extends {
    block?: DataModel.Block;
    db?: {
      network: IdObject<DataModel.Network>;
    };
  }
>(options: {
  network: { networkId };
  artifacts: {
    networks?: {
      [networkId: string]: N | undefined;
    };
  }[];
}): Process<IdObject<DataModel.NetworkGenealogy>[]> {
  const {
    artifacts,
    network: { networkId }
  } = options;

  const artifactNetworks = artifacts
    .filter(({ networks }) => networks && networks[networkId])
    .map(({ networks }) => networks[networkId]);

  const { ancestor, descendant, networkGenealogies } = internalInputs(
    artifactNetworks
  );
  debug("networkGenealogies %o", networkGenealogies);

  const ancestorAncestor = yield* findRelation("ancestor", ancestor);
  debug("ancestorAncestor %o", ancestorAncestor);
  if (ancestorAncestor) {
    networkGenealogies.push({
      ancestor: ancestorAncestor,
      descendant: ancestor
    });
  }

  const descendantDescendant = yield* findRelation("descendant", descendant);
  debug("descendantDescendant %o", descendantDescendant);
  if (descendantDescendant) {
    networkGenealogies.push({
      ancestor: descendant,
      descendant: descendantDescendant
    });
  }

  return yield* resources.load("networkGenealogies", networkGenealogies);
}

const internalInputs = <
  N extends {
    block?: DataModel.Block;
    db?: {
      network: IdObject<DataModel.Network>;
    };
  }
>(
  artifactNetworks: (N | undefined)[]
):
  | {
      ancestor: IdObject<DataModel.Network>;
      descendant: IdObject<DataModel.Network>;
      networkGenealogies: DataModel.NetworkGenealogyInput[];
    }
  | undefined => {
  // ordered ascending
  const networks: IdObject<DataModel.Network>[] = artifactNetworks
    .filter(({ block, db: { network } = {} } = {} as N) => block && network)
    .sort((a, b) => a.block.height - b.block.height)
    .map(({ db: { network } }) => network);

  if (networks.length < 1) {
    return;
  }

  const { networkGenealogies } = networks.slice(1).reduce(
    (
      { ancestor, networkGenealogies },
      descendant
    ): {
      ancestor: IdObject<DataModel.Network>;
      networkGenealogies: DataModel.NetworkGenealogyInput[];
    } => ({
      ancestor: descendant,
      networkGenealogies: [...networkGenealogies, { ancestor, descendant }]
    }),
    { ancestor: networks[0], networkGenealogies: [] }
  );

  return {
    ancestor: networks[0], // first
    descendant: networks.slice(-1)[0], //last
    networkGenealogies
  };
};

function* findRelation(
  relation: "ancestor" | "descendant",
  network: IdObject<DataModel.Network>
): Process<IdObject<DataModel.Network | undefined>> {
  const query =
    relation === "ancestor" ? "possibleAncestors" : "possibleDescendants";
  let result: IdObject<DataModel.Network> | undefined;
  let candidates: DataModel.Network[];
  let alreadyTried: string[] = [];

  do {
    debug("finding %s", query);
    try {
      ({
        [query]: { networks: candidates, alreadyTried }
      } = yield* resources.get(
        "networks",
        network.id,
        gql`
          fragment Possible_${relation}s on Network {
            ${query}(alreadyTried: ${JSON.stringify(alreadyTried)}) {
              networks {
                id
                historicBlock {
                  hash
                  height
                }
              }
              alreadyTried {
                id
              }
            }
          }
        `
      ));
    } catch (error) {
      debug("error %o", error);
    }

    debug("candidates %o", candidates);

    for (const candidate of candidates) {
      const response = yield {
        type: "web3",
        method: "eth_getBlockByNumber",
        params: [candidate.historicBlock.height, false]
      };

      if (response && response.result && response.result.hash === candidate.historicBlock.hash) {
        result = toIdObject(candidate);
        break;
      }
    }
  } while (!result && candidates.length > 0);

  return result;
}
