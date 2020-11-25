import assert from "assert";
import * as sinon from "sinon";
import { describe, it } from "mocha";

import { TruffleDB } from "../lib/sources/db";

describe("TruffleDB ResolverSource", function () {
  describe("resolve", function () {

    it("resolves the source for a contract", async function () {
      const workingDirectory = "/test";
      const contents = `pragma solidity ^0.7.0; contract A {}`;
      const sourcePath = "contracts/A.sol";

      const execute = sinon.stub();
      execute.onCall(0).returns(wrapSource({
        contents,
        sourcePath: `${workingDirectory}/${sourcePath}`
      }));

      const project = { id: "0xdeadbeef" };
      const connect = () => ({ execute });

      const db = new TruffleDB(workingDirectory, connect, project);

      const resolution = await db.resolve("contract:A", "");

      assert.ok(resolution, "failed to resolve `contract:A`");

      const { body, filePath } = resolution;
      assert.equal(body, contents);
      assert.equal(filePath, `contract:A/${sourcePath}`);
    });
  });

});


const wrapSource = ({
  sourcePath,
  contents
}: {
  sourcePath: string;
  contents: string;
}) => ({
  data: {
    project: {
      contract: {
        processedSource: {
          source: {
            contents,
            sourcePath
          }
        }
      }
    }
  }
});
