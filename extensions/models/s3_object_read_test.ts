// deno-lint-ignore-file no-explicit-any -- Mock responses represent heterogeneous S3 SDK command outputs.
import { assertEquals, assertRejects } from "jsr:@std/assert@1.0.19";
import {
  downloadObject,
  GlobalArgumentsSchema,
  keyWithinPrefix,
  listObjects,
  type S3Sender,
  verifyReadAccess,
  withClient,
} from "./s3_object_read.ts";

Deno.test("credentials are optional as a pair for the AWS provider chain", () => {
  const base = {
    endpoint: "https://s3.example.com",
    bucket: "example-bucket",
  };
  assertEquals(GlobalArgumentsSchema.safeParse(base).success, true);
  assertEquals(
    GlobalArgumentsSchema.safeParse({ ...base, accessKeyId: "only-one" })
      .success,
    false,
  );
});

Deno.test("configuration rejects malformed bucket, prefix, key, and bounds", () => {
  const base = { endpoint: "https://s3.example.com", bucket: "example-bucket" };
  assertEquals(
    GlobalArgumentsSchema.safeParse({ ...base, bucket: "x" }).success,
    false,
  );
  assertEquals(
    GlobalArgumentsSchema.safeParse({ ...base, prefix: "bad\0prefix" }).success,
    false,
  );
  assertEquals(
    GlobalArgumentsSchema.safeParse({ ...base, maximumObjectBytes: 0 }).success,
    false,
  );
});

class FakeS3 implements S3Sender {
  commands: any[] = [];
  constructor(private responses: Array<any | Error>) {}
  send(command: any): Promise<any> {
    this.commands.push(command);
    const response = this.responses.shift();
    return response instanceof Error
      ? Promise.reject(response)
      : Promise.resolve(response);
  }
}

const stream = (bytes: number[]) =>
  new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(bytes));
      controller.close();
    },
  });

function fakeContext() {
  const files: Uint8Array[] = [];
  const resources: any[] = [];
  return {
    files,
    resources,
    context: {
      globalArgs: {
        accessKeyId: "redacted",
        secretAccessKey: "redacted",
        endpoint: "https://example.invalid",
        region: "us-east-1",
        bucket: "bucket",
        prefix: "approved/",
        forcePathStyle: true,
        maximumListObjects: 2,
        maximumObjectBytes: 8,
      },
      logger: { info() {} },
      createFileWriter: () => ({
        async writeStream(body: ReadableStream<Uint8Array>) {
          const chunks: number[] = [];
          for await (const chunk of body) chunks.push(...chunk);
          const content = new Uint8Array(chunks);
          files.push(content);
          return { kind: "file" };
        },
      }),
      writeResource(specName: string, name: string, data: unknown) {
        resources.push({ specName, name, data });
        return Promise.resolve({ kind: "resource" });
      },
    },
  };
}

Deno.test("verification is prefix-scoped and uses a one-byte range", async () => {
  const client = new FakeS3([{}, { Contents: [{ Key: "approved/object" }] }, {
    Body: stream([1]),
  }]);
  const result = await verifyReadAccess(client, "bucket", "approved/");
  assertEquals(result.getObjectVerified, true);
  assertEquals(client.commands[1].input, {
    Bucket: "bucket",
    Prefix: "approved/",
    MaxKeys: 1,
  });
  assertEquals(client.commands[2].input.Range, "bytes=0-0");
});

Deno.test("verification rejects malformed listings and sanitizes API errors", async () => {
  await assertRejects(
    () => verifyReadAccess(new FakeS3([{}, { Contents: [{}] }]), "bucket", ""),
    Error,
    "without a key",
  );
  const error = await assertRejects(
    () =>
      verifyReadAccess(
        new FakeS3([new Error("secretAccessKey=do-not-leak")]),
        "bucket",
        "",
      ),
    Error,
    "S3 HeadBucket failed",
  );
  assertEquals(error.message.includes("do-not-leak"), false);
});

Deno.test("listing never exceeds its requested maximum", async () => {
  const client = new FakeS3([{
    Contents: [{ Key: "approved/a", Size: 1 }, { Key: "approved/b", Size: 2 }],
    IsTruncated: true,
  }]);
  const result = await listObjects(client, "bucket", "approved/", 1);
  assertEquals(result.objects.map((object: { key: string }) => object.key), [
    "approved/a",
  ]);
  assertEquals(result.truncated, true);
});

Deno.test("listing rejects malformed object metadata instead of coercing size", async () => {
  const client = new FakeS3([{
    Contents: [{ Key: "approved/a", Size: "1" }],
  }]);
  await assertRejects(
    () => listObjects(client, "bucket", "approved/", 1),
    Error,
    "invalid Size metadata",
  );
});

Deno.test("client destruction runs after successful and failed operations", async () => {
  for (const failure of [false, true]) {
    let destroyed = false;
    const client: S3Sender = {
      send: () => Promise.resolve({}),
      destroy: () => {
        destroyed = true;
      },
    };
    if (failure) {
      await assertRejects(
        () => withClient(client, () => Promise.reject(new Error("failed"))),
        Error,
        "failed",
      );
    } else {
      await withClient(client, () => Promise.resolve("ok"));
    }
    assertEquals(destroyed, true);
  }
});

Deno.test("download rejects keys outside its configured prefix before network access", async () => {
  const fake = fakeContext();
  const client = new FakeS3([]);
  await assertRejects(
    () => downloadObject(client, fake.context, "private/object"),
    Error,
    "outside the configured prefix",
  );
  assertEquals(client.commands.length, 0);
  assertEquals(keyWithinPrefix("approved/object", "approved/"), true);
});

Deno.test("download refuses oversized content before GET or artifact output", async () => {
  const fake = fakeContext();
  const client = new FakeS3([{ ContentLength: 9 }]);
  await assertRejects(
    () => downloadObject(client, fake.context, "approved/object"),
    Error,
    "limit is 8 bytes",
  );
  assertEquals(client.commands.length, 1);
  assertEquals(fake.files.length, 0);
});

Deno.test("download enforces the stream limit when object metadata is inaccurate", async () => {
  const fake = fakeContext();
  const client = new FakeS3([{ ContentLength: 3 }, {
    ContentLength: 3,
    Body: stream([1, 2, 3, 4, 5, 6, 7, 8, 9]),
  }]);
  await assertRejects(
    () => downloadObject(client, fake.context, "approved/object"),
    Error,
    "exceeded the 8-byte limit",
  );
  assertEquals(fake.files.length, 0);
  assertEquals(fake.resources.length, 1);
  assertEquals(fake.resources[0].data.status, "pending");
});

Deno.test("download rejects short and HEAD/GET length mismatches", async () => {
  for (
    const get of [
      { ContentLength: 3, Body: stream([1, 2]) },
      { ContentLength: 2, Body: stream([1, 2]) },
    ]
  ) {
    const fake = fakeContext();
    const client = new FakeS3([{ ContentLength: 3 }, get]);
    await assertRejects(
      () => downloadObject(client, fake.context, "approved/object"),
      Error,
      get.ContentLength === 3 ? "byte count mismatch" : "changed between HEAD",
    );
    assertEquals(fake.files.length, 0);
  }
});

Deno.test("metadata failure prevents file persistence", async () => {
  const fake = fakeContext();
  fake.context.writeResource = () =>
    Promise.reject(new Error("metadata unavailable"));
  const client = new FakeS3([{ ContentLength: 3 }, {
    ContentLength: 3,
    Body: stream([1, 2, 3]),
  }]);
  await assertRejects(
    () => downloadObject(client, fake.context, "approved/object"),
    Error,
    "metadata unavailable",
  );
  assertEquals(fake.files.length, 0);
});

Deno.test("S3 failures add operation context without echoing sensitive messages", async () => {
  const fake = fakeContext();
  const client = new FakeS3([new Error("secretAccessKey=do-not-leak")]);
  const error = await assertRejects(
    () => downloadObject(client, fake.context, "approved/object"),
    Error,
    "S3 HeadObject failed",
  );
  assertEquals(error.message.includes("do-not-leak"), false);
});

Deno.test("download writes bounded content and metadata for an approved key", async () => {
  const fake = fakeContext();
  const client = new FakeS3([{ ContentLength: 3, ContentType: "text/plain" }, {
    ContentLength: 3,
    Body: stream([1, 2, 3]),
  }]);
  const result = await downloadObject(client, fake.context, "approved/object");
  assertEquals(fake.files[0], new Uint8Array([1, 2, 3]));
  assertEquals(fake.resources[1].data, {
    bucket: "bucket",
    key: "approved/object",
    size: 3,
    status: "complete",
    downloadedAt: fake.resources[1].data.downloadedAt,
  });
  assertEquals(result.dataHandles.length, 2);
});
