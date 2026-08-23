// deno-lint-ignore-file no-explicit-any -- S3Sender abstracts command-specific SDK response types for injectable test doubles.
import { z } from "npm:zod@4.0.17";
import {
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "npm:@aws-sdk/client-s3@3.864.0";

const ABSOLUTE_MAX_LIST_OBJECTS = 1_000;
const ABSOLUTE_MAX_OBJECT_BYTES = 5_368_709_120;

function hasControlCharacter(value: string): boolean {
  return [...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0;
    return codePoint < 32 || codePoint === 127;
  });
}

function hasNul(value: string): boolean {
  return value.includes(String.fromCodePoint(0));
}

/**
 * Validates S3 connection settings, prefix scope, credentials, and read limits.
 * Credentials are marked sensitive for Swamp's secret handling.
 */
export const GlobalArgumentsSchema = z.object({
  accessKeyId: z.string().min(1).optional().meta({ sensitive: true }),
  secretAccessKey: z.string().min(1).optional().meta({ sensitive: true }),
  endpoint: z.url().refine((value) => new URL(value).protocol === "https:", {
    message: "endpoint must use HTTPS",
  }),
  region: z.string().min(1).default("us-east-1"),
  bucket: z.string().min(3).max(255).refine(
    (value) => !hasControlCharacter(value),
    { message: "bucket must not contain control characters" },
  ),
  prefix: z.string().max(1_024).refine(
    (value) => !hasNul(value),
    { message: "prefix must not contain NUL" },
  ).default(""),
  forcePathStyle: z.boolean().default(false),
  maximumListObjects: z.number().int().min(1).max(ABSOLUTE_MAX_LIST_OBJECTS)
    .default(100),
  maximumObjectBytes: z.number().int().positive().max(ABSOLUTE_MAX_OBJECT_BYTES)
    .default(104_857_600),
}).strict().refine(
  (args) => Boolean(args.accessKeyId) === Boolean(args.secretAccessKey),
  { message: "accessKeyId and secretAccessKey must be provided together" },
);

type GlobalArguments = z.infer<typeof GlobalArgumentsSchema>;

const ObjectMetadataSchema = z.object({
  key: z.string(),
  size: z.number().int().nonnegative(),
  lastModified: z.iso.datetime().nullable(),
  etag: z.string().nullable(),
  storageClass: z.string().nullable(),
}).strict();

const VerificationSchema = z.object({
  bucket: z.string(),
  prefix: z.string(),
  objectCountObserved: z.number().int().min(0).max(1),
  getObjectVerified: z.boolean(),
  checkedAt: z.iso.datetime(),
}).strict();

const ListingSchema = z.object({
  bucket: z.string(),
  prefix: z.string(),
  requestedMaximum: z.number().int().min(1).max(ABSOLUTE_MAX_LIST_OBJECTS),
  truncated: z.boolean(),
  objects: z.array(ObjectMetadataSchema).max(ABSOLUTE_MAX_LIST_OBJECTS),
  listedAt: z.iso.datetime(),
}).strict();

const DownloadMetadataSchema = z.object({
  bucket: z.string(),
  key: z.string(),
  size: z.number().int().nonnegative(),
  status: z.enum(["pending", "complete"]),
  downloadedAt: z.iso.datetime().nullable(),
}).strict();

/** Minimal injectable S3 client contract used by operations and test doubles. */
export interface S3Sender {
  send(command: unknown): Promise<any>;
  destroy?(): void;
}

interface ExecutionContext {
  globalArgs: GlobalArguments;
  logger: { info(message: string, properties?: Record<string, unknown>): void };
  writeResource(
    specName: string,
    instanceName: string,
    data: unknown,
  ): Promise<any>;
  createFileWriter(
    specName: string,
    instanceName: string,
    overrides?: Record<string, unknown>,
  ): {
    writeStream(stream: ReadableStream<Uint8Array>): Promise<any>;
  };
}

function createClient(args: GlobalArguments): S3Sender {
  return new S3Client({
    endpoint: args.endpoint,
    region: args.region,
    forcePathStyle: args.forcePathStyle,
    credentials: args.accessKeyId && args.secretAccessKey
      ? {
        accessKeyId: args.accessKeyId,
        secretAccessKey: args.secretAccessKey,
      }
      : undefined,
  });
}

/** Runs an S3 operation and always destroys the supplied client afterward. */
export async function withClient<T>(
  client: S3Sender,
  operation: (client: S3Sender) => Promise<T>,
): Promise<T> {
  try {
    return await operation(client);
  } finally {
    client.destroy?.();
  }
}

async function s3Request<T>(
  operation: string,
  request: () => Promise<T>,
): Promise<T> {
  try {
    return await request();
  } catch (error) {
    const code = typeof error === "object" && error !== null &&
        "name" in error && typeof error.name === "string"
      ? error.name.replace(/[^A-Za-z0-9_.-]/g, "").slice(0, 80)
      : "Error";
    throw new Error(`S3 ${operation} failed (${code})`, { cause: error });
  }
}

function objectMetadata(object: any): z.infer<typeof ObjectMetadataSchema> {
  if (typeof object?.Key !== "string") {
    throw new Error("S3 returned an object without a key");
  }
  if (!Number.isSafeInteger(object.Size) || object.Size < 0) {
    throw new Error(
      `S3 returned invalid Size metadata for object ${object.Key}`,
    );
  }
  if (
    object.LastModified !== undefined &&
    (!(object.LastModified instanceof Date) ||
      Number.isNaN(object.LastModified.getTime()))
  ) {
    throw new Error(
      `S3 returned invalid LastModified metadata for object ${object.Key}`,
    );
  }
  for (const field of ["ETag", "StorageClass"] as const) {
    if (object[field] !== undefined && typeof object[field] !== "string") {
      throw new Error(
        `S3 returned invalid ${field} metadata for object ${object.Key}`,
      );
    }
  }
  return ObjectMetadataSchema.parse({
    key: object.Key,
    size: object.Size,
    lastModified: object.LastModified?.toISOString() ?? null,
    etag: object.ETag ?? null,
    storageClass: object.StorageClass ?? null,
  });
}

function responseStream(body: any): ReadableStream<Uint8Array> {
  if (body instanceof ReadableStream) return body;
  if (typeof body?.transformToWebStream === "function") {
    return body.transformToWebStream();
  }
  throw new Error("S3 GetObject response did not contain a readable body");
}

function boundedStream(
  source: ReadableStream<Uint8Array>,
  maximumBytes: number,
  expectedBytes: number,
): ReadableStream<Uint8Array> {
  let bytes = 0;
  return source.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        bytes += chunk.byteLength;
        if (bytes > maximumBytes) {
          throw new Error(`S3 object exceeded the ${maximumBytes}-byte limit`);
        }
        controller.enqueue(chunk);
      },
      flush() {
        if (bytes !== expectedBytes) {
          throw new Error(
            `S3 object byte count mismatch: expected ${expectedBytes}, received ${bytes}`,
          );
        }
      },
    }),
  );
}

/** Returns whether an object key is inside the configured prefix scope. */
export function keyWithinPrefix(key: string, prefix: string): boolean {
  return key.startsWith(prefix);
}

/**
 * Checks bucket and object-read access with a one-item listing and, when an
 * object exists, a one-byte range read under the configured prefix.
 */
export async function verifyReadAccess(
  client: S3Sender,
  bucket: string,
  prefix: string,
) {
  await s3Request(
    "HeadBucket",
    () => client.send(new HeadBucketCommand({ Bucket: bucket })),
  );
  const listing = await s3Request("ListObjectsV2", () =>
    client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        MaxKeys: 1,
      }),
    ));
  const first = listing.Contents?.[0];
  if (first !== undefined && typeof first?.Key !== "string") {
    throw new Error("S3 returned an object without a key");
  }
  if (first?.Key) {
    const response = await s3Request("GetObject range probe", () =>
      client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: first.Key,
          Range: "bytes=0-0",
        }),
      ));
    const reader = responseStream(response.Body).getReader();
    try {
      const read = await reader.read();
      if (!read.done && read.value.byteLength > 1) {
        throw new Error("S3 ignored the bounded one-byte range request");
      }
    } finally {
      await reader.cancel();
    }
  }
  return {
    bucket,
    prefix,
    objectCountObserved: first?.Key ? 1 : 0,
    getObjectVerified: Boolean(first?.Key),
    checkedAt: new Date().toISOString(),
  };
}

/** Lists at most `maximum` object metadata records under a prefix. */
export async function listObjects(
  client: S3Sender,
  bucket: string,
  prefix: string,
  maximum: number,
) {
  const response = await s3Request("ListObjectsV2", () =>
    client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        MaxKeys: maximum,
      }),
    ));
  const objects = (response.Contents ?? []).slice(0, maximum).map(
    objectMetadata,
  );
  return {
    bucket,
    prefix,
    requestedMaximum: maximum,
    truncated: Boolean(response.IsTruncated) ||
      (response.Contents?.length ?? 0) > maximum,
    objects,
    listedAt: new Date().toISOString(),
  };
}

/**
 * Downloads one prefix-scoped object into a sensitive Swamp file artifact,
 * enforcing metadata and streaming byte limits before persisting metadata.
 */
export async function downloadObject(
  client: S3Sender,
  context: ExecutionContext,
  key: string,
) {
  if (!keyWithinPrefix(key, context.globalArgs.prefix)) {
    throw new Error("Object key is outside the configured prefix");
  }
  const head = await s3Request("HeadObject", () =>
    client.send(
      new HeadObjectCommand({ Bucket: context.globalArgs.bucket, Key: key }),
    ));
  const size = head.ContentLength;
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error("S3 object has no valid Content-Length");
  }
  if (size > context.globalArgs.maximumObjectBytes) {
    throw new Error(
      `S3 object is ${size} bytes; limit is ${context.globalArgs.maximumObjectBytes} bytes`,
    );
  }
  const response = await s3Request("GetObject", () =>
    client.send(
      new GetObjectCommand({ Bucket: context.globalArgs.bucket, Key: key }),
    ));
  const getSize = response.ContentLength;
  if (!Number.isSafeInteger(getSize) || getSize < 0) {
    throw new Error("S3 GetObject response has no valid Content-Length");
  }
  if (getSize !== size) {
    throw new Error(
      `S3 object Content-Length changed between HEAD (${size}) and GET (${getSize})`,
    );
  }
  if (getSize > context.globalArgs.maximumObjectBytes) {
    throw new Error(
      `S3 object is ${getSize} bytes; limit is ${context.globalArgs.maximumObjectBytes} bytes`,
    );
  }
  const pendingMetadata = {
    bucket: context.globalArgs.bucket,
    key,
    size: getSize,
    status: "pending" as const,
    downloadedAt: null,
  };
  // Establish metadata before creating a file. If a later stream or final
  // metadata write fails, no file exists without a persisted recovery marker.
  await context.writeResource(
    "downloadMetadata",
    "download-current",
    pendingMetadata,
  );
  const writer = context.createFileWriter("object", "object-current", {
    contentType: head.ContentType ?? "application/octet-stream",
  });
  const fileHandle = await writer.writeStream(
    boundedStream(
      responseStream(response.Body),
      context.globalArgs.maximumObjectBytes,
      getSize,
    ),
  );
  const metadataHandle = await context.writeResource(
    "downloadMetadata",
    "download-current",
    {
      bucket: context.globalArgs.bucket,
      key,
      size: getSize,
      status: "complete",
      downloadedAt: new Date().toISOString(),
    },
  );
  return { dataHandles: [fileHandle, metadataHandle] };
}

/**
 * Swamp model definition for bounded, least-privilege reads from an
 * S3-compatible bucket, including verification, listing, and download methods.
 */
export const model = {
  type: "@mgreten/s3-object-read",
  version: "2026.08.23.1",
  globalArguments: GlobalArgumentsSchema,
  resources: {
    verification: {
      description: "Read-access verification metadata",
      schema: VerificationSchema,
      lifetime: "30d" as const,
      garbageCollection: 10,
    },
    objectListing: {
      description:
        "Bounded object metadata listing; object names may be sensitive",
      schema: ListingSchema,
      lifetime: "30d" as const,
      garbageCollection: 10,
    },
    downloadMetadata: {
      description: "Downloaded object metadata; object names may be sensitive",
      schema: DownloadMetadataSchema,
      lifetime: "30d" as const,
      garbageCollection: 10,
    },
  },
  files: {
    object: {
      description: "Bounded downloaded object; content may be sensitive",
      contentType: "application/octet-stream",
      sensitiveOutput: true,
      lifetime: "7d" as const,
      garbageCollection: 3,
    },
  },
  methods: {
    verifyReadAccess: {
      description:
        "HEAD the bucket, list one prefix-scoped object, and range-read one byte when nonempty",
      arguments: z.object({}).strict(),
      execute: async (
        _args: Record<string, never>,
        context: ExecutionContext,
      ) => {
        context.logger.info("Verifying S3 read access", {
          prefixScoped: context.globalArgs.prefix.length > 0,
        });
        const result = await withClient(
          createClient(context.globalArgs),
          (client) =>
            verifyReadAccess(
              client,
              context.globalArgs.bucket,
              context.globalArgs.prefix,
            ),
        );
        context.logger.info("Verified S3 read access", {
          objectCountObserved: result.objectCountObserved,
        });
        return {
          dataHandles: [
            await context.writeResource(
              "verification",
              "verification-current",
              result,
            ),
          ],
        };
      },
    },
    listObjects: {
      description:
        "Persist a bounded, prefix-scoped listing of object metadata; never object contents",
      arguments: z.object({
        maximum: z.number().int().min(1).max(ABSOLUTE_MAX_LIST_OBJECTS)
          .optional(),
      }).strict(),
      execute: async (
        args: { maximum?: number },
        context: ExecutionContext,
      ) => {
        const maximum = Math.min(
          args.maximum ?? context.globalArgs.maximumListObjects,
          context.globalArgs.maximumListObjects,
        );
        context.logger.info("Listing S3 objects", {
          prefixScoped: context.globalArgs.prefix.length > 0,
          maximum,
        });
        const result = await withClient(
          createClient(context.globalArgs),
          (client) =>
            listObjects(
              client,
              context.globalArgs.bucket,
              context.globalArgs.prefix,
              maximum,
            ),
        );
        context.logger.info("Listed S3 objects", {
          objectCount: result.objects.length,
          truncated: result.truncated,
        });
        return {
          dataHandles: [
            await context.writeResource(
              "objectListing",
              "object-listing-current",
              result,
            ),
          ],
        };
      },
    },
    downloadObject: {
      description:
        "Download one explicitly named, prefix-scoped object as a bounded artifact; contents may be sensitive",
      arguments: z.object({
        key: z.string().min(1).max(1_024).refine(
          (value) => !hasNul(value),
          { message: "key must not contain NUL" },
        ),
      }).strict(),
      execute: async (args: { key: string }, context: ExecutionContext) => {
        context.logger.info("Downloading bounded S3 object", {
          maximumObjectBytes: context.globalArgs.maximumObjectBytes,
        });
        const result = await withClient(
          createClient(context.globalArgs),
          (client) => downloadObject(client, context, args.key),
        );
        context.logger.info("Downloaded bounded S3 object");
        return result;
      },
    },
  },
};
