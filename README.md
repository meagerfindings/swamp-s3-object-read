# S3 Object Read

`@mgreten/s3-object-read` provides bounded, read-only access to an S3-compatible
bucket. It supports AWS S3 and compatible HTTPS endpoints.

## Methods

- `verifyReadAccess`: heads the bucket, lists one object under the configured
  prefix, then range-reads one byte when an object exists.
- `listObjects`: persists bounded metadata only; it never persists object
  content.
- `downloadObject`: downloads one explicitly named, prefix-scoped object to a
  bounded Swamp file artifact. It requires matching HEAD and GET content
  lengths, verifies the streamed byte count, and writes a pending metadata
  recovery marker before file persistence.

## Configuration

```yaml
type: "@mgreten/s3-object-read"
globalArguments:
  # Optional together; omit both to use the standard AWS credential chain.
  accessKeyId: "${{ vault.get(my-vault, S3_ACCESS_KEY_ID) }}"
  secretAccessKey: "${{ vault.get(my-vault, S3_SECRET_ACCESS_KEY) }}"
  endpoint: "https://s3.example.com"
  region: "us-east-1"
  bucket: "example-bucket"
  prefix: "exports/"
  maximumListObjects: 100
  maximumObjectBytes: 104857600
```

`endpoint`, `bucket`, `prefix`, and both limits are configurable. Set
`forcePathStyle` for compatible endpoints that require path-style addressing.
Omit both credential fields to use IAM roles, instance profiles, or the
standard AWS environment credential chain; if explicit credentials are used,
both fields are required and treated as sensitive.
The configured prefix scopes every list and download operation; use an empty
prefix only when the backing credential is already restricted to the intended
object keys.

## Least privilege and sensitive artifacts

Use credentials that grant only `s3:HeadBucket`, `s3:ListBucket`,
`s3:GetObject`, and `s3:HeadObject`, restricted to the configured bucket and
prefix. This extension never writes, deletes, copies, tags, or mutates objects.

`downloadObject` writes object content to Swamp-managed file storage. Object
contents may include credentials, personal data, backups, or other sensitive
artifacts. Download only approved keys, choose the smallest practical byte
limit, restrict access to the resulting file artifact, and do not log or share
its contents. Metadata listings can also reveal sensitive object names.

## Usage

After creating a model with the configuration above, invoke methods through
Swamp. Verification performs a minimal read probe; listing returns metadata up
to the lower of the method request and configured maximum; downloading requires
the full object key, including the configured prefix.

```sh
swamp model method run archive-read verifyReadAccess
swamp model method run archive-read listObjects --input maximum=25
swamp model method run archive-read downloadObject \
  --input key="exports/approved-report.json"
```

Treat downloaded file handles as sensitive even when an object is expected to
be public: bucket contents can change independently of model configuration.
The prefix check is defense in depth, not a replacement for an IAM policy.
Restrict `s3:ListBucket` with an `s3:prefix` condition and restrict
`s3:GetObject` to the same key path. Keep the configured HTTPS endpoint under
administrative control; do not send credentials to an untrusted compatibility
service.

Licensed under the MIT License. See [LICENSE](LICENSE).
