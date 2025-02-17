# Constants for AWS commands
s3Bucket="dp-sandbox-whl-package-upload"
wheelPath="dist/dpypelines-0.1.0-py3-none-any.whl"
wheelKey="dpypelines-0.1.0-py3-none-any.whl"

# Build package with poetry. Default output is dist/
poetry build

# Put object to S3 bucket
aws s3api put-object --bucket $s3Bucket --body $wheelPath --key $wheelKey

rm -r dist