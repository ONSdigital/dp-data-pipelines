# Schemas

## Manifest + Metadata file specifications

We have an OpenAPI schema definition for the metadata.json and manifest.json files at [openapi-specification.yaml](./openapi-specification.yaml)

The definition contains schemas for the 2 files. It also contains 2 mock routes, for non-existent API end-points. This allows you to easier view/create an example JSON for each file type.

### Viewing

The best way to view the schema is using [Swagger Editor](https://editor.swagger.io/):
1. Go to [Swagger Editor](https://editor.swagger.io/)
2. Copy the URL for the raw schema definition by [opening it in GitHub](./openapi-specification.yaml), and then clicking the `Raw` button just above the document in the top right
3. Click `File` -> `Import URL`
4. Paste in the URL from step 2 and click `OK`