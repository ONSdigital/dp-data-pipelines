Docs needed:
E2E tests

external_api_services.md#uploading-metadata - Is state/type being checked currently? GetDatasetResponse.can_publish_new_version not being called anywhere

state_management.md - Add section once retry Lambda available
"The retry Lambda (***TODO Link to retry lambda docs***) is configured to periodically check the database for failed submissions, and automatically rerun the pipeline on these submissions if certain conditions are met (***TODO What conditions***)."

Integration tests README - update with Upload Service mock details when merged