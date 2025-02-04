Feature: Data Ingress v1
  As a data engineer I want a configurable pipeline that:
  - can confirm correct configuration and inputs
  - can send notifications in the event of issues
  - can successfully transform inputs into expected outputs
  - will call the required APIs to pass along these outputs
  - will notify publishing that a resource is ready for publishing

  Scenario: Generic ingress runs without errors
    Given a temporary source directory of files
<<<<<<< HEAD
      | file          | fixture                        |
      | data.xml      | esa2010_test_data_short.xml    |
      | manifest.json | valid_manifest.json            |
      | metadata.json | test_metadata.json             |
    And a dataset id of 'valid_generic_file_ingress'
    And generic_file_ingress_v1 starts using the temporary source directory
=======
        | file          | fixture                        |
        | data.xml      | esa2010_test_data_short.xml    |
        | manifest.json | valid_manifest.json            |
        | metadata.json | test_metadata.json             |
    And a dataset id of 'valid_generic_file_ingress_xml'
    And dataset_ingress_v1 starts using the temporary source directory
>>>>>>> sandbox
    Then the pipeline should generate no errors


  Scenario: Generic ingress using CSV runs without errors.
    Given a temporary source directory of files
<<<<<<< HEAD
      | file          | fixture                        |
      | data.json     | test_data.json                 |
      | manifest.json | valid_manifest.json            |
    And a dataset id of 'valid_generic_file_ingress_json'
    And generic_file_ingress_v1 starts using the temporary source directory
=======
        | file          | fixture                        |
        | data.csv      | test_data.csv                  |
        | manifest.json | valid_manifest.json            |
        | metadata.json | test_metadata.json             |
    And a dataset id of 'valid_generic_file_ingress_csv'
    And dataset_ingress_v1 starts using the temporary source directory
>>>>>>> sandbox
    Then the pipeline should generate no errors


  Scenario: Pipeline runs with an expected error.
    Given a temporary source directory of files
      | file          | fixture               |
      | data.xml      | esa2010_test_data.xml |
      | manifest.json | valid_manifest.json   |
    And a dataset id of 'invalid'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate an error with a message containing "Required file not found: /home/runner/work/dp-data-pipelines/dp-data-pipelines/temporary_output_directory/temporary-data-fixtures/metadata.json"