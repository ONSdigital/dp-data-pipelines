Feature: Data Ingress v1
  As a data engineer I want a configurable pipeline that:
  - can confirm correct configuration and inputs
  - can send notifications in the event of issues
  - can successfully transform inputs into expected outputs
  - will call the required APIs to pass along these outputs
  - will notify publishing that a resource is ready for publishing

  # TODO - this will grow in time to encapsulate the happy path for the above
  Scenario: SDMX 2.0 runs without errors
    Given a temporary source directory of files
        | file          | fixture                     |
        | data.xml      | esa2010_test_data_short.xml |
        | manifest.json | valid_manifest.json         |
    And a dataset id of 'valid_no_supp_dist_2_0'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate no errors
    And I read the csv output 'data.csv'
    And the csv output should have '24' rows
    And the csv output has the columns
          | ID | Test | Name xml:lang |
    And I read the metadata output 'metadata.json'
    And the metadata should match 'fixtures/correct_metadata_2_0.json'
    And the backend receives a request to "/upload-new"
    And the csv payload received should contain "temp-file-part-1"
    And the headers received should match
        | key            | value                   |
        | Authorization  | Bearer not-a-real-token |

  Scenario: SDMX 2.1 runs without errors
    Given a temporary source directory of files
        | file          | fixture             |
        | data.xml      | esa10_sdmx21.xml    |
        | manifest.json | valid_manifest.json |
    And a dataset id of 'valid_no_supp_dist_2_1'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate no errors
    Then I read the csv output 'data.csv'
    And the csv output should have '258042' rows
    And the csv output has the columns
          | ID | Test | Prepared | Sender id | Sender Name xml:lang |
    Then I read the metadata output 'metadata.json'
    And the metadata should match 'fixtures/correct_metadata_2_1.json'

  Scenario: Generic ingress runs without errors
    Given a temporary source directory of files
        | file          | fixture                        |
        | data.xml      | esa2010_test_data_short.xml    |
        | manifest.json | valid_manifest.json            |
    And a dataset id of 'valid_generic_file_ingress'
    And generic_file_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate no errors
    Then I read the xml output 'data.xml'
    And the xml output should have length '3895'
    And the xml output contains 'ESA2010 Table T1500 Transmission'

  Scenario: Pipeline runs with an expected error
    Given a temporary source directory of files
        | file          | fixture               |
        | data.xml      | esa2010_test_data.xml |
        | manifest.json | valid_manifest.json   |
    And a dataset id of 'invalid'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate an error with a message containing "Config version 2 not recognised"