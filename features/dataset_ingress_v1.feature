Feature: Data Ingress v1
  As a data engineer I want a configurable pipeline that..
  - can confirm correct configuration and inputs
  - can send sending notifications in the event of issues
  - can successfully transform input into expected outputs
  - will call the required apis to pass along these outputs
  - will notify publishing that a resource is ready for publishing
  # TODO - this will grow in time to encapsulate the happy path for the above
  Scenario: Pipeline runs without errors
    Given a temporary source directory of files
        | file      | fixture                     |
        | data.xml  | esa2010_test_data_short.xml |
    And a dataset id of 'valid_no_supp_dist'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the backend receives a request to "/upload-new"
    And the csv payload received should contain "temp-file-part-1"
    And the headers received should match
        | key              | value            |
        | X-Florence-Token | not-a-real-token |
    And the pipeline should generate no errors
    And I read the csv output 'data.csv'
    And the csv output should have '24' rows
    And the csv output has the columns
          | ID | Test | Name xml:lang |
    And I read the metadata output 'metadata.json'
    And the metadata should match 'fixtures/correct_metadata.json'

  Scenario: SDMX 2.1 Pipeline runs without errors
    Given a temporary source directory of files
        | file      | fixture                                                |
        | data.xml  | T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_24_Jan_2024 |
    And a SDMX 2.1 dataset id of 'valid_no_supp_dist'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the backend receives a request to "/upload-new"
    And the csv payload received should contain "temp-file-part-1"
    And the headers received should match
        | key              | value            |
        | X-Florence-Token | not-a-real-token |
    And the pipeline should generate no errors
    # And I read the csv output 'data.csv'
    # And the csv output should have '24' rows
    # And the csv output has the columns
    #      | ID | Test | Name xml:lang |
    # And I read the metadata output 'metadata.json'
    # And the metadata should match 'fixtures/correct_metadata.json'

  Scenario: Pipeline runs with an expected error
    Given a temporary source directory of files
        | file     | fixture               |
        | data.xml | esa2010_test_data.xml |
    And a dataset id of 'invalid'
    And dataset_ingress_v1 starts using the temporary source directory
    Then the pipeline should generate an error with a message containing "Config version 2 not recognised"