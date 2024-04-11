Feature: Data Ingress v1
  As a data engineer I want a configurable pipeline that..
  - can confirm correct configuration and inputs
  - can send sending notifications in the event of issues
  - can successfully transform input into rexpected outputs
  - will call the required apis to pass along these outputs
  - will notify publishing that a resource is ready for publishing

  # TODO - this will grow in time to encapsulate the happy path for the above
  Scenario: Pipeline runs without errors
    Given a temporary source directory of files
        | file                   |  fixture                           |
        | pipeline-config.json   |  pipeline_config_basic_valid.json  |
        | data.xml               |  esa2010_test_data.xml             |
    And v1_data_ingress starts using the temporary source directory
    Then the pipeline should generate no errors
    And I read the csv output 'data.csv'
    And the csv output should have '9744' rows
    And the csv output has the columns
          | ID | Test | Name xml:lang |
    And I read the metadata output 'metadata.json'
    And the metadata should match 'fixtures/correct_metadata.json'

  Scenario: Pipeline runs with an expected error
    Given a temporary source directory of files
        | file                            |  fixture                           |
        | incorrectly-named-config.json   |  pipeline_config_basic_valid.json  |
        | data.xml                        |  esa2010_test_data.xml             |
    And v1_data_ingress starts using the temporary source directory
    Then the pipeline should generate an error with a message containing "issue finding a local file"