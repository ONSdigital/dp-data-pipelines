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
        | data.xml               |  data_sdmx_valid_1.sdmx            |
    And v1_data_ingress starts using the temporary source directory
    Then the pipeline should generate no errors

  Scenario: Pipeline runs with an expected error
    Given a temporary source directory of files
        | file                            |  fixture                           |
        | incorrectly-named-config.json   |  pipeline_config_basic_valid.json  |
        | data.xml                        |  data_sdmx_valid_1.sdmx            |
    And v1_data_ingress starts using the temporary source directory
    Then the pipeline should generate an error with a message containing "issue finding a local file"