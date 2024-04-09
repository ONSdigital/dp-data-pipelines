Feature: Delete me later

  # DEV NOTE - THIS IS NOT  A REAL ACCEPTANCE TEST
  # ... and will be removed later. For now it's just a way
  # to build a feature complicant http test endpoint so we can test the
  # pipeline code is sending requests where it should and with
  # the heades and payload it should.
  # Later down the line, this same appraoch will be used when writing
  # the full pipeline acceptance tests to confirm pipelines are making
  # the correct outbound http calls.
  Scenario: Standup fake upload service container 1
    Given a request json payload of "fixtures/test1.json"
    And a request with the headers
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |
    And I send the request to the upload service at "/upload-new"
    Then the backend receives a request to "/upload-new"
    And the json payload received should match "fixtures/test1.json"
    And the headers received should match
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |

  Scenario: Standup fake upload service container 2
    Given a request json payload of "fixtures/test2.json"
    And a request with the headers
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |
    And I send the request to the upload service at "/upload"
    Then the backend receives a request to "/upload"
    And the json payload received should match "fixtures/test2.json"
    And the headers received should match
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |

  Scenario: Standup fake upload service container 3
    Given a request json payload of "fixtures/test3.json"
    And a request with the headers
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |
    And I send the request to the upload service at "/other-service"
    Then the backend receives a request to "/other-service"
    And the json payload received should match "fixtures/test3.json"
    And the headers received should match
        | key        | value      |
        | Foo        | 2          |
        | Bar        | 3          |
        | Baz        | 4          |