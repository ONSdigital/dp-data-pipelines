Feature: HTTP requests
  Scenario: Standup fake upload service container 1
    Given a request json payload of "fixtures/test1.json"
    And a request with the headers
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |
    And I send the request to the upload service at "/upload-new"
    Then the backend receives a request to "/upload-new"
    And the json payload received should match "fixtures/test1.json"
    And the headers received should match
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |

  Scenario: Standup fake upload service container 2
    Given a request json payload of "fixtures/test2.json"
    And a request with the headers
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |
    And I send the request to the upload service at "/upload"
    Then the backend receives a request to "/upload"
    And the json payload received should match "fixtures/test2.json"
    And the headers received should match
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |

  Scenario: Standup fake upload service container 3
    Given a request json payload of "fixtures/test3.json"
    And a request with the headers
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |
    And I send the request to the upload service at "/other-service"
    Then the backend receives a request to "/other-service"
    And the json payload received should match "fixtures/test3.json"
    And the headers received should match
        | key | value |
        | Foo | 2     |
        | Bar | 3     |
        | Baz | 4     |