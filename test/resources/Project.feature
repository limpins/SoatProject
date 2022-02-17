Feature:

  Scenario: exercice scenario 1
    Given the data
      | key | Value   |
      | 1   | Blue    |
      | 1   | Red     |
      | 1   | Blue    |
      | 1   | Blue    |
      | 2   | Green   |
      | 2   | Purpule |
      | 2   | Purpule |
      | 3   | Black   |
    When I process scenario 1
    Then The result should be
      | key | Value |
      | 1   | Blue  |
      | 2   | Green |
      | 3   | Black |