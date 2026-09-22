Feature: Data-model SQL rewrite for CDF UDTF pushdown
  As a cognite-databricks user
  I want catalog WHERE/LIMIT/COUNT queries rewritten into UDTF parameters
  So that filters reach CDF without Spark over-fetch

  Scenario: Equality and IS NOT NULL with LIMIT bind UDTF params
    Given the SQL query
      """
      SELECT * FROM adg_cdf_dev.gold.LimsResults
      WHERE TestSeqNumber = '5889450'
        AND DilutionFactor IS NOT NULL
      LIMIT 10
      """
    When I analyze the data-model pushdown
    Then property equals should include TestSeqNumber as "5889450"
    And exists properties should include "DilutionFactor"
    And row_limit should be 10
    And query_mode should be "list"
    When I rewrite the SQL to a UDTF call
    Then the rewritten SQL should contain "_exists =>"
    And the rewritten SQL should contain "_row_limit => 10"
    And the rewritten SQL should contain "TestSeqNumber => '5889450'"

  Scenario: Instance space equals pushes identity filter
    Given the SQL query
      """
      SELECT * FROM cat.sch.SmallBoat WHERE space = 'sailboat' LIMIT 5
      """
    When I analyze the data-model pushdown
    Then instance_space should be "sailboat"
    And row_limit should be 5

  Scenario: ORDER BY with LIMIT does not push row_limit
    Given the SQL query
      """
      SELECT * FROM cat.sch.SmallBoat WHERE name = 'X' ORDER BY name LIMIT 5
      """
    When I analyze the data-model pushdown
    Then row_limit should be null
    And skip_reasons should mention "ORDER BY"

  Scenario: COUNT star rewrites to aggregate mode
    Given the SQL query
      """
      SELECT count(*) FROM cat.sch.LimsResults WHERE DilutionFactor IS NOT NULL
      """
    When I analyze the data-model pushdown
    Then query_mode should be "aggregate"
    And aggregates should include count on externalId
    And exists properties should include "DilutionFactor"
    And row_limit should be null

  Scenario: MIN and MAX rewrite to aggregate mode
    Given the SQL query
      """
      SELECT min(DateAuthorised), max(DateAuthorised) FROM cat.sch.LimsResults
      """
    When I analyze the data-model pushdown
    Then query_mode should be "aggregate"
    And aggregates should include min on DateAuthorised
    And aggregates should include max on DateAuthorised

  Scenario: Joins are not rewritten
    Given the SQL query
      """
      SELECT a.* FROM cat.sch.A a JOIN cat.sch.B b ON a.external_id = b.external_id WHERE a.name = 'x'
      """
    When I analyze the data-model pushdown
    Then pushdown_supported should be false
