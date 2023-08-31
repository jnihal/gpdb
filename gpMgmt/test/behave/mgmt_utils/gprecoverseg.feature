@gprecoverseg
Feature: gprecoverseg tests

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    When the user asynchronously runs "gprecoverseg -a" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And a sample recovery_progress.file is created from saved lines
    And we run a sample background script to generate a pid on "coordinator" segment
    Then a sample gprecoverseg.lock directory is created using the background pid in coordinator_data_directory
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
#    And gpstate output contains "incremental,incremental,incremental" entries for mirrors of content 0,1,2
#    And gpstate output looks like
#      | Segment | Port   | Recovery type  | Completed bytes \(kB\) | Total bytes \(kB\) | Percentage completed |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the background pid is killed on "coordinator" segment
    Then the gprecoverseg lock directory is removed

    And the cluster is rebalanced
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs for full recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in custom logdir for full recovery of mirrors
    Given the database is running
    And all files in "/tmp/custom_logdir" directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -aF -l /tmp/custom_logdir" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in /tmp/custom_logdir and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in /tmp/custom_logdir
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in /tmp/custom_logdir
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    And all files in "/tmp/custom_logdir" directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs for differential recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg -i creates recovery_progress.file in gpAdminLogs for mixed recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0700
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 incremental
    When the user asynchronously runs gprecoverseg with input file and additional args "-a" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And the old data directories are cleaned up for content 0
    And user can start transactions
    And check segment conf: postgresql.conf
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario:  SIGINT on gprecoverseg should delete the progress file
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And the user suspend the walsender on the primary on content 0
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    Then verify if the gprecoverseg.lock directory is present in coordinator_data_directory
    When the user asynchronously sets up to end gprecoverseg process with SIGINT
    And the user waits until saved async process is completed
    Then recovery_progress.file should not exist in gpAdminLogs
    Then the user reset the walsender on the primary on content 0
    Then the gprecoverseg lock directory is removed
    And the user waits until mirror on content 0,1,2 is up
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario:  SIGINT on gprecoverseg differential recovery should delete the progress file
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    Then verify if the gprecoverseg.lock directory is present in coordinator_data_directory
    When the user asynchronously sets up to end gprecoverseg process with SIGINT
    And the user waits until saved async process is completed
    Then recovery_progress.file should not exist in gpAdminLogs
    Then the gprecoverseg lock directory is removed
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced


  @demo_cluster
  @concourse_cluster
  Scenario:  SIGKILL on gprecoverseg should not display progress in gpstate -e
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And the user suspend the walsender on the primary on content 0
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    Then verify if the gprecoverseg.lock directory is present in coordinator_data_directory
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
    When the user asynchronously sets up to end gprecoverseg process with SIGKILL
    And the user waits until saved async process is completed
    When the user runs "gpstate -e"
    Then gpstate should not print "Segments in recovery" to stdout
    Then the user reset the walsender on the primary on content 0
    And the user waits until mirror on content 0,1,2 is up
    And the gprecoverseg lock directory is removed
    And the cluster is rebalanced
