package init_cluster

import (
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpdb/gp/cli"
	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/test/integration/testutils"
)

func TestLocaleValidation(t *testing.T) {
	localTypes := []string{"LC_COLLATE", "LC_CTYPE", "LC_MESSAGES", "LC_MONETARY", "LC_NUMERIC", "LC_TIME"}

	t.Run("when LC_ALL is given, it sets the locale for all the types", func(t *testing.T) {
		expected := getRandomLocale(t)

		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "locale", cli.Locale{
			LcAll: expected,
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		for _, localType := range localTypes {
			testutils.AssertPgConfig(t, localType, expected)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("individual locale type takes precedence over LC_ALL", func(t *testing.T) {
		expected := getRandomLocale(t)
		expectedLcCtype := getRandomLocale(t)

		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "locale", cli.Locale{
			LcAll:   expected,
			LcCtype: expectedLcCtype,
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		for _, localType := range localTypes {
			if localType == "LC_CTYPE" {
				testutils.AssertPgConfig(t, localType, expectedLcCtype)
			} else {
				testutils.AssertPgConfig(t, localType, expected)
			}
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("when no locale value is provided, inherits the locale from the environment", func(t *testing.T) {
		// TODO: on macos launchd does not inherit the system locale value
		// so skip it for now until we find a way to test it.
		if runtime.GOOS == constants.PlatformDarwin {
			t.Skip()
		}

		configFile := testutils.GetTempFile(t, "config.json")
		UnsetConfigKey(t, configFile, "locale", true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		for _, localType := range localTypes {
			testutils.AssertPgConfig(t, localType, getSystemLocale(t, localType))
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("when invalid locale is given", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "locale", cli.Locale{
			LcAll: "invalid.locale",
		}, true)

		result, err := testutils.RunInitCluster(configFile)
		if e, ok := err.(*exec.ExitError); !ok || e.ExitCode() != 1 {
			t.Fatalf("got %v, want exit status 1", err)
		}

		expectedOut := `\[ERROR\]:-host: (\S+), locale value 'invalid.locale' is not a valid locale`
		match, err := regexp.MatchString(expectedOut, result.OutputMsg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !match {
			t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		}
	})
}

func TestPgConfig(t *testing.T) {
	t.Run("sets the correct config values as provided by the user", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "coordinator-config", map[string]string{
			"max_connections": "15",
		}, true)
		SetConfigKey(t, configFile, "segment-config", map[string]string{
			"max_connections": "10",
		})
		SetConfigKey(t, configFile, "common-config", map[string]string{
			"max_wal_senders": "5",
		})

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		testutils.AssertPgConfig(t, "max_connections", "15", -1)
		testutils.AssertPgConfig(t, "max_connections", "10", 0)
		testutils.AssertPgConfig(t, "max_wal_senders", "5", -1)
		testutils.AssertPgConfig(t, "max_wal_senders", "5", 0)

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("segment-config and coordinator-config take precedence over the common-config", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		SetConfigKey(t, configFile, "coordinator-config", map[string]string{
			"max_connections": "15",
		}, true)
		SetConfigKey(t, configFile, "segment-config", map[string]string{
			"max_connections": "10",
		})
		SetConfigKey(t, configFile, "common-config", map[string]string{
			"max_connections": "25",
		})

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		testutils.AssertPgConfig(t, "max_connections", "15", -1)
		testutils.AssertPgConfig(t, "max_connections", "10", 0)

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("check if the gp_segment_configuration table has the correct value", func(t *testing.T) {
		var value cli.Segment
		var valueSeg []cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		expectedSegs := []cli.Segment{value}
		expectedSegs = append(expectedSegs, valueSeg...)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		conn := dbconn.NewDBConnFromEnvironment("postgres")
		if err := conn.Connect(1); err != nil {
			t.Fatalf("Error connecting to the database: %v", err)
		}
		defer conn.Close()

		segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		fmt.Println("seg config")
		fmt.Println(segConfigs)
		if err != nil {
			t.Fatalf("Error getting segment configuration: %v", err)
		}

		resultSegs := make([]cli.Segment, len(segConfigs))
		fmt.Println("result seg")
		fmt.Println(resultSegs)
		for i, seg := range segConfigs {
			resultSegs[i] = cli.Segment{
				Hostname:      seg.Hostname,
				Port:          seg.Port,
				DataDirectory: seg.DataDir,
				Address:       seg.Hostname,
			}
		}

		fmt.Println("result seg")
		fmt.Println(resultSegs)

		if !reflect.DeepEqual(resultSegs, expectedSegs) {
			t.Fatalf("got %+v, want %+v", resultSegs, expectedSegs)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("initialize cluster with default config and verify default values used correctly", func(t *testing.T) {
		var expectedOut string
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}
		expectedOutput := result.OutputMsg

		expectedOut = "[INFO]:-Could not find encoding in cluster config, defaulting to UTF-8"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		expectedOut = "[INFO]:- Coordinator max_connections not set, will set to value 150 from CommonConfig"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		expectedOut = "[INFO]:-shared_buffers is not set, will set to default value 128000kB"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		testutils.AssertPgConfig(t, "max_connections", "150", -1)
		testutils.AssertPgConfig(t, "shared_buffers", "125MB", -1)
		testutils.AssertPgConfig(t, "client_encoding", "UTF8", -1)

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

}

func TestCollations(t *testing.T) {
	t.Run("collations are imported successfully", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		expectedOut := "[INFO]:-Importing system collations"
		if !strings.Contains(result.OutputMsg, expectedOut) {
			t.Errorf("got %q, want %q", result.OutputMsg, expectedOut)
		}

		// before importing collations
		testutils.ExecQuery(t, "", "CREATE TABLE collationimport1 AS SELECT * FROM pg_collation WHERE collnamespace = 'pg_catalog'::regnamespace")

		// importing collations
		rows := testutils.ExecQuery(t, "", "SELECT pg_import_system_collations('pg_catalog')")
		testutils.AssertRowCount(t, rows, 1)

		// after importing collations
		testutils.ExecQuery(t, "", "CREATE TABLE collationimport2 AS SELECT * FROM pg_collation WHERE collnamespace = 'pg_catalog'::regnamespace")

		// there should be no difference before and after
		rows = testutils.ExecQuery(t, "", "SELECT * FROM collationimport1 EXCEPT SELECT * FROM collationimport2")
		testutils.AssertRowCount(t, rows, 0)

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func getSystemLocale(t *testing.T, localeType string) string {
	t.Helper()

	out, err := exec.Command("locale").CombinedOutput()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, line := range strings.Fields(string(out)) {
		if strings.Contains(line, localeType) {
			value := strings.Split(line, "=")[1]
			return strings.ReplaceAll(value, "\"", "")
		}
	}

	return ""
}

func getRandomLocale(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("locale", "-a").CombinedOutput()
	if err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	// get only UTF-8 locales to match the default encoding value
	var locales []string
	lines := strings.Fields(string(out))
	for _, line := range lines {
		if strings.Contains(strings.ToLower(line), "utf") {
			locales = append(locales, line)
		}
	}

	return locales[rand.Intn(len(locales))]
}

func TestDbNameValidation(t *testing.T) {
	t.Run("database name provided is created properly", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		// before creating database
		targetDBName := "testdb"
		QueryResult := testutils.ExecQuery(t, "", "SELECT datname from pg_database")
		for QueryResult.Next() {
			var dbName string
			err := QueryResult.Scan(&dbName)
			if err != nil {
				t.Fatalf("unexpected error scanning query result: %v", err)
			}
			if dbName == targetDBName {
				t.Fatalf("Database '%s' should not exist before creating it", targetDBName)
			}
		}

		//delete clutser and create it again with dbname specified
		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		SetConfigKey(t, configFile, "db-name", targetDBName, true)
		InitClusterResult, error := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", InitClusterResult.OutputMsg, error)
		}

		// after creating db
		foundDB := false
		rows := testutils.ExecQuery(t, "", "SELECT datname from pg_database")
		for rows.Next() {
			var dbName string
			err := rows.Scan(&dbName)
			if err != nil {
				t.Fatalf("unexpected error scanning result: %v", err)
			}
			if dbName == targetDBName {
				foundDB = true
			}
		}
		if !foundDB {
			t.Fatalf("Database %v should exist after creating it", targetDBName)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestGpToolKitValidation(t *testing.T) {
	t.Run("check if the gp_toolkit extension is created", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		QueryResult := testutils.ExecQuery(t, "", "select extname from pg_extension ")
		foundGpToolkit := false
		for QueryResult.Next() {
			var extName string
			err := QueryResult.Scan(&extName)
			if err != nil {
				t.Fatalf("unexpected error scanning result: %v", err)
			}
			if extName == "gp_toolkit" {
				foundGpToolkit = true
			}
		}

		// Validate that "gp_toolkit" is present
		if !foundGpToolkit {
			t.Fatalf("Extension 'gp_toolkit' should exist in pg_extension")
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPgHbaConfValidation(t *testing.T) {
	/* Bug:concurse is failing to resolve ip to hostname*/
	/*t.Run("pghba config file validation when hbahostname is true", func(t *testing.T) {
		var value cli.Segment
		var ok bool
		var valueSeg []cli.Segment
		var okSeg bool
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		SetConfigKey(t, configFile, "hba-hostnames", true, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		filePathCord := filepath.Join(coordinator.(cli.Segment).DataDirectory, "pg_hba.conf")
		hostCord := coordinator.(cli.Segment).Hostname
		cmdStr := "whoami"
		cmd := exec.Command("ssh", hostCord, cmdStr)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCord := strings.TrimSpace(string(output))
		pgHbaLine := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultCord, coordinator.(cli.Segment).Hostname)
		cmdStrCord := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathCord, pgHbaLine)
		cmdCord := exec.Command("ssh", hostCord, cmdStrCord)
		_, err = cmdCord.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, okSeg = primarySegs.([]cli.Segment); !okSeg {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		pgHbaLineSeg := fmt.Sprintf("host\tall\tall\t%s\ttrust", primarySegs.([]cli.Segment)[0].Hostname)
		filePathSeg := filepath.Join(primarySegs.([]cli.Segment)[0].DataDirectory, "pg_hba.conf")
		cmdStr_seg := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathSeg, pgHbaLineSeg)
		hostSeg := primarySegs.([]cli.Segment)[0].Hostname
		cmdSeg := exec.Command("ssh", hostSeg, cmdStr_seg)
		_, err = cmdSeg.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})*/

	t.Run("pghba config file validation when hbahostname is false", func(t *testing.T) {
		var value cli.Segment
		var ok bool
		var valueSeg []cli.Segment
		var okSeg bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		SetConfigKey(t, configFile, "hba-hostnames", false, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		filePathCord := filepath.Join(coordinator.(cli.Segment).DataDirectory, "pg_hba.conf")
		hostCord := coordinator.(cli.Segment).Hostname
		cmdStr := "whoami"
		cmd := exec.Command("ssh", hostCord, cmdStr)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCord := strings.TrimSpace(string(output))
		cmdStrCord := "ip -4 addr show | grep inet | grep -v 127.0.0.1/8 | awk '{print $2}'"
		cmdCord := exec.Command("ssh", hostCord, cmdStrCord)
		outputCord, err := cmdCord.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCordValue := string(outputCord)
		firstCordValue := strings.Split(resultCordValue, "\n")[0]
		pgHbaLine := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultCord, firstCordValue)
		cmdStrCordValue := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathCord, pgHbaLine)
		cmdCordValue := exec.Command("ssh", hostCord, cmdStrCordValue)
		_, err = cmdCordValue.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, okSeg = primarySegs.([]cli.Segment); !okSeg {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}
		filePathSeg := filepath.Join(primarySegs.([]cli.Segment)[0].DataDirectory, "pg_hba.conf")
		hostSegValue := primarySegs.([]cli.Segment)[0].Hostname
		cmdStrSegValue := "whoami"
		cmdSegvalue := exec.Command("ssh", hostSegValue, cmdStrSegValue)
		outputSeg, errSeg := cmdSegvalue.Output()
		if errSeg != nil {
			t.Fatalf("unexpected error : %v", errSeg)
		}

		resultSeg := strings.TrimSpace(string(outputSeg))
		cmdStrSeg := "ip -4 addr show | grep inet | grep -v 127.0.0.1/8 | awk '{print $2}'"
		cmdSegValueNew := exec.Command("ssh", hostSegValue, cmdStrSeg)
		outputSegNew, err := cmdSegValueNew.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultSegValue := string(outputSegNew)
		firstValueNew := strings.Split(resultSegValue, "\n")[0]
		pgHbaLineNew := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultSeg, firstValueNew)
		cmdStrSegNew := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathSeg, pgHbaLineNew)
		cmdSegNew := exec.Command("ssh", hostSegValue, cmdStrSegNew)
		_, err = cmdSegNew.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestExpansionValidation(t *testing.T) {
	//test once and delete below case it include both just for reference
	t.Run("validate expansion", func(t *testing.T) {
		var value cli.Segment
		var valueSeg []cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		// uncomment below lines once code is ready
		// result, err := testutils.RunInitCluster(configFile)
		// if err != nil {
		// 	t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		// }

		// conn := dbconn.NewDBConnFromEnvironment("postgres")
		// if err := conn.Connect(1); err != nil {
		// 	t.Fatalf("Error connecting to the database: %v", err)
		// }
		// defer conn.Close()

		//segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		// if err != nil {
		// 	t.Fatalf("Error getting segment configuration: %v", err)
		// }
		// fmt.Printf("all primary segs")
		// fmt.Println(segConfigs)

		// Dbelow is for group
		segConfigs := []cluster.SegConfig{
			{DbID: 2, ContentID: 0, Role: "p", Port: 7001, Hostname: "sdw0", DataDir: "/tmp/demo/primary1"},
			{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			{DbID: 4, ContentID: 2, Role: "p", Port: 7003, Hostname: "sdw1", DataDir: "/tmp/demo/primary3"},
			{DbID: 8, ContentID: 3, Role: "p", Port: 7007, Hostname: "sdw1", DataDir: "/tmp/demo/primary4"},
			{DbID: 5, ContentID: 0, Role: "m", Port: 7004, Hostname: "sdw1", DataDir: "/tmp/demo/mirror1"},
			{DbID: 6, ContentID: 1, Role: "m", Port: 7005, Hostname: "sdw1", DataDir: "/tmp/demo/mirror2"},
			{DbID: 7, ContentID: 2, Role: "m", Port: 7006, Hostname: "sdw0", DataDir: "/tmp/demo/mirror3"},
			{DbID: 9, ContentID: 3, Role: "m", Port: 7008, Hostname: "sdw0", DataDir: "/tmp/demo/mirror4"},
		}
		// below is for spread
		// segConfigs := []cluster.SegConfig{
		// 	{DbID: 2, ContentID: 0, Role: "p", Port: 7001, Hostname: "sdw0", DataDir: "/tmp/demo/primary1"},
		// 	{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
		// 	{DbID: 10, ContentID: 4, Role: "p", Port: 7009, Hostname: "sdw0", DataDir: "/tmp/demo/primary5"},
		// 	{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
		// 	{DbID: 4, ContentID: 2, Role: "p", Port: 7003, Hostname: "sdw1", DataDir: "/tmp/demo/primary3"},
		// 	{DbID: 8, ContentID: 3, Role: "p", Port: 7007, Hostname: "sdw1", DataDir: "/tmp/demo/primary4"},
		// 	{DbID: 5, ContentID: 0, Role: "m", Port: 7004, Hostname: "sdw1", DataDir: "/tmp/demo/mirror1"},
		// 	{DbID: 6, ContentID: 1, Role: "m", Port: 7005, Hostname: "sdw2", DataDir: "/tmp/demo/mirror2"},
		// 	{DbID: 11, ContentID: 4, Role: "m", Port: 80001, Hostname: "sdw3", DataDir: "/tmp/demo/mirror5"},
		// 	{DbID: 7, ContentID: 2, Role: "m", Port: 7006, Hostname: "sdw0", DataDir: "/tmp/demo/mirror3"},
		// 	{DbID: 9, ContentID: 3, Role: "m", Port: 7008, Hostname: "sdw2", DataDir: "/tmp/demo/mirror4"},
		// }
		//decalare variable hostname and get it from config hostlist may be hostlist[0] ->u will get sdw0 and remove localhost and replace with hostna,e

		primaries := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "p" && seg.Hostname == "sdw0" { //include code to skip conetent id -1
				primaries[seg.ContentID] = append(primaries[seg.ContentID], seg)
			}
		}

		// Step 3: Fetch corresponding mirrors and their hosts
		mirrors := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "m" {
				if primary, ok := primaries[seg.ContentID]; ok {
					mirrors[primary[0].ContentID] = append(mirrors[primary[0].ContentID], seg)
				}
			}
		}
		//uncomment below once code is ready
		//mirroringType := config.Get("mirroring-type")
		mirroringType := "group"

		// Step 4: Validate spread and group mirroring
		var mirrorHostnames []string
		seen := make(map[string]bool)
		var primaryHostnames []string

		for _, configs := range mirrors {
			for _, config := range configs {
				mirrorHostnames = append(mirrorHostnames, config.Hostname)
				seen[config.Hostname] = true
			}
		}

		for _, configs := range primaries {
			for _, config := range configs {
				primaryHostnames = append(primaryHostnames, config.Hostname)
			}
		}

		for _, mirrorHostname := range mirrorHostnames {
			for _, primaryHostname := range primaryHostnames {
				if mirrorHostname == primaryHostname {
					t.Fatalf("Error: Mirrors are hosted on the same host as primary: %s", mirrorHostname)
				}
			}
		}

		if mirroringType == "group" {
			if len(seen) > 1 {
				t.Fatalf("Error: Hostnames are not all the same: %v", mirrorHostnames)
			}
		} else if mirroringType == "spread" {
			if len(seen) != len(mirrorHostnames) {
				t.Fatalf("Error: Hostnames are not all different. %v", mirrorHostnames)
			}
		}

		//NOTE: EVERYTHING IS WORKING FINE ADD LOGIC TO CHECK WHETHR MIrROR HOSTDS are differnt from primary host
		// 	primaryHostname := primaryList[0].Hostname // Get the hostname of the primary
		//and then comapre each mirror host is difernt from primary
		//ALso optmise the group and sread code u can reduce code numbers

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})

	t.Run("validate expansion with group mirroring", func(t *testing.T) {
		var value cli.Segment
		var valueSeg []cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		// uncomment below lines once code is ready
		// result, err := testutils.RunInitCluster(configFile)
		// if err != nil {
		// 	t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		// }

		// conn := dbconn.NewDBConnFromEnvironment("postgres")
		// if err := conn.Connect(1); err != nil {
		// 	t.Fatalf("Error connecting to the database: %v", err)
		// }
		// defer conn.Close()

		//segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		// if err != nil {
		// 	t.Fatalf("Error getting segment configuration: %v", err)
		// }
		// fmt.Printf("all primary segs")
		// fmt.Println(segConfigs)

		// Dbelow is for group
		segConfigs := []cluster.SegConfig{
			{DbID: 2, ContentID: 0, Role: "p", Port: 7001, Hostname: "sdw0", DataDir: "/tmp/demo/primary1"},
			{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			{DbID: 4, ContentID: 2, Role: "p", Port: 7003, Hostname: "sdw1", DataDir: "/tmp/demo/primary3"},
			{DbID: 8, ContentID: 3, Role: "p", Port: 7007, Hostname: "sdw1", DataDir: "/tmp/demo/primary4"},
			{DbID: 5, ContentID: 0, Role: "m", Port: 7004, Hostname: "sdw1", DataDir: "/tmp/demo/mirror1"},
			{DbID: 6, ContentID: 1, Role: "m", Port: 7005, Hostname: "sdw1", DataDir: "/tmp/demo/mirror2"},
			{DbID: 7, ContentID: 2, Role: "m", Port: 7006, Hostname: "sdw0", DataDir: "/tmp/demo/mirror3"},
			{DbID: 9, ContentID: 3, Role: "m", Port: 7008, Hostname: "sdw0", DataDir: "/tmp/demo/mirror4"},
		}

		//decalare variable hostname and get it from config hostlist may be hostlist[0] ->u will get sdw0 and remove localhost and replace with hostna,e
		//hostname := config.Get(hostList[0])
		primaries := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "p" && seg.Hostname == "sdw0" { //include code to skip conetent id -1
				primaries[seg.ContentID] = append(primaries[seg.ContentID], seg)
			}
		}

		// Step 3: Fetch corresponding mirrors and their hosts
		mirrors := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "m" {
				if primary, ok := primaries[seg.ContentID]; ok {
					mirrors[primary[0].ContentID] = append(mirrors[primary[0].ContentID], seg)
				}
			}
		}
		//uncomment below once code is ready not need becz by default it iakes spread

		//mirroringType := config.Get("mirroring-type")
		//config.Set("mirroring-type", "group")

		if err := config.WriteConfigAs(configFile); err != nil {
			t.Fatalf("failed to write config to file: %v", err)
		}
		// Step 4: Validate spread and group mirroring
		var mirrorHostnames []string
		seen := make(map[string]bool)
		var primaryHostnames []string

		for _, configs := range mirrors {
			for _, config := range configs {
				mirrorHostnames = append(mirrorHostnames, config.Hostname)
				seen[config.Hostname] = true
			}
		}

		for _, configs := range primaries {
			for _, config := range configs {
				primaryHostnames = append(primaryHostnames, config.Hostname)
			}
		}

		for _, mirrorHostname := range mirrorHostnames {
			for _, primaryHostname := range primaryHostnames {
				if mirrorHostname == primaryHostname {
					t.Fatalf("Error: Mirrors are hosted on the same host as primary: %s", mirrorHostname)
				}
			}
		}

		if len(seen) > 1 {
			t.Fatalf("Error: Group mirroing validation Failed: All hostnames are not same for mirrors: %v", mirrorHostnames)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})

	t.Run("validate expansion with spread mirroring", func(t *testing.T) {
		var value cli.Segment
		var valueSeg []cli.Segment
		var ok bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, ok = primarySegs.([]cli.Segment); !ok {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		// uncomment below lines once code is ready
		// result, err := testutils.RunInitCluster(configFile)
		// if err != nil {
		// 	t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		// }

		// conn := dbconn.NewDBConnFromEnvironment("postgres")
		// if err := conn.Connect(1); err != nil {
		// 	t.Fatalf("Error connecting to the database: %v", err)
		// }
		// defer conn.Close()

		//segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		// if err != nil {
		// 	t.Fatalf("Error getting segment configuration: %v", err)
		// }
		// fmt.Printf("all primary segs")
		// fmt.Println(segConfigs)

		// below is for spread
		segConfigs := []cluster.SegConfig{
			{DbID: 2, ContentID: 0, Role: "p", Port: 7001, Hostname: "sdw0", DataDir: "/tmp/demo/primary1"},
			{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			{DbID: 10, ContentID: 4, Role: "p", Port: 7009, Hostname: "sdw0", DataDir: "/tmp/demo/primary5"},
			{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			{DbID: 4, ContentID: 2, Role: "p", Port: 7003, Hostname: "sdw1", DataDir: "/tmp/demo/primary3"},
			{DbID: 8, ContentID: 3, Role: "p", Port: 7007, Hostname: "sdw1", DataDir: "/tmp/demo/primary4"},
			{DbID: 5, ContentID: 0, Role: "m", Port: 7004, Hostname: "sdw1", DataDir: "/tmp/demo/mirror1"},
			{DbID: 6, ContentID: 1, Role: "m", Port: 7005, Hostname: "sdw2", DataDir: "/tmp/demo/mirror2"},
			{DbID: 11, ContentID: 4, Role: "m", Port: 80001, Hostname: "sdw3", DataDir: "/tmp/demo/mirror5"},
			{DbID: 7, ContentID: 2, Role: "m", Port: 7006, Hostname: "sdw0", DataDir: "/tmp/demo/mirror3"},
			{DbID: 9, ContentID: 3, Role: "m", Port: 7008, Hostname: "sdw2", DataDir: "/tmp/demo/mirror4"},
		}

		//decalare variable hostname and get it from config hostlist may be hostlist[0] ->u will get sdw0 and remove localhost and replace with hostna,e
		//hostname := config.Get(hostList[0])
		primaries := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "p" && seg.Hostname == "sdw0" { //include code to skip conetent id -1
				primaries[seg.ContentID] = append(primaries[seg.ContentID], seg)
			}
		}

		// Step 3: Fetch corresponding mirrors and their hosts
		mirrors := make(map[int][]cluster.SegConfig)
		for _, seg := range segConfigs {
			if seg.Role == "m" {
				if primary, ok := primaries[seg.ContentID]; ok {
					mirrors[primary[0].ContentID] = append(mirrors[primary[0].ContentID], seg)
				}
			}
		}
		//uncomment below once code is ready // may be not needed
		//mirroringType := config.Get("mirroring-type")
		//mirroringType := "group"

		// Step 4: Validate spread and group mirroring
		var mirrorHostnames []string
		seen := make(map[string]bool)
		var primaryHostnames []string

		for _, configs := range mirrors {
			for _, config := range configs {
				mirrorHostnames = append(mirrorHostnames, config.Hostname)
				seen[config.Hostname] = true
			}
		}

		for _, configs := range primaries {
			for _, config := range configs {
				primaryHostnames = append(primaryHostnames, config.Hostname)
			}
		}

		for _, mirrorHostname := range mirrorHostnames {
			for _, primaryHostname := range primaryHostnames {
				if mirrorHostname == primaryHostname {
					t.Fatalf("Error: Mirrors are hosted on the same host as primary: %s", mirrorHostname)
				}
			}
		}

		if len(seen) != len(mirrorHostnames) {
			t.Fatalf("Error: Spread mirroing Validation Failed, Hostnames are not different. %v", mirrorHostnames)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})

	t.Run("validate expansion detection with proper number of primary and mirror directories", func(t *testing.T) {
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		primaryDirs := len(config.GetStringSlice("primary-data-directories"))
		mirrorDirs := len(config.GetStringSlice("mirror-data-directories"))

		//hostList := len(config.GetStringSlice("hostlist"))
		hostList := 4

		fmt.Println("primary dir")
		fmt.Println(primaryDirs)

		fmt.Println("mirror dir")
		fmt.Println(mirrorDirs)

		fmt.Println("hostlist dir")
		fmt.Println(hostList)

		//validate the no of dd should be host* no of host

		//uncomment below lines once code is ready
		// result, err := testutils.RunInitCluster(configFile)
		// if err != nil {
		// 	t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		// }

		// expectedOut := "[INFO]:-Cluster initialized successfully"
		// if !strings.Contains(result.OutputMsg, expectedOut) {
		// 	t.Fatalf("got %q, want %q", result.OutputMsg, expectedOut)
		// }

		// conn := dbconn.NewDBConnFromEnvironment("postgres")
		// if err := conn.Connect(1); err != nil {
		// 	t.Fatalf("Error connecting to the database: %v", err)
		// }
		// defer conn.Close()

		// segConfigs, err := cluster.GetSegmentConfiguration(conn, false)
		// if err != nil {
		// 	t.Fatalf("Error getting segment configuration: %v", err)
		// }
		// fmt.Printf("all primary segs")
		// fmt.Println(segConfigs)

		// below is for spread
		segConfigs := []cluster.SegConfig{
			{DbID: 2, ContentID: 0, Role: "p", Port: 7001, Hostname: "sdw0", DataDir: "/tmp/demo/primary1"},
			{DbID: 3, ContentID: 1, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			{DbID: 10, ContentID: 4, Role: "p", Port: 7009, Hostname: "sdw0", DataDir: "/tmp/demo/primary5"},
			{DbID: 3, ContentID: 8, Role: "p", Port: 7002, Hostname: "sdw0", DataDir: "/tmp/demo/primary2"},
			// {DbID: 4, ContentID: 5, Role: "p", Port: 7003, Hostname: "sdw1", DataDir: "/tmp/demo/primary3"},
			// {DbID: 8, ContentID: 3, Role: "p", Port: 7007, Hostname: "sdw1", DataDir: "/tmp/demo/primary4"},
			{DbID: 5, ContentID: 0, Role: "m", Port: 7004, Hostname: "sdw1", DataDir: "/tmp/demo/mirror1"},
			{DbID: 6, ContentID: 1, Role: "m", Port: 7005, Hostname: "sdw2", DataDir: "/tmp/demo/mirror2"},
			{DbID: 11, ContentID: 4, Role: "m", Port: 80001, Hostname: "sdw3", DataDir: "/tmp/demo/mirror5"},
			{DbID: 7, ContentID: 8, Role: "m", Port: 7006, Hostname: "sdw0", DataDir: "/tmp/demo/mirror3"},
			//{DbID: 9, ContentID: 5, Role: "m", Port: 7008, Hostname: "sdw2", DataDir: "/tmp/demo/mirror4"},
		}

		var primaryDataDirs []string
		var mirrorDataDirs []string
		hosts := make(map[string]bool) // Map to store unique hosts

		for _, seg := range segConfigs {
			if seg.Role == "p" {
				primaryDataDirs = append(primaryDataDirs, seg.DataDir)
				hosts[seg.Hostname] = true

			} else if seg.Role == "m" {
				mirrorDataDirs = append(mirrorDataDirs, seg.DataDir)
				hosts[seg.Hostname] = true

			}
		}

		primaryCount := len(primaryDataDirs)
		mirrorCount := len(mirrorDataDirs)
		hostsCount := len(hosts)

		fmt.Printf("Primary Count: %d\n", primaryCount)
		fmt.Printf("Mirror Count: %d\n", mirrorCount)

		actualPrimaryCount := hostsCount * len(primaryDataDirs)
		actualMirrorCount := hostsCount * len(mirrorDataDirs)

		expectedPrimaryCount := primaryDirs * hostList
		expectedMirrorCount := mirrorDirs * hostList

		if actualPrimaryCount != expectedPrimaryCount {
			t.Fatalf("Error: Primary data directories count mismatch: expected %d, got %d", expectedPrimaryCount, primaryCount)
		}

		if actualMirrorCount != expectedMirrorCount {
			t.Fatalf("Error: Mirror data directories count mismatch: expected %d, got %d", expectedMirrorCount, mirrorCount)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		//NOTE: Test with hardcoded hosts list and see and uderstand code als

	})

	t.Run("Verify default values are used correctly with expansion support", func(t *testing.T) {
		var expectedOut string
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}
		expectedOutput := result.OutputMsg
		// Add any other default values which is used for mirror support
		expectedOut = "[INFO]:-Could not find encoding in cluster config, defaulting to UTF-8"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		expectedOut = "[INFO]:- Coordinator max_connections not set, will set to value 150 from CommonConfig"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		expectedOut = "[INFO]:-shared_buffers is not set, will set to default value 128000kB"
		if !strings.Contains(expectedOutput, expectedOut) {
			t.Fatalf("Output does not contain the expected string.\nExpected: %q\nGot: %q", expectedOut, expectedOutput)
		}

		testutils.AssertPgConfig(t, "max_connections", "150", -1)
		testutils.AssertPgConfig(t, "shared_buffers", "125MB", -1)
		testutils.AssertPgConfig(t, "client_encoding", "UTF8", -1)

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	//NOTE: Not sure how below 2 cases related to HBA conf will behave test once and see result
	/* Bug:concurse is failing to resolve ip to hostname*/
	/*t.Run("verify expansion when hbahostname is true", func(t *testing.T) {
		var value cli.Segment
		var ok bool
		var valueSeg []cli.Segment
		var okSeg bool
		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		SetConfigKey(t, configFile, "hba-hostnames", true, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		filePathCord := filepath.Join(coordinator.(cli.Segment).DataDirectory, "pg_hba.conf")
		hostCord := coordinator.(cli.Segment).Hostname
		cmdStr := "whoami"
		cmd := exec.Command("ssh", hostCord, cmdStr)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCord := strings.TrimSpace(string(output))
		pgHbaLine := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultCord, coordinator.(cli.Segment).Hostname)
		cmdStrCord := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathCord, pgHbaLine)
		cmdCord := exec.Command("ssh", hostCord, cmdStrCord)
		_, err = cmdCord.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, okSeg = primarySegs.([]cli.Segment); !okSeg {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}

		pgHbaLineSeg := fmt.Sprintf("host\tall\tall\t%s\ttrust", primarySegs.([]cli.Segment)[0].Hostname)
		filePathSeg := filepath.Join(primarySegs.([]cli.Segment)[0].DataDirectory, "pg_hba.conf")
		cmdStr_seg := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathSeg, pgHbaLineSeg)
		hostSeg := primarySegs.([]cli.Segment)[0].Hostname
		cmdSeg := exec.Command("ssh", hostSeg, cmdStr_seg)
		_, err = cmdSeg.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})*/

	t.Run(" verify expansion when hbahostname is false", func(t *testing.T) {
		var value cli.Segment
		var ok bool
		var valueSeg []cli.Segment
		var okSeg bool

		configFile := testutils.GetTempFile(t, "config.json")
		config := GetDefaultConfig(t, true)

		err := config.WriteConfigAs(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}

		SetConfigKey(t, configFile, "hba-hostnames", false, true)

		result, err := testutils.RunInitCluster(configFile)
		if err != nil {
			t.Fatalf("unexpected error: %s, %v", result.OutputMsg, err)
		}

		coordinator := config.Get("coordinator")
		if value, ok = coordinator.(cli.Segment); !ok {
			t.Fatalf("unexpected data type for coordinator %T", value)
		}

		filePathCord := filepath.Join(coordinator.(cli.Segment).DataDirectory, "pg_hba.conf")
		hostCord := coordinator.(cli.Segment).Hostname
		cmdStr := "whoami"
		cmd := exec.Command("ssh", hostCord, cmdStr)
		output, err := cmd.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCord := strings.TrimSpace(string(output))
		cmdStrCord := "ip -4 addr show | grep inet | grep -v 127.0.0.1/8 | awk '{print $2}'"
		cmdCord := exec.Command("ssh", hostCord, cmdStrCord)
		outputCord, err := cmdCord.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultCordValue := string(outputCord)
		firstCordValue := strings.Split(resultCordValue, "\n")[0]
		pgHbaLine := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultCord, firstCordValue)
		cmdStrCordValue := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathCord, pgHbaLine)
		cmdCordValue := exec.Command("ssh", hostCord, cmdStrCordValue)
		_, err = cmdCordValue.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		primarySegs := config.Get("primary-segments-array")
		if valueSeg, okSeg = primarySegs.([]cli.Segment); !okSeg {
			t.Fatalf("unexpected data type for primary-segments-array %T", valueSeg)
		}
		filePathSeg := filepath.Join(primarySegs.([]cli.Segment)[0].DataDirectory, "pg_hba.conf")
		hostSegValue := primarySegs.([]cli.Segment)[0].Hostname
		cmdStrSegValue := "whoami"
		cmdSegvalue := exec.Command("ssh", hostSegValue, cmdStrSegValue)
		outputSeg, errSeg := cmdSegvalue.Output()
		if errSeg != nil {
			t.Fatalf("unexpected error : %v", errSeg)
		}

		resultSeg := strings.TrimSpace(string(outputSeg))
		cmdStrSeg := "ip -4 addr show | grep inet | grep -v 127.0.0.1/8 | awk '{print $2}'"
		cmdSegValueNew := exec.Command("ssh", hostSegValue, cmdStrSeg)
		outputSegNew, err := cmdSegValueNew.Output()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		resultSegValue := string(outputSegNew)
		firstValueNew := strings.Split(resultSegValue, "\n")[0]
		pgHbaLineNew := fmt.Sprintf("host\tall\t%s\t%s\ttrust", resultSeg, firstValueNew)
		cmdStrSegNew := fmt.Sprintf("/bin/bash -c 'cat %s | grep \"%s\"'", filePathSeg, pgHbaLineNew)
		cmdSegNew := exec.Command("ssh", hostSegValue, cmdStrSegNew)
		_, err = cmdSegNew.CombinedOutput()
		if err != nil {
			t.Fatalf("unexpected error : %v", err)
		}

		_, err = testutils.DeleteCluster()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
