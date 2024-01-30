package init_cluster

import (
	"math/rand"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"

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
