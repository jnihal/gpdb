package postgres

import (
	"bufio"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpdb/gp/utils"
)

const (
	postgresqlConfFile       = "postgresql.conf"
	postgresInternalConfFile = "internal.auto.conf"
	pgHbaConfFile            = "pg_hba.conf"
)

func UpdatePostgresqlConf(pgdata string, configParams map[string]string, overwrite bool) error {
	err := updateConfFile(postgresqlConfFile, pgdata, configParams, overwrite)
	if err != nil {
		return err
	}

	return nil
}

func CreatePostgresInternalConf(pgdata string, dbid int) error {
	postgresInternalConfFilePath := filepath.Join(pgdata, postgresInternalConfFile)
	file, err := utils.System.Create(postgresInternalConfFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	contents := fmt.Sprintf("gp_dbid = %d", dbid)
	_, err = file.WriteString(contents)
	if err != nil {
		return err
	}

	return nil
}

func CreatePgHbaConf(pgdata string, hbaHostnames bool, coordinatorIps []string, hostname string) error {
	pgHbaFilePath := filepath.Join(pgdata, pgHbaConfFile)

	file, err := utils.System.Open(pgHbaFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	updatedLines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "#") {
			updatedLines = append(updatedLines, line)
		}
	}

	user, err := utils.System.CurrentUser()
	if err != nil {
		return err
	}

	var localAccessString = "local\t%s\t%s\tident"
	var hostAccessString = "host\t%s\t%s\t%s\ttrust"

	// Add access entries
	line := fmt.Sprintf(localAccessString, "all", user.Username)
	updatedLines = append(updatedLines, line)

	if hbaHostnames {
		line = fmt.Sprintf(hostAccessString, "all", user.Username, "localhost")
		updatedLines = append(updatedLines, line)
		line = fmt.Sprintf(hostAccessString, "all", user.Username, hostname)
		updatedLines = append(updatedLines, line)
	} else {
		ipAdresses, err := utils.System.InterfaceAddrs()
		if err != nil {
			return err
		}

		for _, ip := range ipAdresses {
			line = fmt.Sprintf(hostAccessString, "all", user.Username, ip)
			updatedLines = append(updatedLines, line)
		}
	}

	// Add coordinator IP addresses
	if len(coordinatorIps) != 0 {
		for _, ip := range coordinatorIps {
			line = fmt.Sprintf(hostAccessString, "all", "all", ip)
			updatedLines = append(updatedLines, line)
		}
	}

	// Add replication entries
	line = fmt.Sprintf(localAccessString, "replication", user.Username)
	updatedLines = append(updatedLines, line)
	line = fmt.Sprintf(hostAccessString, "replication", user.Username, "samehost")
	updatedLines = append(updatedLines, line)

	if hbaHostnames {
		line = fmt.Sprintf(hostAccessString, "replication", user.Username, hostname)
		updatedLines = append(updatedLines, line)
	} else {
		ipAdresses, err := utils.System.InterfaceAddrs()
		if err != nil {
			return err
		}

		for _, ip := range ipAdresses {
			line = fmt.Sprintf(hostAccessString, "replication", user.Username, ip)
			updatedLines = append(updatedLines, line)
		}
	}

	err = utils.WriteLinesToFile(pgHbaFilePath, updatedLines)
	if err != nil {
		return err
	}

	return nil
}

func updateConfFile(filename, pgdata string, configParams map[string]string, overwrite bool) error {
	confFilePath := filepath.Join(pgdata, filename)

	file, err := utils.System.Open(confFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	updatedLines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		for key, value := range configParams {
			pattern, err := regexp.Compile(fmt.Sprintf("^%s[\\s=]+", key))
			if err != nil {
				return err
			}

			if pattern.MatchString(line) {
				if !overwrite {
					updatedLines = append(updatedLines, fmt.Sprintf("# %s", line))
				}
				line = fmt.Sprintf("%s = %s", key, value)
				delete(configParams, key)
			}
		}

		updatedLines = append(updatedLines, line)
	}

	for key, value := range configParams {
		line := fmt.Sprintf("%s = %s", key, value)
		updatedLines = append(updatedLines, line)
	}

	err = utils.WriteLinesToFile(confFilePath, updatedLines)
	if err != nil {
		return err
	}

	return nil
}
