package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/greenplum-db/gpdb/gp/constants"
	"github.com/greenplum-db/gpdb/gp/utils/greenplum"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpdb/gp/idl"
	"github.com/greenplum-db/gpdb/gp/utils"
)

var (
	CheckDirEmpty          = CheckDirEmptyFn
	CheckFileOwnerGroup    = CheckFileOwnerGroupFn
	CheckExecutable        = CheckExecutableFn
	OsIsNotExist           = os.IsNotExist
	GetAllNonEmptyDir      = GetAllNonEmptyDirFn
	CheckFilePermissions   = CheckFilePermissionsFn
	GetAllAvailableLocales = GetAllAvailableLocalesFn
	ValidateLocaleSettings = ValidateLocaleSettingsFn
	ValidatePorts          = ValidatePortsFn
	VerifyPgVersion        = ValidatePgVersionFn
)

/*
ValidateHostEnv implements agent RPC to validate local host environment
Performs various checks on host like gpdb version, permissions to initdb, data directory exists, ports in use etc
*/

func (s *Server) ValidateHostEnv(ctx context.Context, request *idl.ValidateHostEnvRequest) (*idl.ValidateHostEnvReply, error) {
	gplog.Verbose("Starting ValidateHostEnvFn for request:%v", request)
	dirList := request.DirectoryList
	locale := request.Locale
	socketAddressList := request.SocketAddressList
	forced := request.Forced

	// Check if user is non-root
	if utils.System.Getuid() == 0 {
		userInfo, err := utils.System.CurrentUser()
		if err != nil {
			gplog.Error("failed to get user name Error:%v. Current user is a root user. Can't create cluster under root", err)
			return &idl.ValidateHostEnvReply{}, fmt.Errorf("failed to get user name Error:%v. Current user is a root user. Can't create cluster under root", err)
		}
		return &idl.ValidateHostEnvReply{}, fmt.Errorf("user:%s is a root user, Can't create cluster under root user", userInfo.Name)
	}
	gplog.Verbose("Done with checking user is non root")

	//Check for PGVersion
	pgVersionErr := VerifyPgVersion(request.GpVersion, s.GpHome)
	if pgVersionErr != nil {
		gplog.Error("Postgres gp-version validation failed:%v", pgVersionErr)
		return &idl.ValidateHostEnvReply{}, pgVersionErr
	}

	// Check for each directory, if directory is empty
	nonEmptyDirList := GetAllNonEmptyDir(dirList)
	gplog.Verbose("Got the list of all non-empty directories")

	if len(nonEmptyDirList) > 0 && !forced {
		return &idl.ValidateHostEnvReply{}, fmt.Errorf("directory not empty:%v", nonEmptyDirList)
	}
	if forced && len(nonEmptyDirList) > 0 {

		gplog.Verbose("Forced init. Deleting non-empty directories:%s", dirList)
		for _, dir := range nonEmptyDirList {
			err := utils.System.RemoveAll(dir)
			if err != nil {
				return &idl.ValidateHostEnvReply{}, fmt.Errorf("delete not empty dir:%s, error:%v", dir, err)
			}
		}
	}

	// Validate permission to initdb ? Error will be returned upon running
	gplog.Verbose("Checking initdb for permissions")
	initdbPath := filepath.Join(s.GpHome, "bin", "initdb")
	err := CheckFilePermissions(initdbPath)
	if err != nil {
		return &idl.ValidateHostEnvReply{}, err
	}

	// Validate that the different locale settings are available on the system
	err = ValidateLocaleSettings(locale)
	if err != nil {
		gplog.Info("Got error while validating locale %v", err)
		return &idl.ValidateHostEnvReply{}, err
	}

	// Check if port in use
	err = ValidatePorts(socketAddressList)
	if err != nil {
		return &idl.ValidateHostEnvReply{}, err
	}

	// Any checks to raise warnings
	var warnings []*idl.LogMessage

	// check coordinator open file values
	warnings = CheckOpenFilesLimit()
	addressWarnings := CheckHostAddressInHostsFile(request.HostAddressList)
	warnings = append(warnings, addressWarnings...)
	return &idl.ValidateHostEnvReply{Messages: warnings}, nil
}

/*
ValidatePgVersionFn gets current version of gpdb and compares with version from coordinator
returns error if version do not match.
*/
func ValidatePgVersionFn(expectedVersion string, gpHome string) error {
	localPgVersion, err := greenplum.GetPostgresGpVersion(gpHome)
	if err != nil {
		return err
	}

	if expectedVersion != localPgVersion {
		return fmt.Errorf("postgres gp-version does not matches with coordinator postgres gp-version."+
			"Coordinator version:'%s', Current version:'%s'", expectedVersion, localPgVersion)
	}
	return nil

}

/*
CheckOpenFilesLimit sends an warning to CLI if open files limit is not unlimited
*/

func CheckOpenFilesLimit() []*idl.LogMessage {
	var warnings []*idl.LogMessage
	ulimitVal, err := utils.ExecuteAndGetUlimit()
	if err != nil {
		warnMsg := fmt.Sprintf("error getting open files limit:%s", err.Error())
		warnings = append(warnings, &idl.LogMessage{Message: warnMsg, Level: idl.LogLevel_WARNING})
		gplog.Warn(warnMsg)
		return warnings
	}

	if ulimitVal < constants.OsOpenFiles {
		// In case of macOS, no limits file are present, return error
		warnMsg := fmt.Sprintf("Host open file limit is %d should be >= %d. Set open files limit for user and systemd and start gp services again.", ulimitVal, constants.OsOpenFiles)
		warnings = append(warnings, &idl.LogMessage{Message: warnMsg, Level: idl.LogLevel_WARNING})
		gplog.Warn(warnMsg)
		return warnings
	}
	return warnings
}

/*
CheckHostAddressInHostsFile checks if given address present with a localhost entry.
Returns a warning message to CLI if entry is detected
*/
func CheckHostAddressInHostsFile(hostAddressList []string) []*idl.LogMessage {
	var warnings []*idl.LogMessage
	gplog.Verbose("CheckHostAddressInHostsFile checking for address:%v", hostAddressList)
	content, err := utils.System.ReadFile(constants.EtcHostsFilepath)
	if err != nil {
		warnMsg := fmt.Sprintf("error reading file %s error:%v", constants.EtcHostsFilepath, err)
		gplog.Warn(warnMsg)
		warnings = append(warnings, &idl.LogMessage{Message: warnMsg, Level: idl.LogLevel_WARNING})
		return warnings
	}

	lines := strings.Split(string(content), "\n")
	for _, hostAddress := range hostAddressList {
		for _, line := range lines {
			hosts := strings.Split(line, " ")
			if slices.Contains(hosts, hostAddress) && slices.Contains(hosts, "localhost") {
				warnMsg := fmt.Sprintf("HostAddress %s is assigned localhost in %s."+
					"This will cause segment->coordinator communication failures."+
					"Remote %s from local host line in /etc/hosts",
					hostAddress, constants.EtcHostsFilepath, constants.EtcHostsFilepath)

				warnings = append(warnings, &idl.LogMessage{Message: warnMsg, Level: idl.LogLevel_WARNING})
				gplog.Warn(warnMsg)
				break
			}
		}
	}

	return warnings
}

/*
ValidatePortsFn checks if port is already in use.
*/
func ValidatePortsFn(socketAddressList []string) error {
	gplog.Verbose("Started with ValidatePorts")
	var usedSocketAddressList []string
	for _, socketAddress := range socketAddressList {
		listener, err := net.Listen("tcp", socketAddress)
		if err != nil {
			usedSocketAddressList = append(usedSocketAddressList, socketAddress)
		} else {
			_ = listener.Close()
		}
	}
	if len(usedSocketAddressList) > 0 {
		gplog.Error("ports already in use: %v, check if cluster already running", usedSocketAddressList)
		return fmt.Errorf("ports already in use: %v, check if cluster already running", usedSocketAddressList)
	}
	return nil
}

/*
GetAllNonEmptyDirFn returns list of all non-empty directories
*/
func GetAllNonEmptyDirFn(dirList []string) []string {
	var nonEmptyDir []string
	for _, dir := range dirList {
		isEmpty, err := CheckDirEmpty(dir)
		if err != nil {
			gplog.Error("Directory:%s Error checking if empty:%s", dir, err.Error())
			nonEmptyDir = append(nonEmptyDir, dir)
		} else if !isEmpty {
			// Directory not empty
			nonEmptyDir = append(nonEmptyDir, dir)
		}
	}
	return nonEmptyDir
}

/*
CheckDirEmptyFn checks if given directory is empty or not
returns true if directory is empty
*/
func CheckDirEmptyFn(dirPath string) (bool, error) {
	// check if dir exists
	file, err := os.Open(dirPath)
	if OsIsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("error opening file:%v", err)
	}
	defer file.Close()
	_, err = file.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, nil
}

/*
CheckFilePermissionsFn checks if the file has right permissions.
Verified if execute permission is available.
Also checks if file is owned by group or user.
*/
func CheckFilePermissionsFn(filePath string) error {
	fileInfo, err := utils.System.Stat(filePath)
	if err != nil {
		return fmt.Errorf("error getting file info:%v", err)
	}
	// Get current user-id, group-id and checks against initdb file
	err = CheckFileOwnerGroup(filePath, fileInfo)
	if err != nil {
		return err
	}

	// Check if the file has execute permission
	if !CheckExecutable(fileInfo.Mode()) {
		return fmt.Errorf("file %s does not have execute permissions", filePath)
	}
	return nil
}

/*
CheckFileOwnerGroupFn checks if file is owned by user or the group
returns error if not owned by both
*/
func CheckFileOwnerGroupFn(filePath string, fileInfo os.FileInfo) error {
	systemUid := utils.System.Getuid()
	systemGid := utils.System.Getgid()
	// Fetch file info: file owner, group ID
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("error converting fileinfo:%v", ok)
	}

	if int(stat.Uid) != systemUid && int(stat.Gid) != systemGid {
		fmt.Printf("StatUID:%d, StatGID:%d\nSysUID:%d SysGID:%d\n", stat.Uid, stat.Gid, systemUid, systemGid)
		return fmt.Errorf("file %s is neither owned by the user nor by group", filePath)
	}
	return nil
}

func CheckExecutableFn(FileMode os.FileMode) bool {
	return FileMode&0111 != 0
}

func GetAllAvailableLocalesFn() (string, error) {
	cmd := utils.System.ExecCommand("/usr/bin/locale", "-a")
	availableLocales, err := cmd.Output()

	if err != nil {
		return "", fmt.Errorf("failed to get the available locales on this system: %w", err)
	}
	return string(availableLocales), nil
}

// Simplified version of _nl_normalize_codeset from glibc
// https://sourceware.org/git/?p=glibc.git;a=blob;f=intl/l10nflist.c;h=078a450dfec21faf2d26dc5d0cb02158c1f23229;hb=1305edd42c44fee6f8660734d2dfa4911ec755d6#l294
// Input parameter - string with locale define as [language[_territory][.codeset][@modifier]]
func NormalizeCodesetInLocale(locale string) string {
	if locale == "" {
		return locale
	}
	
	localeSplit := strings.Split(locale, ".")
	languageAndTerritory := ""
	codesetAndModifier := []string{}
	codeset := ""
	modifier := ""
	if len(localeSplit) > 0 {
		languageAndTerritory = localeSplit[0]
	}

	if len(localeSplit) > 1 {

		codesetAndModifier = strings.Split(localeSplit[1], "@")
		codeset = codesetAndModifier[0]
	}

	if len(codesetAndModifier) > 1 {
		modifier = codesetAndModifier[1]
	}

	digitPattern := regexp.MustCompile(`^[0-9]+$`)
	if digitPattern.MatchString(codeset) {
		codeset = "iso" + codeset
	} else {
		codeset = strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, codeset)
		codeset = strings.ToLower(codeset)
	}

	result := fmt.Sprintf("%s%s%s", languageAndTerritory, dotIfNotEmpty(codeset), atIfNotEmpty(modifier))
	return result
}

func dotIfNotEmpty(s string) string {
	if s != "" {
		return "." + s
	}
	return ""
}

func atIfNotEmpty(s string) string {
	if s != "" {
		return "@" + s
	}
	return ""
}

func IsLocaleAvailable(locale_type string, allAvailableLocales string) bool {
	locales := strings.Split(allAvailableLocales, "\n")
	locales = append(locales, "")
	normalizedLocale := NormalizeCodesetInLocale(locale_type)

	for _, v := range locales {
		if locale_type == v || normalizedLocale == v {
			return true
		}
	}
	return false
}

func ValidateLocaleSettingsFn(locale *idl.Locale) error {
	systemLocales, err := GetAllAvailableLocales()
	if err != nil {
		return err
	}
	localeMap := make(map[string]bool)
	localeMap[locale.LcMonetory] = true
	localeMap[locale.LcAll] = true
	localeMap[locale.LcNumeric] = true
	localeMap[locale.LcTime] = true
	localeMap[locale.LcCollate] = true
	localeMap[locale.LcMessages] = true
	localeMap[locale.LcCtype] = true

	for lc := range localeMap {
		// TODO normalize codeset in locale and the check for the availability
		if !IsLocaleAvailable(lc, systemLocales) {
			return fmt.Errorf("locale value '%s' is not a valid locale", lc)
		}
	}

	return nil
}