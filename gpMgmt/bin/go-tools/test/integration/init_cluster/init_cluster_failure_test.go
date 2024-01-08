package init_cluster

//func TestInputFileValidationSuccess(t *testing.T) {
//	configFile := "/tmp/config.json"
//	_, _ = testutils.RunConfigure("--ca-certificate", "/tmp/certificates/ca-cert.pem",
//		"--ca-key", "/tmp/certificates/ca-key.pem",
//		"--server-certificate", "/tmp/certificates/server-cert.pem",
//		"--server-key", "/tmp/certificates/server-key.pem",
//		"--hostfile", *hostfile)
//	_, _ = testutils.RunStart("services")
//	time.Sleep(5 * time.Second)
//
//	t.Run("cluster creation with no value for shared_buffers in common config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("common-config", map[string]string{"shared_buffers": ""})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "shared_buffers is not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("common-config", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for common config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("common-config", map[string]string{})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "shared_buffers is not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("common-config", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for encoding", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("encoding", "")
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := fmt.Sprintf("Could not find encoding in cluster config, defaulting to %v", constants.DefaultEncoding)
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("encoding", defaultConfig.CommonConfig)
//	})
//
//	t.Run("cluster creation with no value for max_connection in coordinator config", func(t *testing.T) {
//		viper.SetConfigFile(configFile)
//		viper.Set("coordinator-config", map[string]string{})
//		_ = viper.WriteConfigAs(configFile)
//		result, err := testutils.RunInitCluster(configFile, "--force")
//
//		expectedOut := "COORDINATOR max_connections not set, will set to default value"
//		if err != nil {
//			t.Errorf("\nUnexpected error: %v", err)
//		}
//		if result.ExitCode != 0 {
//			t.Errorf("\nExpected: %v \nGot: %v", 0, result.ExitCode)
//		}
//		if !strings.Contains(result.OutputMsg, expectedOut) {
//			t.Errorf("\nExpected string: %#v \nNot found in: %#v", expectedOut, result.OutputMsg)
//		}
//		viper.Set("encoding", defaultConfig.CommonConfig)
//	})
//}
