package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const (
	DefaultServerHost   = ""
	DefaultServerPort   = "8080"
	DefaultDownloadPath = "./tmp/downloads"
	DefaultSignalingURL = "http://127.0.0.1:8080"
	DefaultSTUNURLs     = "stun:stun.l.google.com:19302"
	DefaultLocalFile    = "file_to_download.txt"
)

type Config struct {
	ServerHost    string
	ServerPort    string
	ServerAddress string
	DownloadPath  string
	AllowedOrigin string

	SignalingURL  string
	ClientID      string
	STUNURLs      []string
	LocalFilePath string
	LocalFileName string
}

var AppConfig Config

func LoadServer() error {
	settings, err := loadSettings()
	if err != nil {
		return err
	}

	AppConfig = commonConfig(settings)
	AppConfig.ServerHost = settings.GetString("SERVER_HOST")
	AppConfig.ServerPort = settings.GetString("SERVER_PORT")
	AppConfig.ServerAddress = netAddress(AppConfig.ServerHost, AppConfig.ServerPort)
	AppConfig.DownloadPath = filepath.Clean(settings.GetString("DOWNLOAD_PATH"))
	AppConfig.AllowedOrigin = settings.GetString("CORS_ALLOW_ORIGIN")
	return nil
}

func LoadClient() error {
	settings, err := loadSettings()
	if err != nil {
		return err
	}

	AppConfig = commonConfig(settings)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-client"
	}
	settings.SetDefault("CLIENT_ID", hostname)

	AppConfig.SignalingURL = strings.TrimRight(settings.GetString("SIGNALING_URL"), "/")
	if err := validateSignalingURL(AppConfig.SignalingURL); err != nil {
		return err
	}

	AppConfig.ClientID = strings.TrimSpace(settings.GetString("CLIENT_ID"))
	if AppConfig.ClientID == "" {
		AppConfig.ClientID = hostname
	}
	AppConfig.LocalFilePath = filepath.Clean(settings.GetString("LOCAL_FILE_PATH"))
	AppConfig.LocalFileName = filepath.Base(settings.GetString("LOCAL_FILE_NAME"))
	if AppConfig.LocalFileName == "." || AppConfig.LocalFileName == string(filepath.Separator) {
		return fmt.Errorf("LOCAL_FILE_NAME must be a file name")
	}
	return nil
}

func commonConfig(settings *viper.Viper) Config {
	return Config{
		STUNURLs: splitNonEmpty(settings.GetString("STUN_URLS")),
	}
}

func loadSettings() (*viper.Viper, error) {
	settings := viper.New()
	settings.SetConfigFile(".env")
	settings.SetConfigType("env")
	settings.AutomaticEnv()

	settings.SetDefault("SERVER_HOST", DefaultServerHost)
	settings.SetDefault("SERVER_PORT", DefaultServerPort)
	settings.SetDefault("DOWNLOAD_PATH", DefaultDownloadPath)
	settings.SetDefault("CORS_ALLOW_ORIGIN", "*")
	settings.SetDefault("SIGNALING_URL", DefaultSignalingURL)
	settings.SetDefault("STUN_URLS", DefaultSTUNURLs)
	settings.SetDefault("LOCAL_FILE_PATH", defaultLocalFilePath())
	settings.SetDefault("LOCAL_FILE_NAME", DefaultLocalFile)

	if err := settings.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("load .env: %w", err)
		}
	}

	return settings, nil
}

func netAddress(host, port string) string {
	if after, ok := strings.CutPrefix(port, ":"); ok {
		port = after
	}
	return net.JoinHostPort(strings.TrimSpace(host), port)
}

func validateSignalingURL(signalingURL string) error {
	parsedURL, err := url.Parse(signalingURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("SIGNALING_URL must be an absolute HTTP or HTTPS URL")
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("SIGNALING_URL scheme must be http or https")
	}
	return nil
}

func defaultLocalFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Clean(home)
}
func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
