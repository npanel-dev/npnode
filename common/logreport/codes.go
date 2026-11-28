package logreport

// 阶段常量。
const (
	StageConfig  = "config"
	StageCore    = "core"
	StageInbound = "inbound"
	StageCert    = "cert"
	StageReport  = "report"
	StageRuntime = "runtime"
)

// 错误码常量（括号内为其所属阶段）。
const (
	CodeConfigLoad          = "CONFIG_LOAD_FAILED"           // config
	CodePanelConfig         = "PANEL_CONFIG_FETCH_FAILED"    // config
	CodeCoreStart           = "CORE_START_FAILED"            // core
	CodeInboundStart        = "INBOUND_START_FAILED"         // inbound
	CodeVlessEncUnsupported = "VLESS_ENCRYPTION_UNSUPPORTED" // inbound
	CodeVlessEncModeEmpty   = "VLESS_ENCRYPTION_MODE_EMPTY"  // inbound
	CodeVlessEncKeyEmpty    = "VLESS_ENCRYPTION_KEY_EMPTY"   // inbound
	CodeUserList            = "USER_LIST_FETCH_FAILED"       // inbound
	CodeCertUnavailable     = "CERT_UNAVAILABLE"             // cert
	CodeStatusReport        = "STATUS_REPORT_FAILED"         // report
	CodeTrafficReport       = "TRAFFIC_REPORT_FAILED"        // report
	CodeReload              = "RELOAD_FAILED"                // runtime
	CodePanic               = "PANIC"                        // runtime
	CodeUnknown             = "UNKNOWN"
)
