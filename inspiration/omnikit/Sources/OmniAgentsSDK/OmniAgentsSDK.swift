public enum OmniAgentsSDK {
    public static var version: String { omniAgentsSDKVersion }
    public static var dontLogModelData: Bool { DONT_LOG_MODEL_DATA }
    public static var dontLogToolData: Bool { DONT_LOG_TOOL_DATA }

    public static func enableVerboseStdoutLogging() {
        OmniAgentsLogger.setVerboseStdoutLoggingEnabled(true)
    }
}
