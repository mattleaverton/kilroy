import Foundation

public func transformStringFunctionStyle(_ name: String) -> String {
    let normalizedName = String(name.map { $0 == " " ? "_" : $0 })

    let expression = "[^a-zA-Z0-9_]"
    let transformedName: String
    if let regex = try? NSRegularExpression(pattern: expression) {
        let range = NSRange(location: 0, length: normalizedName.utf16.count)
        transformedName = regex.stringByReplacingMatches(
            in: normalizedName,
            options: [],
            range: range,
            withTemplate: "_"
        )
    } else {
        transformedName = normalizedName
    }

    if transformedName != normalizedName {
        let finalName = transformedName.lowercased()
        OmniAgentsLogger.warning(
            "Tool name \(String(reflecting: normalizedName)) contains invalid characters for function calling and has been transformed to \(String(reflecting: finalName)). Please use only letters, digits, and underscores to avoid potential naming conflicts."
        )
    }

    return transformedName.lowercased()
}
