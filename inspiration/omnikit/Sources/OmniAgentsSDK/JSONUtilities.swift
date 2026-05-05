import Foundation
import OmniAICore

public func validateJSON<T: Decodable>(
    _ jsonString: String,
    as type: T.Type = T.self,
    partial: Bool = false,
    decoder: JSONDecoder = JSONDecoder()
) throws -> T {
    let firstAttempt = decodeJSON(jsonString, as: type, decoder: decoder)
    if case .success(let decoded) = firstAttempt {
        return decoded
    }

    if partial,
       let partialPayload = extractLeadingJSONValue(from: jsonString),
       case .success(let decoded) = decodeJSON(partialPayload, as: type, decoder: decoder)
    {
        return decoded
    }

    let decodeError: String = {
        if case .failure(let error) = firstAttempt {
            return "; \(error)"
        }
        return ""
    }()

    attachErrorToCurrentSpan(SpanError(message: "Invalid JSON provided", data: [:]))
    throw ModelBehaviorError(
        message: "Invalid JSON when parsing \(jsonString) for \(String(reflecting: type))\(decodeError)"
    )
}

public func toDumpCompatible(_ obj: Any) -> Any {
    toDumpCompatibleInternal(obj)
}

private func decodeJSON<T: Decodable>(
    _ jsonString: String,
    as type: T.Type,
    decoder: JSONDecoder
) -> Result<T, Error> {
    guard let data = jsonString.data(using: .utf8) else {
        return .failure(AgentsError(message: "Unable to encode JSON string as UTF-8 data."))
    }

    do {
        return .success(try decoder.decode(type, from: data))
    } catch {
        return .failure(error)
    }
}

private func toDumpCompatibleInternal(_ obj: Any) -> Any {
    if let jsonValue = obj as? JSONValue {
        return jsonValueToDumpCompatible(jsonValue)
    }

    if let dictionary = obj as? [String: Any] {
        return dictionary.mapValues(toDumpCompatibleInternal)
    }

    if let dictionary = obj as? [String: JSONValue] {
        return dictionary.mapValues(jsonValueToDumpCompatible)
    }

    if let list = obj as? [Any] {
        return list.map(toDumpCompatibleInternal)
    }

    if let list = obj as? [JSONValue] {
        return list.map(jsonValueToDumpCompatible)
    }

    if let set = obj as? Set<AnyHashable> {
        return set.map { toDumpCompatibleInternal($0) }
    }

    let mirror = Mirror(reflecting: obj)
    if mirror.displayStyle == .optional {
        if let child = mirror.children.first {
            return toDumpCompatibleInternal(child.value)
        }
        return NSNull()
    }

    if mirror.displayStyle == .collection || mirror.displayStyle == .set {
        return mirror.children.map { toDumpCompatibleInternal($0.value) }
    }

    if mirror.displayStyle == .dictionary {
        var dictionary: [String: Any] = [:]
        for child in mirror.children {
            let entryMirror = Mirror(reflecting: child.value)
            let values = Array(entryMirror.children)
            guard values.count == 2 else { continue }
            let key = String(describing: values[0].value)
            dictionary[key] = toDumpCompatibleInternal(values[1].value)
        }
        return dictionary
    }

    return obj
}

private func jsonValueToDumpCompatible(_ value: JSONValue) -> Any {
    switch value {
    case .null:
        return NSNull()
    case .bool(let bool):
        return bool
    case .number(let number):
        return number
    case .string(let string):
        return string
    case .array(let array):
        return array.map(jsonValueToDumpCompatible)
    case .object(let object):
        return object.mapValues(jsonValueToDumpCompatible)
    }
}

private func extractLeadingJSONValue(from input: String) -> String? {
    let characters = Array(input)
    var index = 0

    while index < characters.count, characters[index].isWhitespace {
        index += 1
    }

    guard index < characters.count else {
        return nil
    }

    let start = index
    let firstCharacter = characters[start]

    if firstCharacter == "{" || firstCharacter == "[" {
        var depth = 0
        var inString = false
        var escapeNext = false

        for position in start..<characters.count {
            let character = characters[position]
            if inString {
                if escapeNext {
                    escapeNext = false
                } else if character == "\\" {
                    escapeNext = true
                } else if character == "\"" {
                    inString = false
                }
                continue
            }

            if character == "\"" {
                inString = true
                continue
            }

            if character == "{" || character == "[" {
                depth += 1
            } else if character == "}" || character == "]" {
                depth -= 1
                if depth == 0 {
                    return String(characters[start...position])
                }
            }
        }

        return nil
    }

    if firstCharacter == "\"" {
        var escapeNext = false
        for position in (start + 1)..<characters.count {
            let character = characters[position]
            if escapeNext {
                escapeNext = false
                continue
            }
            if character == "\\" {
                escapeNext = true
                continue
            }
            if character == "\"" {
                return String(characters[start...position])
            }
        }
        return nil
    }

    var end = start
    while end < characters.count {
        let character = characters[end]
        if character.isWhitespace || character == "," || character == "]" || character == "}" {
            break
        }
        end += 1
    }

    guard end > start else {
        return nil
    }
    return String(characters[start..<end])
}
