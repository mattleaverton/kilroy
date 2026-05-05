import Testing
import Foundation

private enum XCTestCompatError: Error {
    case unwrapFailed(String)
}

private func _sourceLocation(fileID: StaticString, line: UInt) -> SourceLocation {
    SourceLocation(
        fileID: "\(fileID)",
        filePath: "\(fileID)",
        line: Int(line),
        column: 1
    )
}

private func _recordFailure(
    _ fallbackMessage: String,
    message: @autoclosure () -> String,
    fileID: StaticString,
    line: UInt
) {
    let explicit = message().trimmingCharacters(in: .whitespacesAndNewlines)
    let final = explicit.isEmpty ? fallbackMessage : explicit
    Issue.record(Comment(rawValue: final), sourceLocation: _sourceLocation(fileID: fileID, line: line))
}

public func XCTFail(
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    _recordFailure("XCTFail invoked", message: message(), fileID: fileID, line: line)
}

public func XCTAssertTrue(
    _ expression: @autoclosure () throws -> Bool,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        if try !expression() {
            _recordFailure("Expected condition to be true", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertFalse(
    _ expression: @autoclosure () throws -> Bool,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        if try expression() {
            _recordFailure("Expected condition to be false", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertEqual<T: Equatable>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        if l != r {
            _recordFailure("Expected \(l) to equal \(r)", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertEqual<T: BinaryFloatingPoint>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    accuracy: T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        let delta = l > r ? l - r : r - l
        if delta > accuracy {
            _recordFailure(
                "Expected \(l) to equal \(r) +/- \(accuracy) (delta=\(delta))",
                message: message(),
                fileID: fileID,
                line: line
            )
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertNil<T>(
    _ value: @autoclosure () throws -> T?,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        if let unwrapped = try value() {
            _recordFailure("Expected nil, got \(String(describing: unwrapped))", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertNotNil<T>(
    _ value: @autoclosure () throws -> T?,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        if try value() == nil {
            _recordFailure("Expected non-nil value", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertGreaterThan<T: Comparable>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        if !(l > r) {
            _recordFailure("Expected \(l) to be > \(r)", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertGreaterThanOrEqual<T: Comparable>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        if !(l >= r) {
            _recordFailure("Expected \(l) to be >= \(r)", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertLessThan<T: Comparable>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        if !(l < r) {
            _recordFailure("Expected \(l) to be < \(r)", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertLessThanOrEqual<T: Comparable>(
    _ lhs: @autoclosure () throws -> T,
    _ rhs: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) {
    do {
        let l = try lhs()
        let r = try rhs()
        if !(l <= r) {
            _recordFailure("Expected \(l) to be <= \(r)", message: message(), fileID: fileID, line: line)
        }
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
    }
}

public func XCTAssertThrowsError<T>(
    _ expression: @autoclosure () throws -> T,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line,
    _ errorHandler: (Error) -> Void = { _ in }
) {
    do {
        _ = try expression()
        _recordFailure("Expected expression to throw an error", message: message(), fileID: fileID, line: line)
    } catch {
        errorHandler(error)
    }
}

public func XCTUnwrap<T>(
    _ expression: @autoclosure () throws -> T?,
    _ message: @autoclosure () -> String = "",
    fileID: StaticString = #fileID,
    line: UInt = #line
) throws -> T {
    do {
        if let value = try expression() {
            return value
        }
        let reason = message().isEmpty ? "Expected non-nil value" : message()
        _recordFailure(reason, message: "", fileID: fileID, line: line)
        throw XCTestCompatError.unwrapFailed(reason)
    } catch {
        Issue.record(error, sourceLocation: _sourceLocation(fileID: fileID, line: line))
        throw error
    }
}
