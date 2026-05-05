import Foundation

public enum RealtimeAudioError: Error, Sendable, Equatable {
    case invalidBase64
    case invalidWAV(String)
}

public struct RealtimePCM16Audio: Sendable, Equatable {
    public var data: Data
    public var sampleRate: Int
    public var channelCount: Int

    public init(data: Data, sampleRate: Int, channelCount: Int = 1) {
        self.data = data
        self.sampleRate = sampleRate
        self.channelCount = channelCount
    }
}

public enum RealtimeAudioUtilities {
    public static func encodeBase64(_ data: Data) -> String {
        data.base64EncodedString()
    }

    public static func decodeBase64(_ base64: String) throws -> Data {
        guard let data = Data(base64Encoded: base64) else {
            throw RealtimeAudioError.invalidBase64
        }
        return data
    }

    public static func dataURL(mediaType: String, data: Data) -> String {
        "data:\(mediaType);base64,\(encodeBase64(data))"
    }

    public static func makeWAV(fromPCM16 pcm: Data, sampleRate: Int, channelCount: Int = 1) -> Data {
        let bitsPerSample = 16
        let byteRate = sampleRate * channelCount * bitsPerSample / 8
        let blockAlign = channelCount * bitsPerSample / 8
        let subchunk2Size = pcm.count
        let chunkSize = 36 + subchunk2Size

        var data = Data()
        data.append(contentsOf: Array("RIFF".utf8))
        appendLE(UInt32(chunkSize), to: &data)
        data.append(contentsOf: Array("WAVE".utf8))
        data.append(contentsOf: Array("fmt ".utf8))
        appendLE(UInt32(16), to: &data)
        appendLE(UInt16(1), to: &data)
        appendLE(UInt16(channelCount), to: &data)
        appendLE(UInt32(sampleRate), to: &data)
        appendLE(UInt32(byteRate), to: &data)
        appendLE(UInt16(blockAlign), to: &data)
        appendLE(UInt16(bitsPerSample), to: &data)
        data.append(contentsOf: Array("data".utf8))
        appendLE(UInt32(subchunk2Size), to: &data)
        data.append(pcm)
        return data
    }

    public static func parseWAV(_ wav: Data) throws -> RealtimePCM16Audio {
        let bytes = [UInt8](wav)
        guard bytes.count >= 44 else {
            throw RealtimeAudioError.invalidWAV("WAV too short")
        }
        guard String(bytes: bytes[0..<4], encoding: .ascii) == "RIFF",
              String(bytes: bytes[8..<12], encoding: .ascii) == "WAVE"
        else {
            throw RealtimeAudioError.invalidWAV("Missing RIFF/WAVE header")
        }

        let audioFormat = Int(leUInt16(bytes, at: 20))
        let channelCount = Int(leUInt16(bytes, at: 22))
        let sampleRate = Int(leUInt32(bytes, at: 24))
        let bitsPerSample = Int(leUInt16(bytes, at: 34))
        guard audioFormat == 1 else {
            throw RealtimeAudioError.invalidWAV("Unsupported audio format \(audioFormat)")
        }
        guard bitsPerSample == 16 else {
            throw RealtimeAudioError.invalidWAV("Unsupported bits per sample \(bitsPerSample)")
        }

        var index = 12
        while index + 8 <= bytes.count {
            guard let chunkID = String(bytes: bytes[index..<index+4], encoding: .ascii) else {
                break
            }
            let chunkSize = Int(leUInt32(bytes, at: index + 4))
            let chunkStart = index + 8
            let chunkEnd = chunkStart + chunkSize
            guard chunkEnd <= bytes.count else {
                throw RealtimeAudioError.invalidWAV("Truncated \(chunkID) chunk")
            }
            if chunkID == "data" {
                let pcm = Data(bytes[chunkStart..<chunkEnd])
                return RealtimePCM16Audio(data: pcm, sampleRate: sampleRate, channelCount: channelCount)
            }
            index = chunkEnd + (chunkSize % 2)
        }

        throw RealtimeAudioError.invalidWAV("Missing data chunk")
    }

    private static func appendLE<T: FixedWidthInteger>(_ value: T, to data: inout Data) {
        var le = value.littleEndian
        withUnsafeBytes(of: &le) { data.append(contentsOf: $0) }
    }

    private static func leUInt16(_ bytes: [UInt8], at index: Int) -> UInt16 {
        UInt16(bytes[index]) | (UInt16(bytes[index + 1]) << 8)
    }

    private static func leUInt32(_ bytes: [UInt8], at index: Int) -> UInt32 {
        UInt32(bytes[index])
            | (UInt32(bytes[index + 1]) << 8)
            | (UInt32(bytes[index + 2]) << 16)
            | (UInt32(bytes[index + 3]) << 24)
    }
}

public actor RealtimeAudioAccumulator {
    private var chunks: [Data] = []

    public init() {}

    @discardableResult
    public func appendBase64Delta(_ delta: String) throws -> Data {
        let decoded = try RealtimeAudioUtilities.decodeBase64(delta)
        chunks.append(decoded)
        return decoded
    }

    public func append(_ data: Data) {
        chunks.append(data)
    }

    public func audioData() -> Data {
        chunks.reduce(into: Data()) { $0.append($1) }
    }

    public func reset() {
        chunks.removeAll()
    }
}

public extension RealtimeAudioDeltaEvent {
    func decodedAudioData() throws -> Data {
        try RealtimeAudioUtilities.decodeBase64(delta)
    }
}

public extension RealtimeContentPart {
    func decodedOutputAudioData() throws -> Data? {
        switch self {
        case .outputAudio(let audio, _):
            guard let audio else { return nil }
            return try RealtimeAudioUtilities.decodeBase64(audio)
        default:
            return nil
        }
    }
}
