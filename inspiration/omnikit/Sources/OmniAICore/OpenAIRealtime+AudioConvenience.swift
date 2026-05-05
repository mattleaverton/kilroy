import Foundation

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public extension OpenAIRealtimeClient {
    func sendUserAudio(_ audioData: Data) async throws {
        try await sendUserAudio(RealtimeAudioUtilities.encodeBase64(audioData))
    }

    func appendInputAudio(_ audioData: Data) async throws {
        try await appendInputAudio(RealtimeAudioUtilities.encodeBase64(audioData))
    }

    func sendUserImage(_ imageData: Data, mediaType: String) async throws {
        let format = mediaType.split(separator: "/").last.map(String.init) ?? "png"
        try await sendUserImage(RealtimeAudioUtilities.encodeBase64(imageData), format: format)
    }

    func sendFunctionCallOutput(callId: String, output: JSONValue) async throws {
        let data = try output.data()
        let string = String(decoding: data, as: UTF8.self)
        try await sendFunctionCallOutput(callId: callId, output: string)
    }
}
