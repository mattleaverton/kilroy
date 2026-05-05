import Testing
import Foundation
@testable import OmniAICore

struct RealtimeAudioUtilitiesTests {
    @Test
    func base64_round_trip_works() throws {
        let data = Data([0x00, 0x01, 0x7F, 0x80, 0xFF])
        let encoded = RealtimeAudioUtilities.encodeBase64(data)
        let decoded = try RealtimeAudioUtilities.decodeBase64(encoded)
        #expect(decoded == data)
    }

    @Test
    func wav_wrap_and_parse_round_trip() throws {
        let pcm = Data([0x01, 0x00, 0x02, 0x00, 0xFF, 0x7F, 0x00, 0x80])
        let wav = RealtimeAudioUtilities.makeWAV(fromPCM16: pcm, sampleRate: 24_000, channelCount: 1)
        let parsed = try RealtimeAudioUtilities.parseWAV(wav)
        #expect(parsed.data == pcm)
        #expect(parsed.sampleRate == 24_000)
        #expect(parsed.channelCount == 1)
    }

    @Test
    func audio_delta_decodes_bytes() throws {
        let event = RealtimeAudioDeltaEvent(delta: Data([1, 2, 3]).base64EncodedString(), contentIndex: 0, outputIndex: 1)
        #expect(try event.decodedAudioData() == Data([1, 2, 3]))
    }
}
