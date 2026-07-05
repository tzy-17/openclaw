import Foundation
import OpenClawKit
import Testing
@testable import OpenClawChatUI

// Covers the chat.history afterSeq catch-up cursor: delta append on reconnect,
// multi-page loops, legacy full-page fallback, and seq-gap streaming cleanup.

private func seqMessage(
    role: String,
    text: String,
    timestamp: Double,
    seq: Int? = nil,
    idempotencyKey: String? = nil) -> AnyCodable
{
    var message: [String: Any] = [
        "role": role,
        "content": [["type": "text", "text": text]],
        "timestamp": timestamp,
    ]
    var meta: [String: Any] = [:]
    if let seq {
        meta["seq"] = seq
    }
    if let idempotencyKey {
        meta["idempotencyKey"] = idempotencyKey
    }
    if !meta.isEmpty {
        message["__openclaw"] = meta
    }
    return AnyCodable(message)
}

private func fullPayload(
    sessionId: String? = "sess-main",
    messages: [AnyCodable]) -> OpenClawChatHistoryPayload
{
    OpenClawChatHistoryPayload(
        sessionKey: "main",
        sessionId: sessionId,
        messages: messages,
        thinkingLevel: "off")
}

private func deltaPayload(
    afterSeq: Int,
    nextAfterSeq: Int,
    hasMore: Bool,
    totalMessages: Int,
    messages: [AnyCodable]) -> OpenClawChatHistoryPayload
{
    OpenClawChatHistoryPayload(
        sessionKey: "main",
        sessionId: "sess-main",
        messages: messages,
        thinkingLevel: "off",
        afterSeq: afterSeq,
        nextAfterSeq: nextAfterSeq,
        hasMore: hasMore,
        totalMessages: totalMessages)
}

private actor AfterSeqTransportState {
    var fullHistoryCalls: [String] = []
    var deltaCalls: [Int] = []
    var sentRunIds: [String] = []

    func recordFullHistory(_ sessionKey: String) -> Int {
        self.fullHistoryCalls.append(sessionKey)
        return self.fullHistoryCalls.count - 1
    }

    func recordDelta(_ afterSeq: Int) -> Int {
        self.deltaCalls.append(afterSeq)
        return self.deltaCalls.count - 1
    }

    func recordSentRunId(_ runId: String) {
        self.sentRunIds.append(runId)
    }
}

private final class AfterSeqChatTransport: @unchecked Sendable, OpenClawChatTransport {
    private let state = AfterSeqTransportState()
    private let fullResponses: [OpenClawChatHistoryPayload]
    private let deltaResponses: [OpenClawChatHistoryPayload]
    private let sendMessageStatus: String

    private let stream: AsyncStream<OpenClawChatTransportEvent>
    private let continuation: AsyncStream<OpenClawChatTransportEvent>.Continuation

    init(
        fullResponses: [OpenClawChatHistoryPayload],
        deltaResponses: [OpenClawChatHistoryPayload] = [],
        sendMessageStatus: String = "ok")
    {
        self.fullResponses = fullResponses
        self.deltaResponses = deltaResponses
        self.sendMessageStatus = sendMessageStatus
        var cont: AsyncStream<OpenClawChatTransportEvent>.Continuation!
        self.stream = AsyncStream { c in
            cont = c
        }
        self.continuation = cont
    }

    func events() -> AsyncStream<OpenClawChatTransportEvent> {
        self.stream
    }

    func emit(_ evt: OpenClawChatTransportEvent) {
        self.continuation.yield(evt)
    }

    func requestHistory(sessionKey: String) async throws -> OpenClawChatHistoryPayload {
        let idx = await self.state.recordFullHistory(sessionKey)
        if idx < self.fullResponses.count {
            return self.fullResponses[idx]
        }
        return self.fullResponses.last ?? fullPayload(messages: [])
    }

    func requestHistory(sessionKey _: String, afterSeq: Int) async throws -> OpenClawChatHistoryPayload {
        let idx = await self.state.recordDelta(afterSeq)
        guard idx < self.deltaResponses.count else {
            throw NSError(
                domain: "AfterSeqChatTransport",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "unexpected delta request afterSeq=\(afterSeq)"])
        }
        return self.deltaResponses[idx]
    }

    func sendMessage(
        sessionKey _: String,
        message _: String,
        thinking _: String,
        idempotencyKey: String,
        attachments _: [OpenClawChatAttachmentPayload]) async throws -> OpenClawChatSendResponse
    {
        await self.state.recordSentRunId(idempotencyKey)
        return OpenClawChatSendResponse(runId: idempotencyKey, status: self.sendMessageStatus)
    }

    func requestHealth(timeoutMs _: Int) async throws -> Bool {
        true
    }

    func fullHistoryCalls() async -> [String] {
        await self.state.fullHistoryCalls
    }

    func deltaCalls() async -> [Int] {
        await self.state.deltaCalls
    }

    func lastSentRunId() async -> String? {
        await self.state.sentRunIds.last
    }
}

@MainActor
private func makeLoadedViewModel(
    transport: AfterSeqChatTransport) async throws -> OpenClawChatViewModel
{
    let vm = OpenClawChatViewModel(sessionKey: "main", transport: transport)
    vm.load()
    try await waitUntil("bootstrap") {
        await MainActor.run { vm.healthOK && !vm.isLoading }
    }
    return vm
}

@MainActor
private func visibleTexts(_ vm: OpenClawChatViewModel) -> [String] {
    vm.messages.flatMap { $0.content.compactMap(\.text) }
}

@Suite(.timeLimit(.minutes(1)))
struct ChatHistoryAfterSeqTests {
    @Test
    func seqGapDeltaFetchAppendsWithoutDuplicates() async throws {
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        // Lossless overlap: the trimmed tail row (seq 2) re-arrives with the delta.
        let delta = deltaPayload(
            afterSeq: 2,
            nextAfterSeq: 4,
            hasMore: false,
            totalMessages: 4,
            messages: [
                seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
                seqMessage(role: "assistant", text: "missed you", timestamp: 3000, seq: 3),
            ])
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [delta])
        let vm = try await makeLoadedViewModel(transport: transport)

        transport.emit(.seqGap)
        try await waitUntil("delta applied") {
            await MainActor.run { vm.messages.count == 3 }
        }

        #expect(await transport.deltaCalls() == [2])
        #expect(await transport.fullHistoryCalls().count == 1)
        let texts = await visibleTexts(vm)
        #expect(texts == ["hi", "hello", "missed you"])
    }

    @Test
    func seqGapCatchUpLoopsThroughAllPages() async throws {
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        let pageOne = deltaPayload(
            afterSeq: 2,
            nextAfterSeq: 3,
            hasMore: true,
            totalMessages: 4,
            messages: [seqMessage(role: "assistant", text: "part one", timestamp: 3000, seq: 3)])
        let pageTwo = deltaPayload(
            afterSeq: 3,
            nextAfterSeq: 4,
            hasMore: false,
            totalMessages: 4,
            messages: [seqMessage(role: "assistant", text: "part two", timestamp: 4000, seq: 4)])
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [pageOne, pageTwo])
        let vm = try await makeLoadedViewModel(transport: transport)

        transport.emit(.seqGap)
        try await waitUntil("both pages applied") {
            await MainActor.run { vm.messages.count == 4 }
        }

        #expect(await transport.deltaCalls() == [2, 3])
        let texts = await visibleTexts(vm)
        #expect(texts == ["hi", "hello", "part one", "part two"])
    }

    @Test
    func deltaRowsLandBeforeNewerPushedRowDeliveredDuringGap() async throws {
        // The transport delivers the gap-triggering newer push right after the
        // seqGap signal, so it can apply before the async delta returns. The
        // missed older rows must still land at their transcript positions.
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        let delta = deltaPayload(
            afterSeq: 2,
            nextAfterSeq: 4,
            hasMore: false,
            totalMessages: 4,
            messages: [
                seqMessage(role: "assistant", text: "missed", timestamp: 3000, seq: 3),
                seqMessage(role: "assistant", text: "newest", timestamp: 4000, seq: 4),
            ])
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [delta])
        let vm = try await makeLoadedViewModel(transport: transport)

        // Newer row (seq 4) lands via push before the catch-up fetch runs.
        let pushed = try #require(try? JSONDecoder().decode(
            OpenClawChatMessage.self,
            from: JSONEncoder().encode(
                seqMessage(role: "assistant", text: "newest", timestamp: 4000, seq: 4))))
        transport.emit(
            .sessionMessage(
                OpenClawSessionMessageEventPayload(
                    sessionKey: "main",
                    message: pushed,
                    messageId: "m4",
                    messageSeq: 4)))
        try await waitUntil("pushed row applied") {
            await MainActor.run { vm.messages.count == 3 }
        }

        transport.emit(.seqGap)
        try await waitUntil("delta reordered") {
            await MainActor.run { vm.messages.count == 4 }
        }

        #expect(await transport.deltaCalls() == [2])
        let texts = await visibleTexts(vm)
        #expect(texts == ["hi", "hello", "missed", "newest"])
    }

    @Test
    func legacyResponseWithoutCursorEchoFallsBackToFullReplace() async throws {
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        // Gateway ignored afterSeq (version skew): full page without the cursor
        // echo, and without the first row. Wholesale replace must drop that row;
        // an append would have kept it.
        let legacy = fullPayload(messages: [
            seqMessage(role: "assistant", text: "hello", timestamp: 2000),
            seqMessage(role: "assistant", text: "rewritten", timestamp: 3000),
        ])
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [legacy])
        let vm = try await makeLoadedViewModel(transport: transport)

        transport.emit(.seqGap)
        try await waitUntil("legacy replace applied") {
            await MainActor.run { vm.messages.contains { message in
                message.content.contains { $0.text == "rewritten" }
            } }
        }

        #expect(await transport.deltaCalls() == [2])
        let texts = await visibleTexts(vm)
        #expect(texts == ["hello", "rewritten"])
    }

    @Test
    func freshSessionWithoutSeqUsesFullFetch() async throws {
        // Bootstrap rows carry no `__openclaw.seq`, so no cursor exists and the
        // seq-gap refetch must use the legacy full-history request.
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, idempotencyKey: "u1"),
        ])
        let refreshed = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000),
        ])
        let transport = AfterSeqChatTransport(fullResponses: [bootstrap, refreshed])
        let vm = try await makeLoadedViewModel(transport: transport)

        transport.emit(.seqGap)
        try await waitUntil("full refetch applied") {
            await MainActor.run { vm.messages.count == 2 }
        }

        #expect(await transport.deltaCalls().isEmpty)
        #expect(await transport.fullHistoryCalls().count == 2)
    }

    @Test
    func seqGapClearsStaleStreamingText() async throws {
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        let delta = deltaPayload(
            afterSeq: 2,
            nextAfterSeq: 3,
            hasMore: false,
            totalMessages: 3,
            messages: [seqMessage(role: "assistant", text: "final", timestamp: 3000, seq: 3)])
        // "accepted" keeps the run pending so streamed text stays live until the gap.
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [delta],
            sendMessageStatus: "accepted")
        let vm = try await makeLoadedViewModel(transport: transport)

        await MainActor.run {
            vm.input = "question"
            vm.send()
        }
        try await waitUntil("run pending") {
            await MainActor.run { vm.pendingRunCount == 1 }
        }
        let runId = try #require(await transport.lastSentRunId())
        transport.emit(
            .agent(
                OpenClawAgentEventPayload(
                    runId: runId,
                    seq: 1,
                    stream: "assistant",
                    ts: Int(Date().timeIntervalSince1970 * 1000),
                    data: ["text": AnyCodable("streaming...")])))
        try await waitUntil("streaming text visible") {
            await MainActor.run { vm.streamingAssistantText == "streaming..." }
        }

        transport.emit(.seqGap)
        try await waitUntil("gap reconcile clears streaming text") {
            await MainActor.run {
                vm.streamingAssistantText == nil && vm.pendingRunCount == 0
            }
        }
        #expect(await transport.deltaCalls().first == 2)
    }

    @Test
    func foregroundResumeWithPendingRunUsesDeltaFetch() async throws {
        let bootstrap = fullPayload(messages: [
            seqMessage(role: "user", text: "hi", timestamp: 1000, seq: 1, idempotencyKey: "u1"),
            seqMessage(role: "assistant", text: "hello", timestamp: 2000, seq: 2),
        ])
        let now = Date().timeIntervalSince1970 * 1000
        let delta = deltaPayload(
            afterSeq: 2,
            nextAfterSeq: 4,
            hasMore: false,
            totalMessages: 4,
            messages: [seqMessage(role: "assistant", text: "answer", timestamp: now + 60000, seq: 4)])
        let transport = AfterSeqChatTransport(
            fullResponses: [bootstrap],
            deltaResponses: [delta],
            sendMessageStatus: "accepted")
        let vm = try await makeLoadedViewModel(transport: transport)

        await MainActor.run {
            vm.input = "question"
            vm.send()
        }
        try await waitUntil("run pending") {
            await MainActor.run { vm.pendingRunCount == 1 }
        }

        await MainActor.run { vm.resumeFromForeground() }
        try await waitUntil("foreground delta applied") {
            await MainActor.run { vm.messages.contains { message in
                message.content.contains { $0.text == "answer" }
            } }
        }
        #expect(await transport.deltaCalls() == [2])
    }
}
