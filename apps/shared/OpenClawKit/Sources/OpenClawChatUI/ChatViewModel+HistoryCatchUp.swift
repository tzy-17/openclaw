import OpenClawKit
import OSLog

private let historyCatchUpLogger = Logger(subsystem: "ai.openclaw", category: "OpenClawChatUI")

extension OpenClawChatViewModel {
    /// Reconnect refetch: with a known cursor, fetch only missed rows and loop
    /// afterSeq = nextAfterSeq while hasMore. A response without the afterSeq
    /// echo means the gateway ignored the cursor param (version skew) and served
    /// a legacy full page; wholesale-replace via the standard path.
    @discardableResult
    func refreshHistoryCatchUp(historyRequest request: HistoryRequest) async -> Bool {
        guard var afterSeq = self.lastAppliedTranscriptSeq else {
            return await self.refreshHistoryAfterRun(historyRequest: request)
        }
        var appliedAny = false
        do {
            while true {
                let payload = try await transport.requestHistory(
                    sessionKey: request.session.key,
                    afterSeq: afterSeq)
                guard payload.afterSeq != nil else {
                    return self.applyHistoryPayload(
                        payload,
                        for: request,
                        preservingOptimisticLocalMessages: true)
                }
                guard
                    payload.afterSeq == afterSeq,
                    let expectedSessionId = self.sessionId,
                    payload.sessionId == expectedSessionId,
                    let nextAfterSeq = payload.nextAfterSeq,
                    nextAfterSeq >= afterSeq
                else {
                    return await self.refreshHistoryAfterRun(historyRequest: request)
                }
                if payload.hasMore == true, nextAfterSeq == afterSeq {
                    return await self.refreshHistoryAfterRun(historyRequest: request)
                }
                guard self.appendHistoryDeltaPage(payload, for: request) else { return appliedAny }
                appliedAny = true
                guard payload.hasMore == true else { return true }
                afterSeq = nextAfterSeq
            }
        } catch {
            historyCatchUpLogger.error("catch-up history failed \(error.localizedDescription, privacy: .public)")
            let refetched = await self.refreshHistoryAfterRun(historyRequest: request)
            return refetched || appliedAny
        }
    }

    /// Puts seq-stamped transcript rows back into seq order while seq-less rows
    /// (optimistic echoes, provisional finals) stay anchored at their positions.
    static func reorderTranscriptSeqRows(
        _ messages: [OpenClawChatMessage]) -> [OpenClawChatMessage]
    {
        let seqIndices = messages.indices.filter { messages[$0].transcriptSeq != nil }
        guard seqIndices.count > 1 else { return messages }
        let orderedRows = seqIndices.map { messages[$0] }
            .sorted { ($0.transcriptSeq ?? 0) < ($1.transcriptSeq ?? 0) }
        var result = messages
        for (offset, index) in seqIndices.enumerated() {
            result[index] = orderedRows[offset]
        }
        return result
    }

    /// The session.message envelope mirrors the row's transcript seq; adopt it
    /// when the projected body lost the metadata so seq-gap delta reordering can
    /// still place the pushed row at its transcript position.
    static func messageWithTranscriptSeqIfMissing(
        _ message: OpenClawChatMessage,
        seq: Int?) -> OpenClawChatMessage
    {
        guard message.transcriptSeq == nil, let seq, seq > 0 else { return message }
        return OpenClawChatMessage(
            id: message.id,
            role: message.role,
            content: message.content,
            timestamp: message.timestamp,
            idempotencyKey: message.idempotencyKey,
            transcriptSeq: seq,
            toolCallId: message.toolCallId,
            toolName: message.toolName,
            usage: message.usage,
            stopReason: message.stopReason,
            errorMessage: message.errorMessage)
    }
}
