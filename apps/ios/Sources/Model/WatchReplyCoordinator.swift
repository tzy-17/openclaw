import Foundation

@MainActor
final class WatchMessageOutbox {
    enum Decision {
        case dropMissingFields
        case dropMissingTarget
        case deduped(messageID: String)
        case queue(messageID: String)
        case forward
    }

    // Keep the shipped chat key so upgrades retain messages already queued by the Watch.
    private static let persistedQueueKey = "watch.chat.command.queue.v1"
    private static let maxRecentMessageIDs = 128

    private struct QueuedMessage: Codable, Equatable {
        var gatewayStableID: String
        var event: WatchAppCommandEvent
    }

    private let defaults: UserDefaults
    private var queuedMessages: [QueuedMessage] = []
    private var recentMessageIDs: [String] = []
    private var seenMessageIDs = Set<String>()

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        self.restoreQueue()
    }

    func ingest(
        _ event: WatchAppCommandEvent,
        isAvailable: Bool,
        gatewayStableID: String?) -> Decision
    {
        let messageID = event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
        let text = event.text?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if messageID.isEmpty || text.isEmpty {
            return .dropMissingFields
        }
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !owner.isEmpty else { return .dropMissingTarget }
        if self.seenMessageIDs.contains(messageID) {
            return .deduped(messageID: messageID)
        }
        self.rememberRecentMessageID(messageID)
        // Persist before network delivery; iOS may suspend a background callback at any await.
        self.queuedMessages.append(
            QueuedMessage(gatewayStableID: owner, event: self.message(event, taggedFor: owner)))
        self.rebuildSeenMessageIDs()
        self.persistQueue()
        return isAvailable ? .forward : .queue(messageID: messageID)
    }

    func nextQueuedMessage(isAvailable: Bool, gatewayStableID: String?) -> WatchAppCommandEvent? {
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard isAvailable, !owner.isEmpty else { return nil }
        // Replies are time-sensitive; a retrying chat must not strand them behind it.
        if let reply = self.queuedMessages.first(where: {
            $0.gatewayStableID == owner && self.kind(of: $0.event) == .quickReply
        }) {
            return reply.event
        }
        return self.queuedMessages.first { $0.gatewayStableID == owner }?.event
    }

    func removeQueuedMessage(messageID: String, gatewayStableID: String?) {
        let messageID = messageID.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !messageID.isEmpty, !owner.isEmpty else { return }
        guard let index = self.queuedMessages.firstIndex(where: {
            $0.gatewayStableID == owner && $0.event.commandId == messageID
        }) else { return }
        self.queuedMessages.remove(at: index)
        self.rememberRecentMessageID(messageID)
        self.persistQueue()
    }

    func requeueFront(_ event: WatchAppCommandEvent, gatewayStableID: String?) {
        let messageID = event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !messageID.isEmpty, !owner.isEmpty else { return }
        self.rememberRecentMessageID(messageID)
        self.queuedMessages.removeAll { $0.event.commandId == messageID }
        self.queuedMessages.insert(
            QueuedMessage(gatewayStableID: owner, event: self.message(event, taggedFor: owner)),
            at: 0)
        self.rebuildSeenMessageIDs()
        self.persistQueue()
    }

    func queuedCount(kind: WatchMessageKind? = nil) -> Int {
        guard let kind else { return self.queuedMessages.count }
        return self.queuedMessages.count(where: { self.kind(of: $0.event) == kind })
    }

    func queuedMessageIDs(kind: WatchMessageKind? = nil) -> [String] {
        self.queuedMessages.compactMap { queued in
            guard kind == nil || self.kind(of: queued.event) == kind else { return nil }
            return queued.event.commandId
        }
    }

    private func restoreQueue() {
        guard let data = defaults.data(forKey: Self.persistedQueueKey),
              let persisted = try? JSONDecoder().decode([QueuedMessage].self, from: data)
        else {
            return
        }

        var seen: [String] = []
        var seenSet = Set<String>()
        self.queuedMessages = persisted.compactMap { queued in
            let owner = queued.gatewayStableID.trimmingCharacters(in: .whitespacesAndNewlines)
            let messageID = queued.event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
            let text = queued.event.text?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard !owner.isEmpty, !messageID.isEmpty, !text.isEmpty, seenSet.insert(messageID).inserted else {
                return nil
            }
            seen.append(messageID)
            return QueuedMessage(gatewayStableID: owner, event: self.message(queued.event, taggedFor: owner))
        }
        self.recentMessageIDs = Array(seen.suffix(Self.maxRecentMessageIDs))
        self.rebuildSeenMessageIDs()
        if self.queuedMessages.count != persisted.count {
            self.persistQueue()
        }
    }

    private func rememberRecentMessageID(_ messageID: String) {
        guard !messageID.isEmpty else { return }
        self.recentMessageIDs.removeAll { $0 == messageID }
        self.recentMessageIDs.append(messageID)
        if self.recentMessageIDs.count > Self.maxRecentMessageIDs {
            self.recentMessageIDs.removeFirst(self.recentMessageIDs.count - Self.maxRecentMessageIDs)
        }
        self.rebuildSeenMessageIDs()
    }

    private func rebuildSeenMessageIDs() {
        var ids = Set(self.recentMessageIDs)
        ids.formUnion(self.queuedMessages.map(\.event.commandId))
        self.seenMessageIDs = ids
    }

    private func persistQueue() {
        if self.queuedMessages.isEmpty {
            self.defaults.removeObject(forKey: Self.persistedQueueKey)
            return
        }
        guard let data = try? JSONEncoder().encode(self.queuedMessages) else { return }
        self.defaults.set(data, forKey: Self.persistedQueueKey)
    }

    private func message(_ event: WatchAppCommandEvent, taggedFor gatewayStableID: String) -> WatchAppCommandEvent {
        var tagged = event
        tagged.gatewayStableID = gatewayStableID
        return tagged
    }

    private func kind(of event: WatchAppCommandEvent) -> WatchMessageKind {
        event.messageKind ?? .chat
    }

    static func resetPersistedQueue(defaults: UserDefaults = .standard) {
        defaults.removeObject(forKey: self.persistedQueueKey)
    }
}
