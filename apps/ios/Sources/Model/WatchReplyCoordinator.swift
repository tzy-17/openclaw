import Foundation

@MainActor
final class WatchReplyCoordinator {
    enum Decision {
        case dropMissingFields
        case dropMissingTarget
        case deduped(replyId: String)
        case queue(replyId: String, actionId: String)
        case forward
    }

    private static let persistedQueueKey = "watch.reply.queue.v1"
    private static let maxRecentReplyIds = 128

    private struct QueuedReply: Codable, Equatable {
        var gatewayStableID: String
        var event: WatchQuickReplyEvent
    }

    private let defaults: UserDefaults
    private var queuedReplies: [QueuedReply] = []
    private var recentReplyKeys: [String] = []
    private var seenReplyKeys = Set<String>()

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        self.restoreQueue()
    }

    func ingest(
        _ event: WatchQuickReplyEvent,
        isGatewayConnected: Bool,
        gatewayStableID: String?) -> Decision
    {
        let replyId = event.replyId.trimmingCharacters(in: .whitespacesAndNewlines)
        let actionId = event.actionId.trimmingCharacters(in: .whitespacesAndNewlines)
        if replyId.isEmpty || actionId.isEmpty {
            return .dropMissingFields
        }
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let replyKey = self.replyKey(gatewayStableID: owner, replyId: replyId)
        if self.seenReplyKeys.contains(replyKey) {
            return .deduped(replyId: replyId)
        }
        self.rememberRecentReplyKey(replyKey)
        if !isGatewayConnected {
            guard !owner.isEmpty else { return .dropMissingTarget }
            self.queuedReplies.append(QueuedReply(gatewayStableID: owner, event: event))
            self.rebuildSeenReplyKeys()
            self.persistQueue()
            return .queue(replyId: replyId, actionId: actionId)
        }
        return .forward
    }

    func nextQueuedReply(isGatewayConnected: Bool, gatewayStableID: String?) -> WatchQuickReplyEvent? {
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard isGatewayConnected, !owner.isEmpty else { return nil }
        return self.queuedReplies.first { $0.gatewayStableID == owner }?.event
    }

    func removeQueuedReply(replyId: String, gatewayStableID: String?) {
        let replyId = replyId.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !replyId.isEmpty, !owner.isEmpty else { return }
        guard let index = self.queuedReplies.firstIndex(where: {
            $0.gatewayStableID == owner && $0.event.replyId.trimmingCharacters(in: .whitespacesAndNewlines) == replyId
        }) else { return }
        self.queuedReplies.remove(at: index)
        self.rememberRecentReplyKey(self.replyKey(gatewayStableID: owner, replyId: replyId))
        self.persistQueue()
    }

    func requeueFront(_ event: WatchQuickReplyEvent, gatewayStableID: String?) {
        let replyId = event.replyId.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !owner.isEmpty else { return }
        if !replyId.isEmpty {
            self.rememberRecentReplyKey(self.replyKey(gatewayStableID: owner, replyId: replyId))
            self.queuedReplies.removeAll {
                $0.gatewayStableID == owner
                    && $0.event.replyId.trimmingCharacters(in: .whitespacesAndNewlines) == replyId
            }
        }
        self.queuedReplies.insert(QueuedReply(gatewayStableID: owner, event: event), at: 0)
        self.rebuildSeenReplyKeys()
        self.persistQueue()
    }

    var queuedCount: Int {
        self.queuedReplies.count
    }

    private func restoreQueue() {
        guard let data = defaults.data(forKey: Self.persistedQueueKey),
              let persisted = try? JSONDecoder().decode([QueuedReply].self, from: data)
        else {
            return
        }

        var seen: [String] = []
        var seenSet = Set<String>()
        self.queuedReplies = persisted.compactMap { queued in
            let owner = queued.gatewayStableID.trimmingCharacters(in: .whitespacesAndNewlines)
            let replyId = queued.event.replyId.trimmingCharacters(in: .whitespacesAndNewlines)
            let actionId = queued.event.actionId.trimmingCharacters(in: .whitespacesAndNewlines)
            let replyKey = self.replyKey(gatewayStableID: owner, replyId: replyId)
            guard !owner.isEmpty, !replyId.isEmpty, !actionId.isEmpty, seenSet.insert(replyKey).inserted else {
                return nil
            }
            seen.append(replyKey)
            var restored = queued.event
            restored.replyId = replyId
            restored.actionId = actionId
            return QueuedReply(gatewayStableID: owner, event: restored)
        }
        self.recentReplyKeys = Array(seen.suffix(Self.maxRecentReplyIds))
        self.rebuildSeenReplyKeys()
        if self.queuedReplies.count != persisted.count {
            self.persistQueue()
        }
    }

    private func rememberRecentReplyKey(_ replyKey: String) {
        guard !replyKey.isEmpty else { return }
        self.recentReplyKeys.removeAll { $0 == replyKey }
        self.recentReplyKeys.append(replyKey)
        if self.recentReplyKeys.count > Self.maxRecentReplyIds {
            self.recentReplyKeys.removeFirst(self.recentReplyKeys.count - Self.maxRecentReplyIds)
        }
        self.rebuildSeenReplyKeys()
    }

    private func rebuildSeenReplyKeys() {
        var ids = Set(self.recentReplyKeys)
        ids.formUnion(
            self.queuedReplies.map {
                self.replyKey(gatewayStableID: $0.gatewayStableID, replyId: $0.event.replyId)
            })
        self.seenReplyKeys = ids
    }

    private func persistQueue() {
        if self.queuedReplies.isEmpty {
            self.defaults.removeObject(forKey: Self.persistedQueueKey)
            return
        }
        guard let data = try? JSONEncoder().encode(self.queuedReplies) else { return }
        self.defaults.set(data, forKey: Self.persistedQueueKey)
    }

    private func replyKey(gatewayStableID: String, replyId: String) -> String {
        let owner = gatewayStableID.trimmingCharacters(in: .whitespacesAndNewlines)
        let replyId = replyId.trimmingCharacters(in: .whitespacesAndNewlines)
        return owner.isEmpty ? replyId : "\(owner):\(replyId)"
    }

    static func resetPersistedQueue(defaults: UserDefaults = .standard) {
        defaults.removeObject(forKey: self.persistedQueueKey)
    }
}

@MainActor
final class WatchChatCoordinator {
    enum Decision {
        case dropMissingFields
        case dropMissingTarget
        case deduped(commandId: String)
        case queue(commandId: String)
        case forward
    }

    private static let persistedQueueKey = "watch.chat.command.queue.v1"
    private static let maxRecentCommandIds = 128

    private struct QueuedCommand: Codable, Equatable {
        var gatewayStableID: String
        var event: WatchAppCommandEvent
    }

    private let defaults: UserDefaults
    private var queuedCommands: [QueuedCommand] = []
    private var recentCommandIds: [String] = []
    private var seenCommandIds = Set<String>()

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        self.restoreQueue()
    }

    func ingest(
        _ event: WatchAppCommandEvent,
        isChatAvailable: Bool,
        gatewayStableID: String?) -> Decision
    {
        let commandId = event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
        let text = event.text?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if commandId.isEmpty || text.isEmpty {
            return .dropMissingFields
        }
        if self.seenCommandIds.contains(commandId) {
            return .deduped(commandId: commandId)
        }
        self.rememberRecentCommandId(commandId)
        if !isChatAvailable {
            let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard !owner.isEmpty else { return .dropMissingTarget }
            self.queuedCommands.append(
                QueuedCommand(gatewayStableID: owner, event: self.command(event, taggedFor: owner)))
            self.rebuildSeenCommandIds()
            self.persistQueue()
            return .queue(commandId: commandId)
        }
        return .forward
    }

    func nextQueuedCommand(isChatAvailable: Bool, gatewayStableID: String?) -> WatchAppCommandEvent? {
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard isChatAvailable, !owner.isEmpty else { return nil }
        return self.queuedCommands.first { $0.gatewayStableID == owner }?.event
    }

    func removeQueuedCommand(commandId: String, gatewayStableID: String?) {
        let commandId = commandId.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !commandId.isEmpty, !owner.isEmpty else { return }
        guard let index = self.queuedCommands.firstIndex(where: {
            $0.gatewayStableID == owner && $0.event.commandId == commandId
        }) else { return }
        self.queuedCommands.remove(at: index)
        self.rememberRecentCommandId(commandId)
        self.persistQueue()
    }

    func requeueFront(_ event: WatchAppCommandEvent, gatewayStableID: String?) {
        let commandId = event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
        let owner = gatewayStableID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !owner.isEmpty else { return }
        if !commandId.isEmpty {
            self.rememberRecentCommandId(commandId)
            self.queuedCommands.removeAll { $0.event.commandId == commandId }
        }
        self.queuedCommands.insert(
            QueuedCommand(gatewayStableID: owner, event: self.command(event, taggedFor: owner)),
            at: 0)
        self.rebuildSeenCommandIds()
        self.persistQueue()
    }

    var queuedCount: Int {
        self.queuedCommands.count
    }

    var queuedCommandIds: [String] {
        self.queuedCommands.map(\.event.commandId)
    }

    private func restoreQueue() {
        guard let data = defaults.data(forKey: Self.persistedQueueKey),
              let persisted = try? JSONDecoder().decode([QueuedCommand].self, from: data)
        else {
            return
        }

        var seen: [String] = []
        var seenSet = Set<String>()
        self.queuedCommands = persisted.compactMap { queued in
            let owner = queued.gatewayStableID.trimmingCharacters(in: .whitespacesAndNewlines)
            let commandId = queued.event.commandId.trimmingCharacters(in: .whitespacesAndNewlines)
            let text = queued.event.text?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard !owner.isEmpty, !commandId.isEmpty, !text.isEmpty, seenSet.insert(commandId).inserted else {
                return nil
            }
            seen.append(commandId)
            return QueuedCommand(gatewayStableID: owner, event: self.command(queued.event, taggedFor: owner))
        }
        self.recentCommandIds = Array(seen.suffix(Self.maxRecentCommandIds))
        self.rebuildSeenCommandIds()
        if self.queuedCommands.count != persisted.count {
            self.persistQueue()
        }
    }

    private func rememberRecentCommandId(_ commandId: String) {
        guard !commandId.isEmpty else { return }
        self.recentCommandIds.removeAll { $0 == commandId }
        self.recentCommandIds.append(commandId)
        if self.recentCommandIds.count > Self.maxRecentCommandIds {
            self.recentCommandIds.removeFirst(self.recentCommandIds.count - Self.maxRecentCommandIds)
        }
        self.rebuildSeenCommandIds()
    }

    private func rebuildSeenCommandIds() {
        var ids = Set(self.recentCommandIds)
        ids.formUnion(self.queuedCommands.map(\.event.commandId))
        self.seenCommandIds = ids
    }

    private func persistQueue() {
        if self.queuedCommands.isEmpty {
            self.defaults.removeObject(forKey: Self.persistedQueueKey)
            return
        }
        guard let data = try? JSONEncoder().encode(queuedCommands) else { return }
        self.defaults.set(data, forKey: Self.persistedQueueKey)
    }

    private func command(_ event: WatchAppCommandEvent, taggedFor gatewayStableID: String) -> WatchAppCommandEvent {
        var tagged = event
        tagged.gatewayStableID = gatewayStableID
        return tagged
    }

    static func resetPersistedQueue(defaults: UserDefaults = .standard) {
        defaults.removeObject(forKey: self.persistedQueueKey)
    }
}
