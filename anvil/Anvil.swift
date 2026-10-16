// Anvil menu bar app. Talks to the Anvil service over its private Unix socket; system frameworks only.
import AppKit
import CryptoKit
import SwiftUI

// MARK: - Transport

struct ServiceInfo {
    var python = "/usr/bin/python3", root = "", home = NSHomeDirectory() + "/.anvil"
    static let shared: ServiceInfo = {
        var s = ServiceInfo()
        if let url = Bundle.main.url(forResource: "service", withExtension: "json"),
           let d = try? Data(contentsOf: url),
           let o = try? JSONSerialization.jsonObject(with: d) as? [String: String] {
            s.python = o["python"] ?? s.python; s.root = o["root"] ?? ""; s.home = o["home"] ?? s.home
        }
        return s
    }()
    /// Same rule as anvil/config.py: macOS limits AF_UNIX paths to 104 bytes.
    var socket: String {
        let p = home + "/anvil.sock"
        if p.utf8.count <= 100 { return p }
        let h = Insecure.SHA1.hash(data: Data(home.utf8)).map { String(format: "%02x", $0) }.joined().prefix(10)
        return "/tmp/anvil-\(getuid())-\(h).sock"
    }
}

struct APIError: Error { let message: String }

/// Minimal HTTP/1.0 over AF_UNIX: one request per connection, read to EOF.
func call(_ method: String, _ path: String, _ body: [String: Any]? = nil, timeout: Int = 10) throws -> Any {
    let fd = socket(AF_UNIX, SOCK_STREAM, 0)
    guard fd >= 0 else { throw APIError(message: "socket failed") }
    defer { close(fd) }
    var tv = timeval(tv_sec: timeout, tv_usec: 0)
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
    var one: Int32 = 1  // a service that closes mid-request must not kill the app with SIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    let p = Array(ServiceInfo.shared.socket.utf8)
    withUnsafeMutableBytes(of: &addr.sun_path) { raw in
        for (i, b) in p.enumerated() where i < raw.count - 1 { raw[i] = b }
    }
    let rc = withUnsafePointer(to: &addr) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
    }
    guard rc == 0 else { throw APIError(message: "Anvil service is not running") }
    var payload = Data()
    if let body {
        guard let d = try? JSONSerialization.data(withJSONObject: body) else { throw APIError(message: "couldn't encode the request") }
        payload = d
    }
    var req = Data("\(method) \(path) HTTP/1.0\r\nHost: anvil\r\nContent-Type: application/json\r\nContent-Length: \(payload.count)\r\n\r\n".utf8)
    req.append(payload)
    var sent = 0
    while sent < req.count {
        let n = req.withUnsafeBytes { write(fd, $0.baseAddress! + sent, req.count - sent) }
        guard n > 0 else { throw APIError(message: "write failed") }
        sent += n
    }
    var resp = Data()
    var buf = [UInt8](repeating: 0, count: 65536)
    while true {
        let n = read(fd, &buf, buf.count)
        if n <= 0 { break }
        resp.append(buf, count: n)
    }
    guard let sep = resp.range(of: Data("\r\n\r\n".utf8)) else { throw APIError(message: "no response") }
    let head = String(decoding: resp[..<sep.lowerBound], as: UTF8.self)
    let status = Int(head.split(separator: " ").dropFirst().first ?? "") ?? 0
    guard let obj = try? JSONSerialization.jsonObject(with: resp[sep.upperBound...]) else {
        throw APIError(message: status == 0 ? "no response from the service (busy?)" : "unreadable response (\(status))")
    }
    if status >= 400 {
        throw APIError(message: (obj as? [String: Any])?["message"] as? String ?? "error \(status)")
    }
    return obj
}

func q(_ items: [String: String?]) -> String {
    var c = URLComponents()
    c.queryItems = items.compactMap { k, v in v.map { URLQueryItem(name: k, value: $0) } }
    // URLComponents leaves "+" alone, which the server reads as a space ("c++bench" would filter to nothing)
    return c.queryItems!.isEmpty ? "" : "?" + (c.percentEncodedQuery ?? "").replacingOccurrences(of: "+", with: "%2B")
}

// MARK: - Model

struct Job: Identifiable, Equatable {
    let d: [String: Any]
    var id: String { d["id"] as? String ?? "" }
    func s(_ k: String) -> String? { d[k] as? String }
    func n(_ k: String) -> Double? { (d[k] as? NSNumber)?.doubleValue }
    var status: String { s("status") ?? "" }
    var project: String { s("project") ?? "" }
    var agent: String { s("agent") ?? "" }
    var name: String { s("name") ?? "" }
    var pending: Bool { status == "queued" || status == "held" }
    var progress: [String: Any]? { d["progress"] as? [String: Any] }
    static func == (a: Job, b: Job) -> Bool { NSDictionary(dictionary: a.d).isEqual(to: b.d) }
}

/// Time limit as typed in the panel: "90", "30m", "2h"; empty, "0" or "none" means no limit ("none" to the service).
func parseLimit(_ t: String) -> Any? {
    let v = t.trimmingCharacters(in: .whitespaces).lowercased()
    if v.isEmpty || v == "0" || v == "none" { return "none" }
    let mult: Double = v.hasSuffix("h") ? 3600 : v.hasSuffix("m") ? 60 : 1
    guard let n = Double(mult == 1 ? v.replacingOccurrences(of: "s", with: "") : String(v.dropLast())), n > 0 else { return nil }
    return n * mult
}

func limitText(_ s: Double) -> String {
    let t = Int(s)
    return t % 3600 == 0 ? "\(t / 3600)h" : t % 60 == 0 ? "\(t / 60)m" : "\(t)"
}

func fmt(_ s: Double?) -> String {
    guard let s else { return "—" }
    let t = max(0, Int(s.rounded()))
    if t >= 3600 { return String(format: "%dh %02dm", t / 3600, t % 3600 / 60) }
    if t >= 60 { return String(format: "%dm %02ds", t / 60, t % 60) }
    return "\(t)s"
}

@MainActor
final class Store: ObservableObject {
    @Published var gpu = "idle"
    @Published var paused = false
    @Published var queue: [Job] = []
    @Published var recent: [Job] = []
    @Published var older: [Job] = []
    @Published var olderNext: String?
    @Published var outside: [[String: Any]] = []
    @Published var projects: [String] = []
    @Published var agents: [String] = []
    @Published var connected = false
    @Published var fetchedAt = Date()
    @Published var message: String?
    @Published var messageIsError = false
    /// One pending confirmation (stop, clear), shown above the list: (question, button, action).
    @Published var confirm: (text: String, button: String, action: () -> Void)?
    @Published var attention: [[String: Any]] = []
    @Published var pinned = false { didSet { onPin?(pinned) } }
    // Row reordering, driven by the panel's own mouse tracking (see Panel.sendEvent).
    @Published var dragID: String?
    @Published var dragY: CGFloat = 0
    var dragStartY: CGFloat = 0
    var rowFrames: [String: CGRect] = [:]  // queue rows, window coordinates with a top-left origin

    func row(at p: CGPoint) -> String? {
        pendingJobs.first { rowFrames[$0.id]?.contains(p) ?? false }?.id
    }

    var dropIndex: Int {
        pendingJobs.firstIndex { $0.id != dragID && (rowFrames[$0.id]?.midY ?? 0) > dragY } ?? pendingJobs.count
    }
    var onPin: ((Bool) -> Void)?
    @Published var expanded: String?
    @Published var detail: [String: Any]?
    @Published var log = ""
    @Published var project: String? { didSet { gen += 1; older = []; olderNext = nil; refresh() } }
    @Published var agent: String? { didSet { gen += 1; older = []; olderNext = nil; refresh() } }
    nonisolated(unsafe) var cursor: Int?  // bootstrap for the poller, which then follows the wait replies' cursors
    nonisolated(unsafe) var fastPoll = false  // an expanded running job: refresh its log every 2s
    private var gen = 0, inFlight = false, dirty = false
    var onChange: (() -> Void)?
    private var lastStart = Date.distantPast

    var running: Job? { queue.first { $0.status == "running" } }
    var pendingJobs: [Job] { queue.filter { $0.pending } }

    func start() {
        Thread.detachNewThread { [weak self] in
            var cur: Int?
            while true {
                guard let self else { return }
                if cur == nil { cur = self.cursor }
                do {
                    if let c = cur {
                        let t = self.fastPoll ? 2 : 25
                        let r = try call("GET", "/api/wait" + q(["cursor": String(c), "timeout": String(t)]), timeout: t + 10)
                        cur = ((r as? [String: Any])?["cursor"] as? NSNumber)?.intValue ?? c  // never re-read the same events
                    } else {
                        _ = try call("GET", "/api/health")
                    }
                    DispatchQueue.main.async { self.refresh() }
                    if cur == nil { Thread.sleep(forTimeInterval: 0.5) }
                } catch {
                    cur = nil
                    DispatchQueue.main.async { self.connected = false; self.cursor = nil; self.onChange?(); self.ensureService() }
                    Thread.sleep(forTimeInterval: 2)
                }
            }
        }
    }

    func ensureService() {
        guard Date().timeIntervalSince(lastStart) > 30 else { return }
        lastStart = Date()
        let info = ServiceInfo.shared
        guard !info.root.isEmpty else { return }
        let p = Process()
        p.executableURL = URL(fileURLWithPath: info.python)
        p.arguments = ["-m", "anvil", "serve"]
        p.currentDirectoryURL = URL(fileURLWithPath: info.root)
        p.environment = ["ANVIL_HOME": info.home, "PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": NSHomeDirectory()]
        try? FileManager.default.createDirectory(atPath: info.home, withIntermediateDirectories: true)
        if !FileManager.default.fileExists(atPath: info.home + "/service.log") {
            FileManager.default.createFile(atPath: info.home + "/service.log", contents: nil)
        }
        let log = FileHandle(forWritingAtPath: info.home + "/service.log")
        log?.seekToEndOfFile()
        p.standardOutput = log; p.standardError = log
        try? p.run()
    }

    /// One overview request in flight at a time; a refresh asked for meanwhile runs right after, and a reply
    /// from before a filter change is dropped, so an older snapshot never overwrites a newer one.
    func refresh() {
        if inFlight { dirty = true; return }
        inFlight = true
        let path = "/api/overview" + q(["project": project, "agent": agent])
        let exp = expanded, g = gen
        DispatchQueue.global().async {
            let o = try? call("GET", path) as? [String: Any]
            var det: [String: Any]?, log: String?
            if let exp, o != nil {
                det = try? call("GET", "/api/jobs/\(exp)/detail") as? [String: Any]
                log = (try? call("GET", "/api/jobs/\(exp)/logs?tail=6000") as? [String: Any])?["data"] as? String
            }
            DispatchQueue.main.async {
                self.inFlight = false
                defer { if self.dirty { self.dirty = false; self.refresh() } }
                guard g == self.gen else { self.dirty = true; return }
                guard let o else { self.connected = false; self.onChange?(); return }
                self.connected = true
                self.fetchedAt = Date()
                self.gpu = o["gpu"] as? String ?? "idle"
                self.paused = o["paused"] as? Bool ?? false
                self.queue = (o["queue"] as? [[String: Any]] ?? []).map(Job.init)
                self.recent = (o["recent"] as? [[String: Any]] ?? []).map(Job.init)
                self.outside = o["outside"] as? [[String: Any]] ?? []
                self.projects = o["projects"] as? [String] ?? []
                self.agents = o["agents"] as? [String] ?? []
                self.attention = o["attention"] as? [[String: Any]] ?? []
                self.cursor = (o["cursor"] as? NSNumber)?.intValue
                if exp == self.expanded, let det { self.detail = det; self.log = log ?? "" }
                self.fastPoll = self.expanded != nil && self.queue.contains { $0.id == self.expanded && $0.status == "running" }
                self.onChange?()
            }
        }
    }

    func act(_ path: String, _ body: [String: Any] = [:]) {
        var b = body
        b["actor"] = "panel"
        DispatchQueue.global().async {
            do { _ = try call("POST", path, b, timeout: 30) } catch let e as APIError {
                DispatchQueue.main.async { self.flash(e.message, error: true) }
            } catch {
                DispatchQueue.main.async { self.flash("\(error)", error: true) }
            }
            DispatchQueue.main.async {
                if !self.older.isEmpty { self.older = []; self.olderNext = nil; self.loadOlder() }  // drop removed rows
                self.refresh()
            }
        }
    }

    func control(_ id: String, _ op: String, _ body: [String: Any] = [:]) { act("/api/jobs/\(id)/\(op)", body) }

    func toggle(_ id: String) {
        expanded = expanded == id ? nil : id
        detail = nil; log = ""
        refresh()
    }

    func flash(_ m: String, error: Bool = false) {
        message = m; messageIsError = error
        DispatchQueue.main.asyncAfter(deadline: .now() + (error ? 8 : 4)) { if self.message == m { self.message = nil } }
    }

    func ask(_ text: String, _ button: String, _ action: @escaping () -> Void) { confirm = (text, button, action) }

    func stop(_ j: Job) {
        ask("Stop \(j.name) (\(j.id), running \(fmt(j.n("elapsed_s"))))?", "Stop") { self.control(j.id, "cancel") }
    }

    /// Drop a pending job at index `i` of the visible queue. Next to the same agent's jobs it only reorders them
    /// (fair sharing continues); anywhere else the service pins the jobs above it in the order shown (visible
    /// "next"), or, below every other queued job, moves it to the back. Held rows don't count as places.
    func move(_ id: String, to i: Int) {
        let jobs = pendingJobs
        guard let from = jobs.firstIndex(where: { $0.id == id }), i != from, i != from + 1 else { return }
        let j = jobs[from]
        guard j.status == "queued" else { flash("\(id) is held: release it before moving it", error: true); return }
        func same(_ o: Job) -> Bool { o.project == j.project && o.agent == j.agent }
        let others = jobs.filter { $0.status == "queued" && $0.id != id }
        let k = jobs[..<i].filter { $0.status == "queued" && $0.id != id }.count  // queued jobs that stay above it
        if others.isEmpty { return }
        if k == 0 {
            if same(others[0]) { control(id, "reorder", ["before": others[0].id]) }
            else { control(id, "move", ["before": others[0].id]) }
        } else if k >= others.count {
            if same(others[k - 1]) { control(id, "reorder", ["after": others[k - 1].id]) }
            else { control(id, "move", ["last": true]) }
        } else if same(others[k]) {
            control(id, "reorder", ["before": others[k].id])
        } else if same(others[k - 1]) {
            control(id, "reorder", ["after": others[k - 1].id])
        } else {
            control(id, "move", ["before": others[k].id])
        }
    }

    func clearQueue() {
        let n = pendingJobs.count
        ask("Cancel \(n) queued or held job\(n == 1 ? "" : "s")\(project.map { " in " + $0 } ?? "")\(agent.map { " from " + $0 } ?? "")? Their agents are told.",
            "Clear Queue") {
            self.act("/api/clear", ["project": self.project as Any, "agent": self.agent as Any, "queue": true])
        }
    }

    func clearFinished() {
        ask("Remove all finished jobs from the list? Their logs are deleted.", "Clear Finished") {
            self.act("/api/clear", ["project": self.project as Any, "agent": self.agent as Any])
        }
    }

    /// Acknowledge every failure behind "Needs Attention" (drift holds need a release or cancel instead).
    func dismissAll() {
        for a in attention where (a["status"] as? String).map(failed.contains) == true {
            if let id = a["id"] as? String { control(id, "ack") }
        }
    }

    func loadOlder() {
        let path = "/api/jobs" + q(["status": "succeeded,failed,canceled,timed_out,interrupted", "limit": "20",
                                    "project": project, "agent": agent, "after": olderNext ?? recent.last?.id])
        DispatchQueue.global().async {
            guard let r = try? call("GET", path) as? [String: Any] else {
                DispatchQueue.main.async { self.flash("Couldn't load older jobs", error: true) }
                return
            }
            DispatchQueue.main.async {
                let seen = Set((self.recent + self.older).map(\.id))
                self.older += (r["jobs"] as? [[String: Any]] ?? []).map(Job.init).filter { !seen.contains($0.id) }
                self.olderNext = r["next"] as? String
            }
        }
    }
}

// MARK: - Views

let failed: Set<String> = ["failed", "timed_out", "interrupted"]
let rowHeight: CGFloat = 22

func statusColor(_ s: String) -> Color {
    switch s {
    case "running", "succeeded": return .green
    case "held": return .orange
    case _ where failed.contains(s): return .red
    default: return .secondary
    }
}

func statusSymbol(_ s: String) -> String {
    switch s {
    case "succeeded": return "checkmark.circle.fill"
    case "canceled": return "minus.circle"
    case "timed_out": return "clock.badge.exclamationmark"
    case "held": return "pause.circle"
    case _ where failed.contains(s): return "xmark.circle.fill"
    default: return "circle"
    }
}

struct HeightKey: PreferenceKey {
    static let defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) { value = max(value, nextValue()) }
}

struct NoticeHeightKey: PreferenceKey {
    static let defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) { value = max(value, nextValue()) }
}

struct RowFrames: PreferenceKey {
    static let defaultValue: [String: CGRect] = [:]
    static func reduce(value: inout [String: CGRect], nextValue: () -> [String: CGRect]) {
        value.merge(nextValue()) { $1 }
    }
}

struct PanelView: View {
    @ObservedObject var store: Store
    var onHeight: (CGFloat) -> Void
    @State private var showAllRecent = false
    @State private var contentHeight: CGFloat = 0
    @State private var noticeHeight: CGFloat = 0

    static let chrome: CGFloat = 28 + 24  // header + footer

    var body: some View {
        VStack(spacing: 0) {
            header
            VStack(spacing: 0) { notices }  // above the list, so they never scroll out of sight
                .background(GeometryReader { Color.clear.preference(key: NoticeHeightKey.self, value: $0.size.height) })
            ScrollView {
                content.background(GeometryReader { Color.clear.preference(key: HeightKey.self, value: $0.size.height) })
            }
            .scrollBounceBehavior(.basedOnSize)
            footer
        }
        .onPreferenceChange(HeightKey.self) { contentHeight = $0; onHeight($0 + noticeHeight + Self.chrome) }
        .onPreferenceChange(NoticeHeightKey.self) { noticeHeight = $0; onHeight(contentHeight + $0 + Self.chrome) }
    }

    var header: some View {
        HStack(spacing: 10) {
            StateBadge(gpu: store.connected ? store.gpu : "offline", paused: store.paused)
            Spacer()
            Button { store.act(store.paused ? "/api/resume" : "/api/pause") } label: {
                Image(systemName: store.paused ? "play.fill" : "pause.fill")
            }
            .help(store.paused ? "Resume new starts" : "Pause new starts (the running job continues)")
            Button { store.pinned.toggle() } label: {
                Image(systemName: store.pinned ? "pin.fill" : "pin").foregroundStyle(store.pinned ? Color.accentColor : .secondary)
            }
            .help(store.pinned ? "Pinned: stays open. Click to unpin." : "Pin: keep open while you work elsewhere")
        }
        .buttonStyle(.borderless).font(.system(size: 11))
        .padding(.horizontal, 10).frame(height: 28)
    }

    @ViewBuilder var content: some View {
        VStack(alignment: .leading, spacing: 0) {
            if let r = store.running { JobRow(job: r, store: store) }
            queue
            if store.running == nil && store.pendingJobs.isEmpty && store.connected {
                Text("No GPU work queued").font(.system(size: 12)).foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity).padding(.vertical, 8)
            }
            let recent = store.recent + store.older
            if !recent.isEmpty {
                Divider().padding(.horizontal, 10).padding(.vertical, 3)
                ForEach(showAllRecent ? recent : Array(recent.prefix(3))) { JobRow(job: $0, store: store) }
                if recent.count > 3 || store.olderNext != nil {
                    Button(showAllRecent ? "Show less" : "Show \(recent.count > 3 ? "\(recent.count - 3) " : "")more") {
                        if !showAllRecent && store.older.isEmpty { store.loadOlder() }
                        showAllRecent.toggle()
                    }
                    .buttonStyle(.link).font(.system(size: 11)).padding(.horizontal, 12).frame(height: 18)
                }
            }
        }
        .padding(.vertical, 2)
    }

    /// Queue rows. Dragging is tracked by the panel itself (SwiftUI drag gestures and system drag-and-drop
    /// don't receive drags reliably in a non-activating panel); the views only draw the feedback.
    var queue: some View {
        let jobs = store.pendingJobs
        return VStack(spacing: 0) {
            ForEach(jobs) { j in
                JobRow(job: j, store: store)
                    .background(GeometryReader { Color.clear.preference(key: RowFrames.self, value: [j.id: $0.frame(in: .global)]) })
                    .opacity(store.dragID == j.id ? 0.5 : 1)
                    .offset(y: store.dragID == j.id ? store.dragY - store.dragStartY : 0)
                    .zIndex(store.dragID == j.id ? 1 : 0)
            }
        }
        .onPreferenceChange(RowFrames.self) { store.rowFrames = $0 }
        .overlay(alignment: .topLeading) {
            if store.dragID != nil, let top = jobs.first.flatMap({ store.rowFrames[$0.id] }) {
                let i = store.dropIndex
                let y = (i < jobs.count ? store.rowFrames[jobs[i].id]?.minY : store.rowFrames[jobs.last!.id]?.maxY) ?? top.minY
                Capsule().fill(Color.accentColor).frame(height: 2).padding(.horizontal, 10).offset(y: y - top.minY - 1)
                    .allowsHitTesting(false)
            }
        }
    }

    var footer: some View {
        HStack(spacing: 6) {
            Menu {
                Picker("Project", selection: $store.project) {
                    Text("All Projects").tag(String?.none)
                    ForEach(store.projects, id: \.self) { Text($0).tag(String?.some($0)) }
                }
                Picker("Agent", selection: $store.agent) {
                    Text("All Agents").tag(String?.none)
                    ForEach(store.agents, id: \.self) { Text($0).tag(String?.some($0)) }
                }
            } label: {
                Label(filterLabel, systemImage: "line.3.horizontal.decrease")
            }
            .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize()
            Spacer()
            Menu {
                Button("Clear Finished…") { store.clearFinished() }
                    .disabled(store.recent.isEmpty && store.older.isEmpty)
                Button("Clear Queue…") { store.clearQueue() }
                    .disabled(store.pendingJobs.isEmpty)
                if !store.attention.isEmpty {
                    Button("Dismiss All Failures") { store.dismissAll() }
                }
                Button("Open Data Folder") { NSWorkspace.shared.open(URL(fileURLWithPath: ServiceInfo.shared.home)) }
                Divider()
                Button("Quit Anvil") { NSApp.terminate(nil) }
            } label: { Image(systemName: "ellipsis") }
                .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize()
        }
        .font(.system(size: 11)).foregroundStyle(.secondary)
        .padding(.horizontal, 10).frame(height: 24)
    }

    var filterLabel: String {
        [store.project, store.agent].compactMap { $0 }.joined(separator: " · ").nilIfEmpty ?? "All work"
    }

    @ViewBuilder var notices: some View {
        if !store.connected { notice("Service not reachable — reconnecting…", .red) }
        if let m = store.message { notice(m, store.messageIsError ? .red : .blue) }
        if let c = store.confirm {
            HStack(spacing: 6) {
                Text(c.text).font(.system(size: 11)).fixedSize(horizontal: false, vertical: true)
                Spacer()
                Button("Cancel") { store.confirm = nil }.keyboardShortcut(.cancelAction)
                Button(c.button, role: .destructive) { store.confirm = nil; c.action() }
            }
            .controlSize(.small).font(.system(size: 11))
            .padding(.horizontal, 12).padding(.vertical, 4)
        }
        if store.gpu == "attention" && !store.attention.isEmpty { attentionNotice }
        if !store.outside.isEmpty {
            notice("Outside GPU work (\(store.outside.count)): " + store.outside.prefix(3)
                .map { o in
                    let name = (o["cmd"] as? String ?? "").split(separator: " ").first.map { ($0 as NSString).lastPathComponent } ?? ""
                    return name + ((o["pid"] as? NSNumber).map { " (pid \($0))" } ?? "")
                }
                .joined(separator: ", "), .orange)
        }
    }

    /// What "Needs Attention" is about, across all projects, and how to clear it.
    var attentionNotice: some View {
        let items = store.attention
        let ackable = items.filter { ($0["status"] as? String).map(failed.contains) == true }
        return VStack(alignment: .leading, spacing: 2) {
            ForEach(Array(items.prefix(3).enumerated()), id: \.offset) { _, a in
                let id = a["id"] as? String ?? ""
                Text("\(id) \(a["name"] as? String ?? "") · \(a["agent"] as? String ?? ""): \(a["why"] as? String ?? "")")
                    .font(.system(size: 11)).foregroundStyle(.red).lineLimit(1).truncationMode(.middle)
                    .help("To clear: \(a["clear"] as? String ?? "")")
            }
            HStack(spacing: 6) {
                Text(items.count > 3 ? "and \(items.count - 3) more" : "Needs attention")
                    .font(.system(size: 10)).foregroundStyle(.secondary)
                Spacer()
                if !ackable.isEmpty {
                    Button("Dismiss \(ackable.count == 1 ? "failure" : "\(ackable.count) failures")") { store.dismissAll() }
                        .help("Acknowledge: the jobs stay in the list; held jobs still need Release or Cancel")
                }
            }
            .controlSize(.small).font(.system(size: 11))
        }
        .padding(.horizontal, 12).padding(.vertical, 4)
    }

    func notice(_ t: String, _ c: Color) -> some View {
        Text(t).font(.system(size: 11)).lineLimit(3).foregroundStyle(c).textSelection(.enabled)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 12).padding(.vertical, 3)
    }
}

extension String { var nilIfEmpty: String? { isEmpty ? nil : self } }

struct StateBadge: View {
    let gpu: String, paused: Bool
    var body: some View {
        let (label, color): (String, Color) = switch gpu {
        case "running": ("Running", .green)
        case "paused": ("Paused", .orange)
        case "attention": ("Needs Attention", .red)
        case "offline": ("Offline", .secondary)
        default: ("Idle", .secondary)
        }
        HStack(spacing: 5) {
            Circle().fill(color).frame(width: 6, height: 6)
            Text("GPU " + label).font(.system(size: 12, weight: .semibold))
            if paused && gpu != "paused" { Text("· starts paused").font(.system(size: 11)).foregroundStyle(.orange) }
        }
    }
}

/// One line per job, like a menu item: hover highlight, × to remove, right-click for every action,
/// click to expand details inline.
struct JobRow: View {
    let job: Job
    @ObservedObject var store: Store
    @State private var hover = false
    @State private var reason = ""
    @State private var askReason = false
    @State private var limit = ""

    var open: Bool { store.expanded == job.id }
    var final: Bool { !job.pending && job.status != "running" }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: 6) {
                leading.frame(width: 14)
                (Text(job.name).font(.system(size: 12)) + Text("  " + meta).font(.system(size: 11)).foregroundColor(.secondary))
                    .lineLimit(1).truncationMode(.tail)
                Spacer(minLength: 4)
                if hover && !open { removeButton } else { trailing.font(.system(size: 11).monospacedDigit()).foregroundStyle(.secondary) }
            }
            .frame(height: rowHeight)
            .padding(.horizontal, 6)
            .contentShape(Rectangle())
            .onTapGesture { store.toggle(job.id) }
            if job.status == "running" { timeBar }
            if open { detail.padding(.leading, 26).padding(.trailing, 6).padding(.bottom, 6) }
        }
        .background(RoundedRectangle(cornerRadius: 5).fill(open ? Color.primary.opacity(0.06)
                                                           : hover ? Color.primary.opacity(0.09) : .clear))
        .padding(.horizontal, 4)
        .onHover { hover = $0 }
        .contextMenu { menu }
        .help(tooltip)
    }

    /// Everything that tells jobs apart: shown on hover (the row's grey text switches to it) and as a tooltip.
    var tooltip: String {
        var l = ["\(job.id) · \(job.status) · \(job.agent) · \(job.project)"]
        if let w = job.s("why") { l.append(w) }
        if let r = job.d["run_next"] as? [String: Any] { l.append("run next: \(r["reason"] as? String ?? "") (\(r["by"] as? String ?? ""))") }
        l.append("est. \(job.n("est_s").map(fmt) ?? "unknown") · limit \(job.n("timeout_s").map(fmt) ?? "none")")
        if let n = job.s("note") { l.append(n) }
        return l.joined(separator: "\n")
    }

    var meta: String {
        var p: [String] = []
        if hover && !open { p += [job.id, job.agent] }
        if job.status == "running", let m = job.progress?["message"] as? String { p.append(m) }
        p.append(job.project)
        if job.status == "held" { p.append(job.s("why") ?? "held") }
        if job.d["run_next"] != nil { p.append("next") }
        if job.d["run_last"] != nil { p.append("last") }
        if job.d["acked"] as? Bool == false { p.append("unacknowledged") }
        if final && job.status != "succeeded" { p.append(job.s("reason") ?? job.status) }
        if let sub = job.s("sub") { p.append(sub) }
        return p.joined(separator: " · ")
    }

    /// Running time against the agent's estimate (full at the estimate, orange past it); without an estimate,
    /// the job's own reported progress fraction.
    @ViewBuilder var timeBar: some View {
        if let est = job.n("est_s"), est > 0 {
            TimelineView(.periodic(from: .now, by: 1)) { ctx in
                let el = (job.n("elapsed_s") ?? 0) + ctx.date.timeIntervalSince(store.fetchedAt)
                ProgressView(value: min(1, max(0, el / est))).controlSize(.mini)
                    .tint(el > est ? .orange : .accentColor)
                    .help(el > est ? "Past the agent's estimate by \(fmt(el - est))" : "Running time / agent's estimate")
            }
            .padding(.horizontal, 26).padding(.bottom, 3)
        } else if let f = job.progress?["fraction"] as? Double {
            ProgressView(value: min(1, max(0, f))).controlSize(.mini).padding(.horizontal, 26).padding(.bottom, 3)
                .help("Progress reported by the job")
        }
    }

    @ViewBuilder var leading: some View {
        if job.status == "running" {
            ProgressView().controlSize(.mini)
        } else if job.status == "queued" {
            Text(job.n("position").map { "\(Int($0))" } ?? "·")
                .font(.system(size: 10, weight: .medium).monospacedDigit()).foregroundStyle(.secondary)
        } else {
            Image(systemName: statusSymbol(job.status)).font(.system(size: 11)).foregroundStyle(statusColor(job.status))
        }
    }

    @ViewBuilder var trailing: some View {
        if job.status == "running" {
            TimelineView(.periodic(from: .now, by: 1)) { ctx in
                Text(fmt((job.n("elapsed_s") ?? 0) + ctx.date.timeIntervalSince(store.fetchedAt))
                     + (job.n("est_s").map { " / ~" + fmt($0) } ?? ""))
            }
            .help("Running time / agent's estimate · hard limit " + (job.n("timeout_s").map(fmt) ?? "none"))
        } else if job.status == "queued", let w = job.n("approx_wait_s") {
            Text("~" + fmt(w)).help("Approximate wait, from agent estimates")
        } else if final {
            Text(fmt(job.n("elapsed_s")))
        }
    }

    var removeButton: some View {
        Button {
            if job.status == "running" { store.stop(job) } else { store.control(job.id, "remove") }
        } label: {
            Image(systemName: job.status == "running" ? "stop.circle.fill" : "xmark.circle.fill").foregroundStyle(.secondary)
        }
        .buttonStyle(.borderless).font(.system(size: 12))
        .help(job.status == "running" ? "Stop (asks first)" : job.pending ? "Cancel and remove (its agent is told)"
              : "Remove from list (deletes its logs)")
    }

    @ViewBuilder var menu: some View {
        let s = job.status
        if s == "queued" {
            Button("Run Next") { store.control(job.id, "run_next", ["reason": "moved to front in the Anvil panel"]) }
            Button("Hold") { store.control(job.id, "hold") }
        }
        if s == "held" {
            let drift = (job.s("why") ?? "").hasPrefix("drift")
            Button(drift ? "Release (use current source)" : "Release") {
                store.control(job.id, "release", drift ? ["accept_drift": true] : [:])
            }
        }
        if job.d["run_next"] != nil || job.d["run_last"] != nil {
            Button("Unpin (back to fair order)") { store.control(job.id, "unpin") }
        }
        if final {
            Button("Rerun") { store.control(job.id, "rerun") }
            if job.d["acked"] as? Bool == false { Button("Dismiss Failure") { store.control(job.id, "ack") } }
            Button("Show Results") {
                let dir = ServiceInfo.shared.home + "/results/" + job.id
                if FileManager.default.fileExists(atPath: dir) { NSWorkspace.shared.open(URL(fileURLWithPath: dir)) }
                else { store.flash("\(job.id) wrote no results", error: true) }
            }
        }
        if job.n("attempt") != nil || job.status == "running" || final {
            Button("Open Log") { openLog() }
        }
        Button("Copy Job ID") {
            NSPasteboard.general.clearContents(); NSPasteboard.general.setString(job.id, forType: .string)
        }
        Divider()
        if s == "running" { Button("Stop…", role: .destructive) { store.stop(job) } }
        else { Button(job.pending ? "Cancel and Remove" : "Remove", role: .destructive) { store.control(job.id, "remove") } }
    }

    func openLog() {
        let n = job.n("attempt").map { Int($0) } ?? 1
        let f = ServiceInfo.shared.home + "/logs/\(job.id)/\(n).log"
        if FileManager.default.fileExists(atPath: f) { NSWorkspace.shared.open(URL(fileURLWithPath: f)) }
        else { store.flash("No log for \(job.id) (removed, or it never started)", error: true) }
    }

    @ViewBuilder var detail: some View {
        let d = (store.detail?["id"] as? String) == job.id ? store.detail : nil
        VStack(alignment: .leading, spacing: 4) {
            if let n = job.s("note") { Text(n).font(.system(size: 11)).fixedSize(horizontal: false, vertical: true) }
            line("\(job.id) · \(job.agent) · est. \(job.n("est_s").map(fmt) ?? "unknown") · limit \((job.n("timeout_s").map(fmt) ?? "none"))")
            if job.pending, let w = job.s("why") { line(w) }
            if let d {
                line((d["cmd"] as? [String] ?? []).map { ($0 as NSString).lastPathComponent }.joined(separator: " "))
                if let r = d["results"] as? [String], !r.isEmpty { line("results: " + r.prefix(3).joined(separator: ", ")) }
            }
            if !store.log.isEmpty {
                Text(store.log.replacingOccurrences(of: "\r\n", with: "\n").split(separator: "\n")
                        .map { $0.split(separator: "\r", omittingEmptySubsequences: false).last.map(String.init) ?? "" }  // progress bars redraw with \r
                        .suffix(4).joined(separator: "\n"))
                    .font(.system(size: 10, design: .monospaced)).foregroundStyle(.secondary)
                    .lineLimit(4).textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading).padding(4)
                    .background(Color.primary.opacity(0.05), in: RoundedRectangle(cornerRadius: 4))
            }
            if askReason {
                HStack(spacing: 4) {
                    TextField("Reason, visible to everyone", text: $reason).textFieldStyle(.roundedBorder).onSubmit(runNext)
                    Button("Run Next", action: runNext).disabled(reason.isEmpty)
                }
            }
            actions
        }
        .controlSize(.small)
    }

    func line(_ t: String) -> some View {
        Text(t).font(.system(size: 11)).foregroundStyle(.secondary).lineLimit(1).truncationMode(.middle).textSelection(.enabled)
    }

    func runNext() {
        guard !reason.isEmpty else { return }
        store.control(job.id, "run_next", ["reason": reason]); reason = ""; askReason = false
    }

    @ViewBuilder var actions: some View {
        let s = job.status
        HStack(spacing: 4) {
            if s == "queued" {
                Button("Hold") { store.control(job.id, "hold") }
                Button("Run Next…") { askReason.toggle() }
            }
            if s == "held" {
                let drift = (job.s("why") ?? "").hasPrefix("drift")
                Button(drift ? "Release (current source)" : "Release") {
                    store.control(job.id, "release", drift ? ["accept_drift": true] : [:])
                }
            }
            if job.pending || s == "running" {
                TextField("none", text: $limit).frame(width: 54).textFieldStyle(.roundedBorder)
                    .onAppear { limit = job.n("timeout_s").map { limitText($0) } ?? "" }
                    .onSubmit {
                        if let v = parseLimit(limit) { store.control(job.id, "timeout", ["seconds": v]) }
                        else { store.flash("Time limit: seconds (90), 30m, 2h, or empty for none", error: true) }
                    }
                    .help("Hard time limit: 90 (seconds), 30m, 2h, or empty for none; press Return to apply")
            }
            if final {
                Button("Rerun") { store.control(job.id, "rerun") }
                if failed.contains(s) && (store.detail?["acked"] as? Bool) == false {
                    Button("Dismiss") { store.control(job.id, "ack") }.help("Acknowledge this failure")
                }
            }
            Spacer()
            if job.d["run_next"] != nil || job.d["run_last"] != nil { Button("Unpin") { store.control(job.id, "unpin") } }
            if s == "running" { Button("Stop…", role: .destructive) { store.stop(job) } }
            else { Button(job.pending ? "Cancel" : "Remove", role: .destructive) { store.control(job.id, "remove") } }
        }
        .font(.system(size: 11))
    }
}

// MARK: - Panel window

/// Accepts the first click even though a menu bar app is never the active app (otherwise the first click
/// on a button or row only focuses the panel).
final class FirstMouseHostingView<V: View>: NSHostingView<V> {
    override func acceptsFirstMouse(for event: NSEvent?) -> Bool { true }
}

/// Floating, non-activating, resizable panel anchored under the menu bar icon.
final class Panel: NSPanel, NSWindowDelegate {
    var pinned = false
    var onClose: (() -> Void)?
    var onResized: ((NSSize, Bool) -> Void)?  // (new size, whether the height was changed by hand)
    weak var store: Store?
    private(set) var closedAt = Date.distantPast
    private var pressed: (id: String, y: CGFloat)?

    init(content: NSView) {
        super.init(contentRect: NSRect(x: 0, y: 0, width: 300, height: 200),
                   styleMask: [.nonactivatingPanel, .resizable, .fullSizeContentView], backing: .buffered, defer: false)
        delegate = self
        isFloatingPanel = true
        level = .statusBar
        collectionBehavior = [.auxiliary, .stationary, .moveToActiveSpace, .fullScreenAuxiliary]
        backgroundColor = .clear
        isOpaque = false
        hasShadow = true
        hidesOnDeactivate = false
        animationBehavior = .utilityWindow
        isMovableByWindowBackground = false  // drags reorder rows; the panel stays under its icon
        minSize = NSSize(width: 240, height: 120)

        let bg: NSView
        if #available(macOS 26.0, *) {
            let g = NSGlassEffectView()
            g.cornerRadius = 10
            g.contentView = content
            bg = g
        } else {
            let v = NSVisualEffectView()
            v.material = .popover
            v.blendingMode = .behindWindow
            v.state = .active
            v.wantsLayer = true
            v.layer?.cornerRadius = 10
            v.layer?.cornerCurve = .continuous
            v.layer?.masksToBounds = true
            content.translatesAutoresizingMaskIntoConstraints = false
            v.addSubview(content)
            NSLayoutConstraint.activate([
                content.leadingAnchor.constraint(equalTo: v.leadingAnchor),
                content.trailingAnchor.constraint(equalTo: v.trailingAnchor),
                content.topAnchor.constraint(equalTo: v.topAnchor),
                content.bottomAnchor.constraint(equalTo: v.bottomAnchor)])
            bg = v
        }
        contentView = bg
    }

    override var canBecomeKey: Bool { true }

    /// Reorder by dragging a queue row. Events still reach SwiftUI; once the pointer moves, the row's tap no
    /// longer fires, so a drag never also expands the row.
    override func sendEvent(_ e: NSEvent) {
        if let store {
            let p = CGPoint(x: e.locationInWindow.x, y: frame.height - e.locationInWindow.y)
            switch e.type {
            case .leftMouseDown:
                pressed = store.row(at: p).map { ($0, p.y) }
            case .leftMouseDragged:
                if let pr = pressed {
                    if store.dragID == nil && abs(p.y - pr.y) > 4 { store.dragID = pr.id; store.dragStartY = pr.y }
                    if store.dragID != nil { store.dragY = p.y }
                }
            case .leftMouseUp:
                if let id = store.dragID { store.move(id, to: store.dropIndex) }
                store.dragID = nil; pressed = nil
            default: break
            }
        }
        super.sendEvent(e)
    }

    override func resignKey() {
        super.resignKey()
        if !pinned { close() }
    }

    override func close() {
        super.close()
        store?.confirm = nil
        closedAt = Date()
        onClose?()
    }

    override func cancelOperation(_ sender: Any?) { close() }  // Esc closes, pinned or not

    private var resizeStart: NSSize?
    func windowWillStartLiveResize(_ notification: Notification) { resizeStart = frame.size }
    func windowDidEndLiveResize(_ notification: Notification) {
        onResized?(frame.size, abs(frame.height - (resizeStart?.height ?? frame.height)) > 1)
    }
}

// MARK: - Menu bar icon

/// A small anvil drawn as a template image: outline when idle, filled while running, with a badge for
/// paused (pause bars) and needs attention (exclamation mark).
enum AnvilIcon {
    static func image(_ state: String) -> NSImage {
        let img = NSImage(size: NSSize(width: 18, height: 18), flipped: false) { _ in
            let p = NSBezierPath()
            p.move(to: NSPoint(x: 1, y: 13.2))         // horn tip
            p.line(to: NSPoint(x: 4.8, y: 14.5))
            p.line(to: NSPoint(x: 17, y: 14.5))        // working face
            p.line(to: NSPoint(x: 17, y: 11.5))
            p.curve(to: NSPoint(x: 12, y: 8.8), controlPoint1: NSPoint(x: 14.5, y: 11.2), controlPoint2: NSPoint(x: 12.4, y: 10.4))
            p.line(to: NSPoint(x: 12, y: 6.6))         // waist
            p.line(to: NSPoint(x: 15, y: 4.3))
            p.line(to: NSPoint(x: 15, y: 2.5))         // base
            p.line(to: NSPoint(x: 5, y: 2.5))
            p.line(to: NSPoint(x: 5, y: 4.3))
            p.line(to: NSPoint(x: 8, y: 6.6))
            p.line(to: NSPoint(x: 8, y: 8.8))
            p.curve(to: NSPoint(x: 4.6, y: 11.6), controlPoint1: NSPoint(x: 7.6, y: 10.4), controlPoint2: NSPoint(x: 6.2, y: 11.3))
            p.close()
            p.lineJoinStyle = .round
            NSColor.black.set()
            if state == "idle" || state == "paused" {
                p.lineWidth = 1.3
                p.stroke()
            } else {
                p.fill()
            }
            guard state == "paused" || state == "attention" else { return true }
            // badge: punch a hole bottom-right, then draw the mark inside it
            let hole = NSBezierPath(ovalIn: NSRect(x: 10.5, y: 0, width: 7.5, height: 7.5))
            NSGraphicsContext.current?.compositingOperation = .clear
            hole.fill()
            NSGraphicsContext.current?.compositingOperation = .sourceOver
            if state == "paused" {
                NSBezierPath(roundedRect: NSRect(x: 12.4, y: 1.4, width: 1.4, height: 4.6), xRadius: 0.5, yRadius: 0.5).fill()
                NSBezierPath(roundedRect: NSRect(x: 14.8, y: 1.4, width: 1.4, height: 4.6), xRadius: 0.5, yRadius: 0.5).fill()
            } else {
                NSBezierPath(ovalIn: NSRect(x: 11.2, y: 0.7, width: 6.1, height: 6.1)).fill()
                NSGraphicsContext.current?.compositingOperation = .clear
                NSBezierPath(roundedRect: NSRect(x: 13.65, y: 2.9, width: 1.2, height: 3), xRadius: 0.4, yRadius: 0.4).fill()
                NSBezierPath(ovalIn: NSRect(x: 13.6, y: 1.5, width: 1.3, height: 1.1)).fill()
            }
            return true
        }
        img.isTemplate = true
        return img
    }
}

// MARK: - App

@main
@MainActor
struct AnvilApp {
    static func main() {
        let app = NSApplication.shared
        let delegate = AppDelegate()
        app.delegate = delegate
        app.setActivationPolicy(.accessory)
        app.run()
    }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    var item: NSStatusItem!
    var panel: Panel!
    let store = Store()
    var contentHeight: CGFloat = 200
    /// User-chosen size (drag the panel edges): width, and the height the panel may grow to.
    var width: CGFloat { CGFloat(UserDefaults.standard.object(forKey: "panelWidth") as? Double ?? 300) }
    var heightCap: CGFloat { CGFloat(UserDefaults.standard.object(forKey: "panelHeight") as? Double ?? 460) }

    func applicationDidFinishLaunching(_ n: Notification) {
        item = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        item.button?.target = self
        item.button?.action = #selector(toggle)
        let host = FirstMouseHostingView(rootView: PanelView(store: store) { [weak self] h in
            self?.contentHeight = h
            self?.fit()
        })
        host.sizingOptions = []
        panel = Panel(content: host)
        panel.onClose = { [weak self] in self?.item.button?.isHighlighted = false }
        panel.onResized = { [weak self] size, heightChanged in
            UserDefaults.standard.set(Double(size.width), forKey: "panelWidth")
            // widening a short panel must not cap its height at that moment's content height
            if heightChanged { UserDefaults.standard.set(Double(size.height), forKey: "panelHeight") }
            self?.fit()
        }
        store.onPin = { [weak self] in self?.panel.pinned = $0 }
        panel.store = store
        if let i = CommandLine.arguments.firstIndex(of: "--snapshot"), i + 1 < CommandLine.arguments.count {
            snapshot(to: CommandLine.arguments[i + 1]); return
        }
        if let i = CommandLine.arguments.firstIndex(of: "--icons"), i + 1 < CommandLine.arguments.count {
            renderIcons(to: CommandLine.arguments[i + 1]); NSApp.terminate(nil); return
        }
        store.onChange = { [weak self] in self?.updateIcon() }
        updateIcon()
        store.start()
        if CommandLine.arguments.contains("--selftest") { SelfTest(app: self).run() }
    }

    var screen: NSScreen? { item.button?.window?.screen ?? NSScreen.main }
    var maxHeight: CGFloat { min(heightCap, (screen?.visibleFrame.height ?? 800) - 16) }

    /// Fit the content up to the user's chosen height (capped to the screen), top edge just under the icon.
    func fit() {
        // Never move the window under the pointer mid-click or mid-drag; refit when the button is released.
        if panel.inLiveResize || NSEvent.pressedMouseButtons != 0 {
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) { [weak self] in self?.fit() }
            return
        }
        let w = width, h = max(min(contentHeight, maxHeight), 90)
        guard let b = item.button, let win = b.window, let vf = screen?.visibleFrame else {
            panel.setContentSize(NSSize(width: w, height: h)); return
        }
        let r = win.convertToScreen(b.convert(b.bounds, to: nil))
        let x = min(max(r.midX - w / 2, vf.minX + 8), vf.maxX - w - 8)
        let top = min(r.minY - 4, vf.maxY)
        panel.setFrame(NSRect(x: x, y: top - h, width: w, height: h), display: true)
    }

    @objc func toggle() {
        if panel.isVisible { panel.close(); return }
        // Clicking the icon while the panel is open first makes it lose focus (which closes it); don't reopen.
        if Date().timeIntervalSince(panel.closedAt) < 0.3 { return }
        store.refresh()
        fit()
        panel.orderFrontRegardless()
        panel.makeKey()
        item.button?.isHighlighted = true
    }

    /// Render the panel offscreen to a PNG (for checking the UI without clicking the menu bar).
    func snapshot(to path: String) {
        store.refresh()
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) { [self] in
            if CommandLine.arguments.contains("--expand"), let id = store.running?.id ?? store.queue.first?.id { store.toggle(id) }
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) { [self] in
                panel.appearance = NSAppearance(named: CommandLine.arguments.contains("--dark") ? .darkAqua : .aqua)
                panel.setFrame(NSRect(x: 200, y: 200, width: width, height: max(min(contentHeight, maxHeight), 90)),
                               display: true)
                panel.orderFrontRegardless()
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) { [self] in
                    let v = panel.contentView!
                    let rep = v.bitmapImageRepForCachingDisplay(in: v.bounds)!
                    v.cacheDisplay(in: v.bounds, to: rep)
                    try? rep.representation(using: .png, properties: [:])?.write(to: URL(fileURLWithPath: path))
                    NSApp.terminate(nil)
                }
            }
        }
    }

    func renderIcons(to path: String) {
        let states = ["idle", "running", "paused", "attention"]
        let out = NSImage(size: NSSize(width: 18 * 4 + 30, height: 18), flipped: false) { _ in
            for (i, st) in states.enumerated() {
                AnvilIcon.image(st).draw(in: NSRect(x: CGFloat(i) * 28, y: 0, width: 18, height: 18))
            }
            return true
        }
        let rep = NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: 102 * 8, pixelsHigh: 18 * 8, bitsPerSample: 8,
                                   samplesPerPixel: 4, hasAlpha: true, isPlanar: false, colorSpaceName: .deviceRGB,
                                   bytesPerRow: 0, bitsPerPixel: 0)!
        rep.size = out.size
        NSGraphicsContext.saveGraphicsState()
        NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: rep)
        NSColor.white.setFill(); NSRect(origin: .zero, size: out.size).fill()
        out.draw(in: NSRect(origin: .zero, size: out.size))
        NSGraphicsContext.restoreGraphicsState()
        try? rep.representation(using: .png, properties: [:])?.write(to: URL(fileURLWithPath: path))
    }

    func updateIcon() {
        guard let b = item?.button else { return }
        let (symbol, tip): (String, String) = switch store.connected ? store.gpu : "offline" {
        case "running": ("running", "Anvil: running")
        case "paused": ("paused", "Anvil: new starts paused")
        case "attention": ("attention", "Anvil: needs attention")
        case "offline": ("idle", "Anvil: service not reachable")
        default: ("idle", "Anvil: idle")
        }
        let img = AnvilIcon.image(symbol)
        img.accessibilityDescription = tip
        b.image = img
        b.toolTip = tip
        b.appearsDisabled = !store.connected
    }
}

// MARK: - Self-test

/// `Anvil --selftest`: drives the real panel and the same Store methods the buttons call, against the running
/// service, using throwaway jobs in project "anvil-selftest". Prints PASS/FAIL per check and exits non-zero on failure.
@MainActor
final class SelfTest {
    let app: AppDelegate
    var failures = 0
    let other = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 10, height: 10), styleMask: [.titled], backing: .buffered, defer: false)
    init(app: AppDelegate) { self.app = app }

    func check(_ name: String, _ ok: Bool) {
        print((ok ? "PASS " : "FAIL ") + name); fflush(stdout)
        if !ok { failures += 1 }
    }

    func status(_ id: String) -> [String: Any] { (try? call("GET", "/api/jobs/\(id)") as? [String: Any]) ?? [:] }

    /// Real mouse events delivered to the panel (hit-testing, SwiftUI gestures and buttons all run).
    func mouse(_ type: NSEvent.EventType, _ top: NSPoint) {
        let p = app.panel!
        let loc = NSPoint(x: top.x, y: p.frame.height - top.y)  // from top-left to window coordinates
        if let e = NSEvent.mouseEvent(with: type, location: loc, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                                      windowNumber: p.windowNumber, context: nil, eventNumber: 0, clickCount: 1, pressure: 1) {
            NSApp.postEvent(e, atStart: false)  // through the real event queue, as hardware events arrive
        }
    }

    func click(_ top: NSPoint) async {
        mouse(.leftMouseDown, top); await wait(0.05); mouse(.leftMouseUp, top); await wait(0.3)
    }

    func drag(_ from: NSPoint, _ to: NSPoint) async {
        mouse(.leftMouseDown, from)
        for i in 1...12 {
            await wait(0.02)
            mouse(.leftMouseDragged, NSPoint(x: from.x, y: from.y + (to.y - from.y) * CGFloat(i) / 12))
        }
        await wait(0.1); mouse(.leftMouseUp, to); await wait(0.5)
    }

    func wait(_ s: Double) async { try? await Task.sleep(nanoseconds: UInt64(s * 1e9)) }

    func until(_ s: Double = 4, _ cond: () -> Bool) async -> Bool {
        let end = Date().addingTimeInterval(s)
        while Date() < end { if cond() { return true }; await wait(0.1) }
        return cond()
    }

    /// Stop and remove every job in the self-test project.
    func cleanup() async {
        func active() -> [[String: Any]] {
            (try? call("GET", "/api/jobs?project=anvil-selftest&status=queued,held,running") as? [String: Any])?["jobs"] as? [[String: Any]] ?? []
        }
        for j in active() {
            let op = j["status"] as? String == "running" ? "cancel" : "remove"
            _ = try? call("POST", "/api/jobs/\(j["id"] as? String ?? "")/\(op)", ["actor": "selftest"])
        }
        _ = await until(12) { active().isEmpty }
    }

    func submit(_ name: String, _ secs: String, agent: String = "selftest") -> String {
        let r = try? call("POST", "/api/submit", ["project": "anvil-selftest", "agent": agent, "name": name,
            "cmd": ["/bin/sleep", secs], "cwd": "/tmp", "check_drift": false, "actor": "selftest"]) as? [String: Any]
        return r?["id"] as? String ?? ""
    }

    func run() {
        Task { @MainActor in
            await wait(1.5)
            let p = app.panel!
            // Panel open / close / pin
            app.toggle()
            check("opens on icon click", p.isVisible)
            if let b = app.item.button, let w = b.window {
                let icon = w.convertToScreen(b.convert(b.bounds, to: nil))
                check("anchored under the icon", abs(p.frame.maxY - (icon.minY - 4)) < 2 && p.frame.minX <= icon.midX && p.frame.maxX >= icon.midX)
                check("fits on screen", NSScreen.main.map { $0.visibleFrame.contains(p.frame) } ?? false)
            }
            other.makeKeyAndOrderFront(nil)
            app.toggle()  // a click on the icon arrives right after the panel lost focus
            check("closes when it loses focus (unpinned)", !p.isVisible)
            check("icon click while open closes it, without reopening", !p.isVisible)
            await wait(0.4)
            app.toggle(); app.store.pinned = true
            other.makeKeyAndOrderFront(nil); await wait(0.3)
            check("stays open when pinned and focus moves away", p.isVisible)
            app.toggle(); check("icon click closes a pinned panel", !p.isVisible)
            app.store.pinned = false; other.orderOut(nil)

            await cleanup()  // anything a previous run left behind
            let blocker = submit("blocker", "30"), a = submit("first", "5"), b = submit("second", "5")
            check("jobs accepted", !blocker.isEmpty && !a.isEmpty && !b.isEmpty)
            _ = await until { self.status(blocker)["status"] as? String == "running" }
            let st = app.store
            // Real clicks and drags inside the panel
            _ = await until { st.pendingJobs.count >= 2 }
            NSApp.deactivate(); await wait(0.3)  // like real use: the menu bar app is not the active app
            app.toggle(); await wait(0.6)
            let w = p.frame.width
            await click(NSPoint(x: w - 18, y: 14))  // pin button, top-right of the header
            check("pin button click pins", st.pinned && p.pinned)
            other.makeKeyAndOrderFront(nil); await wait(0.3)
            check("pinned panel survives clicking elsewhere", p.isVisible)
            other.orderOut(nil); NSApp.deactivate(); await wait(0.3)
            // the rows' real on-screen frames, as laid out by SwiftUI
            let rowA = (st.rowFrames[a]?.minY ?? 0) + 8, rowB = st.rowFrames[b]?.midY ?? 0
            check("queue shows a above b", rowA < rowB && rowA > 0)
            await drag(NSPoint(x: 120, y: rowB), NSPoint(x: 120, y: rowA + 3))
            check("mouse drag reorders rows", await until {
                (self.status(b)["position"] as? Int ?? 99) < (self.status(a)["position"] as? Int ?? 0) })
            st.move(a, to: 0 + (st.pendingJobs.firstIndex { $0.id == b } ?? 0))  // restore a before b
            _ = await until { (self.status(a)["position"] as? Int ?? 99) < (self.status(b)["position"] as? Int ?? 0) }
            await click(NSPoint(x: w - 18, y: 14))
            check("pin button click unpins", !st.pinned && !p.pinned)
            p.close()
            st.control(a, "hold")
            check("hold", await until { self.status(a)["status"] as? String == "held" })
            st.control(a, "release")
            check("release", await until { self.status(a)["status"] as? String == "queued" })
            _ = await until { st.pendingJobs.count >= 2 }
            st.move(b, to: st.pendingJobs.firstIndex { $0.id == a } ?? 0)  // drop b above a
            check("drag reorder within an agent", await until {
                (self.status(b)["position"] as? Int ?? 99) < (self.status(a)["position"] as? Int ?? 0) })
            let c = submit("other-agent", "5", agent: "selftest-2")
            _ = await until { st.pendingJobs.contains { $0.id == c } }
            st.move(c, to: 0)
            check("drag to top runs next", await until { self.status(c)["run_next"] != nil })
            st.control(c, "remove")
            check("remove a queued job", await until { !st.queue.contains { $0.id == c } && self.status(c)["status"] as? String == "canceled" })
            st.control(a, "timeout", ["seconds": 77.0])
            check("set limit", await until { (self.status(a)["timeout_s"] as? Double) == 77 })
            st.control(a, "run_next", ["reason": "selftest"])
            check("run next", await until { self.status(a)["run_next"] != nil })
            st.act("/api/pause")
            check("pause new starts", await until { st.paused })
            st.act("/api/resume")
            check("resume", await until { !st.paused })
            st.control(blocker, "cancel")
            check("stop running job", await until(10) { self.status(blocker)["status"] as? String == "canceled" })
            for id in [a, b] { st.control(id, "cancel") }
            _ = await until(10) { [a, b].allSatisfy { ["canceled", "succeeded"].contains(self.status($0)["status"] as? String ?? "") } }
            st.control(blocker, "rerun")
            let rerun = await until { st.queue.contains { $0.d["rerun_of"] as? String == blocker } }
            check("rerun", rerun)
            for j in st.queue where j.project == "anvil-selftest" { st.control(j.id, "remove") }
            _ = await until(5) { !st.queue.contains { $0.project == "anvil-selftest" } }
            st.control(blocker, "remove")
            check("remove a finished job", await until { !(st.recent + st.older).contains { $0.id == blocker } })
            st.act("/api/clear", ["project": "anvil-selftest"])
            check("clear finished", await until { !st.recent.contains { $0.project == "anvil-selftest" } })
            p.onResized?(NSSize(width: 333, height: 300), true); app.toggle()
            check("resize is remembered", abs(p.frame.width - 333) < 1 && p.frame.height <= 300.5)
            p.onResized?(NSSize(width: 300, height: 460), true); p.close()
            await cleanup()
            st.act("/api/clear", ["project": "anvil-selftest"])
            st.refresh(); await wait(1)
            check("panel shows live queue", st.connected && st.cursor != nil)
            print(failures == 0 ? "ALL PASSED" : "\(failures) FAILED")
            exit(failures == 0 ? 0 : 1)
        }
    }
}
