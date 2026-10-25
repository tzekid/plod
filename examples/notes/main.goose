// A service of notes kept in SQLite, with the pieces a real one needs
// together: a JSON API whose writes commit on a database lane, a search, a
// stream of each note as it commits (Server-Sent Events) on every worker,
// and a webhook for each note, delivered within a budget the route sets.
//
//   goose build -o build/examples/notes examples/notes/main.goose
//   NOTES_DB=notes.db NOTES_WEBHOOK=http://127.0.0.1:8080/hooks/receive build/examples/notes 8080
//   curl -N localhost:8080/events &
//   curl -s -H 'Content-Type: application/json' -d '{"text": "hello"}' localhost:8080/notes
//   curl -s 'localhost:8080/notes?q=hel'
//
// Three lanes, each a thread that owns what blocks:
//
// - db owns the SQLite connection. A note it inserts goes to the hub once
//   its transaction has committed, never before.
// - hub owns the event streams' subscriptions, across the workers. Each
//   stream, when it can take an event, sends the hub its address, and the
//   hub sends it the next note it hasn't had. The hub keeps the last RING
//   notes: a stream more than RING behind is told it lagged, and ends, as a
//   client that can't keep up has to fetch what it missed with a search.
// - hook delivers each note to NOTES_WEBHOOK, within the budget the route
//   gave it: an absolute time, so a slow receiver can't hold the lane past
//   it. The note is committed whatever the webhook does, and the answer says
//   which happened: delivered, timed_out, failed, or skipped.
import std;
import os;
import json;
// v0.5.11 rather than Plod's v0.5.12, whose shim doesn't build on macOS: it
// uses IPV6_DONTFRAG without asking for RFC 3542's API.
import http from "github.com/kaitk/goose-http@v0.5.11";
import tls from "github.com/kaitk/goose-tls@v0.0.6";
import sqlite from "github.com/kaitk/goose-sqlite@v0.0.3";
import http.client;
import plod from "../../";

struct Note { id: i64 = 0, text: u8[..256] = "" }

fn format(out: u8[>..]&, n: const Note&) {
    format(out, "{\"id\":", n.id, ",\"text\":");
    http::json_string(out, n.text);
    out.push('}');
}

// Each Plod lane's jobs.
enum DbJob {
    Create { text: u8[..256] },
    Get { id: i64 },
    Search { q: u8[..64] },
}
enum HookJob { Deliver { note: Note, until: i64 } }

// What a route waits on.
enum Use { None, Db, Hook, Events, Slow }

// A stream's token, and the key of its worker's box, once it has subscribed.
struct St { use: Use = Use.None, token: u64 = 0, worker: u64 = 0 }

struct App { db: plod::Lane<DbJob, Use>, hook: plod::Lane<HookJob, Use>, hub: http::Lane }

// The longest note, in code points and in bytes, and the most body a note's
// JSON may have.
let TEXT_POINTS = 200;
let BODY_MOST = 4096;

// How long a webhook may take, from the route's first call, and how long an
// idle event stream waits before it sends a comment to keep its connection.
let WEBHOOK_MS: i64 = 1000;
let IDLE_MS: i64 = 15000;

// The hub's messages, by tag, and what it sends a stream.
let HUB_SUBSCRIBE: i32 = 1;
let HUB_LEAVE: i32 = 2;
let HUB_PUBLISH: i32 = 3;
struct Subscribe { token: u64 = 0, to: http::MailAddress = http::MailAddress { .. } }
struct Leave { worker: u64 = 0, token: u64 = 0 }
let EVENT_NOTE = 0;
let EVENT_LAGGED = 1;
let EVENT_FULL = 2;
struct Event { kind: i64 = EVENT_NOTE, note: Note = Note { .. } }

// What the hook lane replies: what became of a note's webhook.
let DELIVERED = 0;
let TIMED_OUT = 1;
let FAILED = 2;
let SKIPPED = 3;

struct Outcome { id: i64, text: const u8[:], webhook: const u8[:] }

fn format(out: u8[>..]&, o: const Outcome&) {
    format(out, "{\"id\":", o.id, ",\"text\":");
    http::json_string(out, o.text);
    format(out, ",\"webhook\":\"", o.webhook, "\"}");
}

// What POST /notes takes: {"text": "..."}, 1 to TEXT_POINTS code points.
struct NoteCreate { text: u8[..256] = "" }

fn read_create(r: json::Reader&, scratch: u8[>..]&) -> NoteCreate {
    var text = r.text[..0];
    var has = false;
    json::each_member(r) { key =>
        if key == "text" { text = json::read_string(r, scratch); has = true; }
        else { json::fail(r, "unknown member"); }
    };
    if !has { json::fail(r, "text: required"); }
    let n = plod::code_points(text);
    if has && (n < 1 || n > TEXT_POINTS || text.len > 256) { json::fail(r, "text: length"); }
    if r.error.len > 0 { return NoteCreate { .. }; }
    var c = NoteCreate { .. };
    c.text.append(text);
    return c;
}

// Each stream's number on its worker, which with the worker's box names it
// at the hub.
var next_token: u64 = 0;

fn handle(cx) {
    plod::routes(cx) { cx =>
        plod::get(cx, "/health") { plod::text(cx, 200, "ok"); };
        plod::post(cx, "/notes") {
            plod::body(cx, BODY_MOST);
            let c = plod::json_body(cx) { r, scratch => read_create(r, scratch) };
            plod::ask(cx, cx.app.db, DbJob.Create { text: c.text });
        };
        plod::get(cx, "/notes/:id") { plod::ask(cx, cx.app.db, DbJob.Get { id: plod::param_int(cx, "id") }); };
        plod::get(cx, "/notes") {
            let q, has = plod::query(cx, "q");
            guard q.len <= 64 else { plod::fail(cx, 400, "invalid_q"); }
            var s = DbJob.Search { .. };
            s.q.append(q);
            plod::ask(cx, cx.app.db, s);
        };
        plod::get(cx, "/events") {
            next_token++;
            cx.state.token = next_token;
            plod::sse_begin(cx, Use.Events);
            if cx.out.head { return; }
            plod::own_waits(cx, Use.Events);
            // MORE comes once the client has taken this; the stream then
            // subscribes.
            plod::sse_comment(cx, "subscribed");
        };
        // The webhook's receiver, here for the example's own requests: a
        // note whose text says slow takes 3 s, past the webhook's budget.
        plod::post(cx, "/hooks/receive") {
            let b = plod::body(cx, BODY_MOST);
            if find(b, "slow") >= 0 { plod::pause(cx, 3000, Use.Slow); } else { plod::text(cx, 200, "received"); }
        };
    };
}

fn resume(u: Use.None, cx) { plod::fail(cx, 500, "not_waiting"); }
fn resume(u: Use.Db, cx) { resume(plod::job<DbJob>(cx), cx); }
fn resume(u: Use.Hook, cx) { resume(plod::job<HookJob>(cx), cx); }
fn resume(u: Use.Slow, cx) { if cx.kind == http::TIMEOUT { plod::text(cx, 200, "received, slowly"); } }

// The note is committed: deliver its webhook next, within WEBHOOK_MS, and
// wait for the hook lane a little longer than that, so its answer, not the
// wait's limit, ends the route.
fn resume(u: DbJob.Create, cx) {
    let note = plod::answer<Note>(cx);
    plod::ask(cx, cx.app.hook, HookJob.Deliver { note: note, until: http::net_clock_ms() + WEBHOOK_MS },
              WEBHOOK_MS + 1000);
}

fn resume(u: HookJob.Deliver, cx) {
    let outcome = plod::answer<i64>(cx);
    let names = ["delivered", "timed_out", "failed", "skipped"];
    plod::json(cx, 201, Outcome { id: u.note.id, text: u.note.text, webhook: names[outcome] });
}

fn resume(u: DbJob.Get, cx) { plod::json(cx, 200, plod::answer<Note>(cx)); }

fn resume(u: DbJob.Search, cx) { plod::relay(cx, 200, "application/json"); }

// An event stream: subscribes at each MORE, writes the note the hub sends at
// WAKE, keeps the connection with a comment at an idle TIMEOUT, and leaves
// the hub at CLOSED.
fn resume(u: Use.Events, cx) {
    if cx.kind == http::MORE {
        let to = http::later(cx.out, IDLE_MS);
        cx.state.worker = http::key_of(to.box);
        let sent = http::lane_send_value(cx.app.hub, HUB_SUBSCRIBE, Subscribe { token: cx.state.token, to: to });
        guard sent == http::SENT else {
            cx.out.stream.waiting = false;
            cx.out.stream.rearm = false;
            plod::sse_event(cx, "busy", "");
            plod::end(cx);
            return;
        }
    } else if cx.kind == http::WAKE {
        var ev = Event { .. };
        guard http::mail_value(cx.reply, ev) else { plod::end(cx); return; }
        if ev.kind == EVENT_NOTE {
            plod::sse_json(cx, ev.note, "note");
        } else {
            plod::sse_event(cx, if ev.kind == EVENT_LAGGED { "lagged" } else { "busy" }, "");
            plod::end(cx);
        }
    } else if cx.kind == http::TIMEOUT {
        plod::sse_comment(cx, "idle");
    } else if cx.kind == http::CLOSED && cx.state.worker != 0 {
        http::lane_send_value(cx.app.hub, HUB_LEAVE, Leave { worker: cx.state.worker, token: cx.state.token });
    }
}

// On the database lane.
fn run(u: DbJob.Create, c: Db&, a) {
    var id: i64 = 0;
    var ins = sqlite::Stmt {};
    let failed = sqlite::run() {
        let errs = sqlite::transaction(c.db) {
            ins = sqlite::prepare(c.db, "INSERT INTO note (text) VALUES (?)");
            sqlite::bind_text(ins, 1, u.text);
            while sqlite::step(ins) {}
            id = sqlite::last_insert_rowid(c.db);
        };
        for e in errs { sqlite::fail(e.code, e.message); }
    };
    sqlite::finalize(ins);
    guard failed.len == 0 && id > 0 else { plod::fail(a, 500, "database"); }
    var note = Note { id: id, .. };
    note.text.append(u.text);
    // Committed: the streams may see it now. A hub too full to take it
    // costs the streams this note, not the note its commit.
    http::lane_send_value(c.hub, HUB_PUBLISH, note);
    plod::reply_value(a, note);
}

fn run(u: DbJob.Get, c: Db&, a) {
    var note = Note { .. };
    var q = sqlite::Stmt {};
    let failed = sqlite::run() {
        q = sqlite::prepare(c.db, "SELECT id, text FROM note WHERE id = ?");
        sqlite::bind_i64(q, 1, u.id);
        while sqlite::step(q) {
            note.id = sqlite::column_i64(q, 0);
            var text: u8[>..] = [];
            sqlite::column_text(q, 1, text);
            note.text.clear();
            note.text.append(text[..min(text.len, 256)]);
        }
    };
    sqlite::finalize(q);
    guard failed.len == 0 else { plod::fail(a, 500, "database"); }
    guard note.id > 0 else { plod::fail(a, 404, "note_not_found"); }
    plod::reply_value(a, note);
}

// The notes whose text holds u.q, newest first, at most 100, as a JSON
// array in pieces of the worker's message size.
fn run(u: DbJob.Search, c: Db&, a) {
    var pattern: u8[>..] = [];
    pattern.push('%');
    for b in u.q {
        if b == '%' || b == '_' || b == '\\' { pattern.push('\\'); }
        pattern.push(b);
    }
    pattern.push('%');
    var out: u8[>..] = [];
    out.push('[');
    var q = sqlite::Stmt {};
    let failed = sqlite::run() {
        q = sqlite::prepare(c.db, "SELECT id, text FROM note WHERE text LIKE ? ESCAPE '\\' ORDER BY id DESC LIMIT 100");
        sqlite::bind_text(q, 1, pattern);
        var first = true;
        while sqlite::step(q) {
            var note = Note { id: sqlite::column_i64(q, 0), .. };
            var text: u8[>..] = [];
            sqlite::column_text(q, 1, text);
            note.text.append(text[..min(text.len, 256)]);
            if !first { out.push(','); }
            first = false;
            format(out, note);
        }
    };
    sqlite::finalize(q);
    guard failed.len == 0 else { plod::fail(a, 500, "database"); }
    out.push(']');
    plod::reply(a, out);
}

// On the hook lane: POSTs the note's JSON to the webhook, each wait cut to
// the time left before u.until, so the job ends by then however slow the
// receiver is. goose-http's client limits each wait, not the request, so a
// receiver that sends a byte now and then could hold it a little longer.
fn run(u: HookJob.Deliver, h: Hook&, a) {
    guard h.url.len > 0 else { plod::reply_value(a, SKIPPED); return; }
    let left = u.until - http::net_clock_ms();
    guard left > 0 else { plod::reply_value(a, TIMED_OUT); return; }
    h.session.ms = left;
    let r, e = http::post(h.session, h.url) { req => http::json(req, u.note); };
    let outcome = if e.code == http::ETIMEDOUT { TIMED_OUT } else if e.code != 0 || r.status >= 300 { FAILED } else { DELIVERED };
    plod::reply_value(a, outcome);
}

// The hub: the last RING notes, how many it has published, and each
// stream's subscription, named by its worker's box and its token: the
// address it waits at, if it waits, and the next note it hasn't had.
let RING = 16;
let MOST_STREAMS = 256;

struct Stream { worker: u64 = 0, token: u64 = 0, to: http::MailAddress = http::MailAddress { .. },
                waiting: bool = false, next: i64 = 0 }
struct Hub { notes: Note[RING] = [Note { .. }; RING], published: i64 = 0, streams: Stream[..MOST_STREAMS] = [] }

fn find_stream(h: const Hub&, worker: u64, token: u64) -> i64 {
    for s, i in h.streams { if s.worker == worker && s.token == token { return i; } }
    return -1;
}

fn drop_stream(h: Hub&, i: i64) {
    h.streams[i] = h.streams[h.streams.len - 1];
    h.streams.resize(h.streams.len - 1);
}

// Sends stream i its next note, if it waits and there is one: one more than
// RING behind, it's sent that it lagged, and dropped. A stream whose address
// went stale has closed; one whose worker's box is full waits for the next
// note, or its idle TIMEOUT, to be sent again.
fn deliver(h: Hub&, i: i64) {
    let s = h.streams[i];
    guard s.waiting && s.next < h.published else { return; }
    if h.published - s.next > RING {
        http::mail_send_value(s.to, 0, Event { kind: EVENT_LAGGED, .. });
        drop_stream(h, i);
        return;
    }
    let sent = http::mail_send_value(s.to, 0, Event { kind: EVENT_NOTE, note: h.notes[s.next % RING] });
    if sent == http::SENT {
        h.streams[i].next = s.next + 1;
        h.streams[i].waiting = false;
    } else if sent == http::STALE {
        drop_stream(h, i);
    }
}

thread_fn hub(box: http::Box) {
    var h = Hub { .. };
    http::lane(box) { tag, bytes =>
        if tag == HUB_PUBLISH {
            var note = Note { .. };
            if http::mail_value(bytes, note) {
                h.notes[h.published % RING] = note;
                h.published++;
                var i = h.streams.len;
                while i > 0 {
                    i--;
                    deliver(h, i);
                }
            }
        } else if tag == HUB_SUBSCRIBE {
            var s = Subscribe { .. };
            if http::mail_value(bytes, s) {
                let worker = http::key_of(s.to.box);
                var i = find_stream(h, worker, s.token);
                if i < 0 && h.streams.len >= MOST_STREAMS {
                    http::mail_send_value(s.to, 0, Event { kind: EVENT_FULL, .. });
                } else {
                    if i < 0 {
                        h.streams.push(Stream { worker: worker, token: s.token, next: h.published, .. });
                        i = h.streams.len - 1;
                    }
                    h.streams[i].to = s.to;
                    h.streams[i].waiting = true;
                    deliver(h, i);
                }
            }
        } else if tag == HUB_LEAVE {
            var l = Leave { .. };
            if http::mail_value(bytes, l) {
                let i = find_stream(h, l.worker, l.token);
                if i >= 0 { drop_stream(h, i); }
            }
        }
    };
}

// The database lane's connection, and the hub it publishes to.
struct Db { db: sqlite::Db = sqlite::Db {}, hub: http::Lane = http::Lane { .. } }
struct DbArgs { path: u8[..256] = "", hub: http::Lane = http::Lane { .. } }

thread_fn database(box: http::Box, jobs: plod::Jobs<DbJob>, args: DbArgs) {
    var c = Db { hub: args.hub, .. };
    let failed = sqlite::run() {
        c.db = sqlite::open(args.path);
        sqlite::exec(c.db, "PRAGMA journal_mode = WAL");
        sqlite::exec(c.db, "CREATE TABLE IF NOT EXISTS note (id INTEGER PRIMARY KEY, text TEXT NOT NULL)");
    };
    for e in failed { write_stderr(str("notes: the database: ", e.message, "\n")); }
    plod::lane(box, jobs) { u, a => run(u, c, a); };
    sqlite::close(c.db);
}

// The hook lane's session, and where it delivers.
struct Hook { session: http::Session = http::Session { .. }, url: u8[..256] = "" }
struct HookArgs { url: u8[..256] = "" }

thread_fn webhook(box: http::Box, jobs: plod::Jobs<HookJob>, args: HookArgs) {
    var h = Hook { session: http::session(WEBHOOK_MS), url: args.url };
    plod::lane(box, jobs) { u, a => run(u, h, a); };
    http::free(h.session);
}

thread_fn worker(cfg: http::Config, lanes: App) {
    var app = lanes;
    let e = plod::serve(cfg, app, St { .. }) { cx =>
        if plod::resumed(cx) {
            plod::on_reply(cx) { resume(cx.state.use, cx); };
        } else {
            handle(cx);
        }
    };
    if e.code != 0 { http::report(e); }
}

fn main() {
    let a = args();
    var port: i64 = 8080;
    if a.len > 1 {
        let p, ok = parse_int(a[1]);
        if ok { port = p; }
    }
    var path: u8[>..] = [];
    if !env("NOTES_DB", path) || path.len == 0 { path.append("notes.db"); }
    var url: u8[>..] = [];
    env("NOTES_WEBHOOK", url);
    http::exit_on_error(http::stop_on_signals());
    let hub_lane = http::lane_open(http::LaneConfig { slots: 256, size: 512, .. }, hub);
    var db_args = DbArgs { hub: hub_lane, .. };
    db_args.path.append(path[..min(path.len, 256)]);
    var hook_args = HookArgs { .. };
    hook_args.url.append(url[..min(url.len, 256)]);
    let app = App {
        db: plod::lane_open<DbJob>(http::LaneConfig { size: 512, .. }, Use.Db, db_args, database),
        hook: plod::lane_open<HookJob>(http::LaneConfig { threads: 2, size: 512, .. }, Use.Hook, hook_args, webhook),
        hub: hub_lane,
    };
    let cfg = http::Config { port: port as i32, workers: 2, mail_slots: 64, mail_size: 512, max_waiting: 1024,
                             grace_ms: 2000, .. };
    http::run(cfg, app, worker);
    plod::lane_stop(app.db);
    plod::lane_stop(app.hook);
    http::lane_stop(hub_lane);
}
