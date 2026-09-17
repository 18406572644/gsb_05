'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const defaultConfig = require('./config');
const {
  randomId,
  randomSecret,
  signToken,
  verifyToken,
  isNonEmptyString,
  parseFrame,
  now,
} = require('./util');

/** 业务错误：handler 抛出，统一转成 error 帧回给客户端 */
class ChatError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const fail = (code, message) => {
  throw new ChatError(code, message);
};

/** 令牌桶限流（按用户），防刷屏 */
class TokenBucket {
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.buckets = new Map();
  }
  take(key) {
    const t = now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: this.burst, updated: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.burst, b.tokens + ((t - b.updated) / 1000) * this.rate);
    b.updated = t;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }
}

/** 数据库消息行 -> 下发帧（撤回消息在 db 层已把 content 脱敏为 null） */
function msgFrame(m) {
  return {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
    version: m.version ?? 1,
    editedAt: m.editedAt ?? null,
    revokedAt: m.revokedAt ?? null,
    revokedBy: m.revokedBy ?? null,
    revokedByName: m.revokedByName ?? null,
    revokeReason: m.revokeReason ?? null,
  };
}

/** 审计事件行 -> 下发帧。编辑/撤回前的原文（contentBefore）不随事件流下发，只走 msg_audit */
function eventFrame(e) {
  const base = {
    type: e.type === 'edit' ? 'msg_edited' : 'msg_revoked',
    roomId: e.roomId,
    rev: e.rev,
    seq: e.seq,
    version: e.version,
    by: e.actorId,
    byName: e.actorName,
    reason: e.reason ?? null,
    ts: e.ts,
  };
  if (e.type === 'edit') {
    base.content = e.contentAfter;
    base.editedAt = e.editedAt;
  }
  return base;
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /**
   * 断线补发：双流回放。
   * 1) 先按 seq 补消息快照（快照即最新态：已编辑内容 / 已撤回占位）；
   * 2) 消息追平后再按 rev 补编辑/撤回事件——客户端按 version/rev 幂等应用，
   *    事件不会把已为新态的快照回滚成旧态；
   * 3) 一个 sync_done 同时携带两个游标，hasMore 时客户端续拉。
   */
  function replayRoom(conn, roomId, fromSeq, fromRev) {
    const msgBatch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const msgsHasMore = msgBatch.length > config.syncBatchSize;
    const msgs = msgsHasMore ? msgBatch.slice(0, config.syncBatchSize) : msgBatch;
    for (const m of msgs) hub.send(conn, msgFrame(m), { track: true, roomId, seq: m.seq });
    const lastSeq = msgs.length ? msgs[msgs.length - 1].seq : fromSeq;

    let lastRev = fromRev;
    let eventsHasMore = false;
    if (!msgsHasMore) {
      const evBatch = db.getEventsAfter(roomId, fromRev, config.syncBatchSize + 1);
      eventsHasMore = evBatch.length > config.syncBatchSize;
      const events = eventsHasMore ? evBatch.slice(0, config.syncBatchSize) : evBatch;
      for (const e of events) hub.send(conn, eventFrame(e), { track: true, roomId, rev: e.rev });
      lastRev = events.length ? events[events.length - 1].rev : fromRev;
    }

    hub.send(conn, {
      type: 'sync_done',
      roomId,
      lastSeq,
      lastRev,
      hasMore: msgsHasMore || eventsHasMore,
    });
  }

  function requireMember(conn, roomId) {
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this room');
    return member;
  }

  function requireAdmin(conn, roomId) {
    const member = requireMember(conn, roomId);
    if (member.role !== 'admin') fail('FORBIDDEN', 'admin role required');
    return member;
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    create_room(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid room name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'room name already taken');
      const room = db.createRoom(randomId('r_'), msg.name, conn.userId);
      hub.joinRoom(conn, room.id);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: 'admin',
        mutedUntil: 0,
        lastSeq: 0,
      });
    },

    join(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.getRoom(msg.room) || db.getRoomByName(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');
      db.joinRoom(room.id, conn.userId);
      hub.joinRoom(conn, room.id);
      const member = db.getMember(room.id, conn.userId);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: member.role,
        mutedUntil: member.muted_until,
        lastSeq: room.last_seq,
        lastEventRev: room.last_event_rev,
      });
      // 补发：优先用客户端上报的进度，否则用服务端游标（新设备则从游标开始）
      const cur = db.getCursor(room.id, conn.userId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : cur.lastAckSeq;
      const fromRev = Number.isInteger(msg.lastRev) ? msg.lastRev : cur.lastAckRev;
      if (fromSeq < room.last_seq || fromRev < room.last_event_rev) {
        replayRoom(conn, room.id, fromSeq, fromRev);
      }
    },

    leave(conn, msg) {
      hub.leaveRoom(conn, msg.roomId);
      hub.send(conn, { type: 'left', roomId: msg.roomId });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');

      // 先落库（同事务分配 seq），再 ACK，再广播 —— 崩溃也不丢已确认消息
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
      });
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化双游标（断线补发的兜底依据）
    // seq 确认消息流、rev 确认事件流；可同时上报，也可分别上报。
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      const hasSeq = Number.isInteger(msg.seq) && msg.seq >= 0;
      const hasRev = Number.isInteger(msg.rev) && msg.rev >= 0;
      if (!hasSeq && !hasRev) return;
      if (hasSeq) conn.ack(msg.roomId, msg.seq);
      if (hasRev) conn.ackEvent(msg.roomId, msg.rev);
      const cur = db.getCursor(msg.roomId, conn.userId);
      db.saveCursor(
        msg.roomId,
        conn.userId,
        hasSeq ? msg.seq : cur.lastAckSeq,
        hasRev ? msg.rev : cur.lastAckRev
      );
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const cur = db.getCursor(msg.roomId, conn.userId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : cur.lastAckSeq;
      const fromRev = Number.isInteger(msg.lastRev) ? msg.lastRev : cur.lastAckRev;
      replayRoom(conn, msg.roomId, fromSeq, fromRev);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      const messages = db.getMessagesBefore(msg.roomId, before, limit);
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
    },

    /**
     * 编辑自己的消息（CAS）。
     * 必须带 seq + version（客户端所见版本）+ opId（操作幂等）。
     * 撤回是终态：已撤回消息拒绝编辑；版本不匹配返回 VERSION_CONFLICT + 服务端当前态，
     * 由客户端合并刷新，旧操作不会覆盖新状态。
     */
    edit_msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!Number.isInteger(msg.seq) || msg.seq <= 0) fail('BAD_REQUEST', 'invalid seq');
      if (!Number.isInteger(msg.version) || msg.version < 1) fail('BAD_REQUEST', 'invalid version');
      if (!isNonEmptyString(msg.opId, 64)) fail('BAD_REQUEST', 'invalid opId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      if (member.muted_until > now()) fail('MUTED', 'you are muted');
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'too many operations, slow down');

      const target = db.getMessage(msg.roomId, msg.seq);
      if (!target) fail('NO_SUCH_MESSAGE', 'message not found');
      if (target.from !== conn.userId) fail('FORBIDDEN', 'you can only edit your own messages');

      const r = db.editMessage({
        roomId: msg.roomId,
        seq: msg.seq,
        content: msg.content,
        expectedVersion: msg.version,
        editorId: conn.userId,
        opId: msg.opId,
      });

      if (r.status === 'not_found') fail('NO_SUCH_MESSAGE', 'message not found');
      if (r.status === 'revoked') {
        // 终态冲突同样回传权威当前态（已撤回占位），客户端直接覆盖本地
        hub.send(conn, {
          type: 'error',
          code: 'MESSAGE_REVOKED',
          message: 'message has been revoked and cannot be edited',
          ref: msg.opId,
          current: msgFrame(r.message),
        });
        return;
      }
      if (r.status === 'conflict') {
        // 先回操作失败（带服务端当前态），让客户端覆盖本地旧版本
        hub.send(conn, {
          type: 'error',
          code: 'VERSION_CONFLICT',
          message: 'message was modified by a newer operation',
          ref: msg.opId,
          current: msgFrame(r.current),
        });
        return;
      }

      // ok / duplicate：都回 edit_ack（重试拿到同一 rev，操作幂等）
      hub.send(conn, {
        type: 'edit_ack',
        roomId: msg.roomId,
        seq: msg.seq,
        opId: msg.opId,
        rev: r.event.rev,
        version: r.message.version,
        editedAt: r.message.editedAt,
      });
      if (r.status === 'ok') {
        hub.broadcast(msg.roomId, eventFrame(r.event), { track: true, rev: r.event.rev });
      }
    },

    /**
     * 撤回消息：本人可撤回自己的消息；管理员可按权限处理违规消息（需填原因）。
     * 撤回为终态且幂等——重复撤回（同 opId 或消息已是撤回态）都成功返回，不重复广播。
     */
    revoke_msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!Number.isInteger(msg.seq) || msg.seq <= 0) fail('BAD_REQUEST', 'invalid seq');
      if (!isNonEmptyString(msg.opId, 64)) fail('BAD_REQUEST', 'invalid opId');
      // 原因可选，但管理员审核处理违规消息时强制填写
      if (msg.reason !== undefined && msg.reason !== null && !isNonEmptyString(msg.reason, 500)) {
        fail('BAD_REQUEST', 'invalid reason');
      }
      const member = requireMember(conn, msg.roomId);
      const target = db.getMessage(msg.roomId, msg.seq);
      if (!target) fail('NO_SUCH_MESSAGE', 'message not found');

      const isOwner = target.from === conn.userId;
      const isAdmin = member.role === 'admin';
      if (!isOwner && !isAdmin) fail('FORBIDDEN', 'you can only revoke your own messages');
      // 管理员撤回他人消息 = 审核处理，必须给原因
      if (isAdmin && !isOwner && !isNonEmptyString(msg.reason, 500)) {
        fail('BAD_REQUEST', 'reason is required when an admin revokes another member\'s message');
      }
      if (member.muted_until > now() && !isAdmin) fail('MUTED', 'you are muted');
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'too many operations, slow down');

      const r = db.revokeMessage({
        roomId: msg.roomId,
        seq: msg.seq,
        actorId: conn.userId,
        opId: msg.opId,
        reason: msg.reason ?? null,
      });

      if (r.status === 'not_found') fail('NO_SUCH_MESSAGE', 'message not found');

      hub.send(conn, {
        type: 'revoke_ack',
        roomId: msg.roomId,
        seq: msg.seq,
        opId: msg.opId,
        rev: r.event ? r.event.rev : null,
        revokedAt: r.message.revokedAt,
      });
      if (r.status === 'ok') {
        hub.broadcast(msg.roomId, eventFrame(r.event), { track: true, rev: r.event.rev });
      }
    },

    /** 查看单条消息的完整审计记录（房间成员可见；含历次编辑的前后原文与撤回原因） */
    msg_audit(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!Number.isInteger(msg.seq) || msg.seq <= 0) fail('BAD_REQUEST', 'invalid seq');
      requireMember(conn, msg.roomId);
      const message = db.getMessage(msg.roomId, msg.seq);
      if (!message) fail('NO_SUCH_MESSAGE', 'message not found');
      const events = db.getAuditTrail(msg.roomId, msg.seq);
      hub.send(conn, {
        type: 'msg_audit',
        roomId: msg.roomId,
        seq: msg.seq,
        message: msgFrame(message),
        // 审计视图才下发历次原文；普通事件广播不含 contentBefore
        events: events.map((e) => ({
          rev: e.rev,
          type: e.type,
          by: e.actorId,
          byName: e.actorName,
          reason: e.reason ?? null,
          version: e.version,
          editedAt: e.editedAt ?? null,
          contentBefore: e.contentBefore,
          contentAfter: e.contentAfter,
          ts: e.ts,
        })),
      });
    },

    rooms(conn) {
      hub.send(conn, { type: 'rooms', rooms: db.listRoomsForUser(conn.userId) });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    mute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'admin') fail('FORBIDDEN', 'cannot mute an admin');
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(msg.roomId, msg.userId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'muted',
        userId: msg.userId,
        until,
        by: conn.userId,
      });
    },

    unmute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(msg.roomId, msg.userId, 0);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'unmuted',
        userId: msg.userId,
        by: conn.userId,
      });
    },
  };

  function onFrame(conn, raw) {
    const msg = parseFrame(raw);
    if (!msg) {
      hub.send(conn, { type: 'error', code: 'BAD_FRAME', message: 'invalid JSON frame' });
      return;
    }
    const handler = handlers[msg.type];
    if (!handler) {
      hub.send(conn, { type: 'error', code: 'UNKNOWN_TYPE', message: `unknown type: ${msg.type}` });
      return;
    }
    try {
      handler(conn, msg);
    } catch (err) {
      if (err instanceof ChatError) {
        hub.send(conn, {
          type: 'error',
          code: err.code,
          message: err.message,
          ref: msg.opId || msg.clientMsgId || msg.roomId || undefined,
        });
      } else {
        console.error('[handler error]', msg.type, err);
        hub.send(conn, { type: 'error', code: 'INTERNAL', message: 'internal error' });
      }
    }
  }

  // ---------------------------------------------------------------- HTTP 层

  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css' };

  function readBody(req, limit = 4096) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && url.pathname === '/api/login') {
      // 演示级登录：按用户名创建/复用账号，返回签名 token
      try {
        const body = JSON.parse(await readBody(req));
        if (!isNonEmptyString(body.name, 32)) return json(400, { error: 'invalid name' });
        let user = db.getUserByName(body.name);
        if (!user) user = db.createUser(randomId('u_'), body.name, randomSecret());
        const token = signToken(user.id, user.token_random, config.authSecret);
        return json(200, { userId: user.id, name: user.name, token });
      } catch {
        return json(400, { error: 'bad request' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json(200, { ok: true, ...hub.stats() });
    }

    if (req.method === 'GET') {
      const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.resolve(publicDir, rel);
      if (!file.startsWith(publicDir) || !MIME[path.extname(file)]) {
        res.writeHead(404).end('not found');
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end('not found');
          return;
        }
        res.writeHead(200, { 'content-type': MIME[path.extname(file)] });
        res.end(data);
      });
      return;
    }

    res.writeHead(404).end('not found');
  });

  // ---------------------------------------------------------------- WS 层

  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', (req, socket, head) => {
    const reject = (code, text) => {
      socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    };
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') return reject(404, 'Not Found');

    const userId = verifyToken(url.searchParams.get('token'), config.authSecret);
    const user = userId && db.getUserById(userId);
    if (!user) return reject(401, 'Unauthorized');

    const denied = hub.checkAdmission(user.id);
    if (denied) return reject(503, denied);

    wss.handleUpgrade(req, socket, head, (ws) => {
      const conn = new Connection(ws, user);
      hub.add(conn);

      ws.on('pong', () => {
        conn.lastPong = now();
      });
      ws.on('message', (raw) => onFrame(conn, raw));
      ws.on('close', () => hub.remove(conn));
      ws.on('error', () => {}); // 错误后必随 close，统一在 close 清理

      hub.send(conn, { type: 'welcome', userId: user.id, name: user.name, serverTime: now() });
    });
  });

  // ---------------------------------------------------------------- 定时任务

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
  ];
  for (const t of timers) t.unref();

  // ---------------------------------------------------------------- 生命周期

  function start() {
    return new Promise((resolve) => {
      httpServer.listen(config.port, config.host, () => {
        const addr = httpServer.address();
        console.log(`[chat] listening on http://${addr.address}:${addr.port}  (db: ${config.dbPath})`);
        resolve(addr);
      });
    });
  }

  function stop() {
    for (const t of timers) clearInterval(t);
    for (const conn of [...hub.all]) {
      hub.send(conn, { type: 'server_shutdown' });
      conn.ws.terminate();
    }
    wss.close();
    httpServer.close();
    db.close();
  }

  return { config, db, hub, httpServer, wss, start, stop };
}

// 直接运行：node src/server.js
if (require.main === module) {
  const server = createChatServer();
  server.start();
  const shutdown = () => {
    console.log('\n[chat] shutting down...');
    server.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createChatServer };
