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

/** 数据库消息视图 -> 下发帧。撤回消息 content 已在持久层投影为空串，保留占位元数据 */
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
    version: m.version,
    editedAt: m.editedAt,
    recalled: m.recalled,
    recalledAt: m.recalledAt,
    recalledBy: m.recalledBy,
    recalledByName: m.recalledByName,
    recallReason: m.recallReason,
  };
}

/** 编辑/撤回事件 -> 下发帧（实时广播与断线补发共用，保证两条路径结果一致） */
function eventFrame(roomId, e) {
  if (e.action === 'edit') {
    return {
      type: 'msg_updated',
      roomId,
      seq: e.msgSeq,
      clientMsgId: e.clientMsgId,
      version: e.version,
      content: e.content,
      editedAt: e.ts,
      by: e.actorId,
      byName: e.actorName,
      reason: e.reason,
      eventSeq: e.eventSeq,
      ts: e.ts,
    };
  }
  return {
    type: 'msg_recalled',
    roomId,
    seq: e.msgSeq,
    clientMsgId: e.clientMsgId,
    version: e.version,
    recalledAt: e.ts,
    recalledBy: e.actorId,
    recalledByName: e.actorName,
    reason: e.reason,
    eventSeq: e.eventSeq,
    ts: e.ts,
  };
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /**
   * 断线补发：消息流（seq > fromSeq）与状态事件流（eventSeq > fromEventSeq）分别按序推送，
   * 分批，客户端按 sync_done 携带的双水位续拉。撤回消息在消息流里就是「已撤回」占位，
   * 编辑/撤回事件在事件流里回放——历史翻页、实时广播、补发三条路径共用同一帧构造。
   */
  function replayRoom(conn, roomId, fromSeq, fromEventSeq) {
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const msgHasMore = batch.length > config.syncBatchSize;
    const msgs = msgHasMore ? batch.slice(0, config.syncBatchSize) : batch;
    for (const m of msgs) {
      hub.send(conn, msgFrame(m), { track: true, roomId, kind: 'msg', seq: m.seq });
    }
    const lastSeq = msgs.length ? msgs[msgs.length - 1].seq : fromSeq;

    const evBatch = db.getEventsAfter(roomId, fromEventSeq, config.syncBatchSize + 1);
    const evHasMore = evBatch.length > config.syncBatchSize;
    const events = evHasMore ? evBatch.slice(0, config.syncBatchSize) : evBatch;
    for (const e of events) {
      hub.send(conn, eventFrame(roomId, e), { track: true, roomId, kind: 'event', seq: e.eventSeq });
    }
    const lastEventSeq = events.length ? events[events.length - 1].eventSeq : fromEventSeq;

    hub.send(conn, {
      type: 'sync_done',
      roomId,
      lastSeq,
      lastEventSeq,
      hasMore: msgHasMore || evHasMore,
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

  /** 操作原因：管理员处理他人消息时必填；本人操作可选。长度受限防滥用 */
  function readReason(msg, { required }) {
    if (msg.reason == null) {
      if (required) fail('BAD_REQUEST', 'reason is required for moderation actions');
      return null;
    }
    if (typeof msg.reason !== 'string' || msg.reason.length > 200) {
      fail('BAD_REQUEST', 'reason must be a string of <=200 chars');
    }
    const reason = msg.reason.trim();
    if (!reason && required) fail('BAD_REQUEST', 'reason is required for moderation actions');
    return reason || null;
  }

  /** 编辑/撤回事务的状态码 → 错误帧；冲突时携带最新消息视图，供客户端立即收敛 */
  function handleMutationResult(res) {
    switch (res.status) {
      case 'ok':
        return res;
      case 'not_found':
        return fail('NOT_FOUND', 'message not found');
      case 'recalled':
        return fail('RECALLED', 'message has been recalled');
      case 'already_recalled':
        return res; // 幂等成功（调用方自行处理）
      case 'conflict': {
        const err = new ChatError('VERSION_CONFLICT', 'message version mismatch, refresh state');
        err.current = msgFrame(res.current);
        throw err;
      }
      default:
        return fail('INTERNAL', 'unexpected mutation status');
    }
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
        lastEventSeq: 0,
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
        lastEventSeq: room.last_event_seq,
      });
      // 补发：优先用客户端上报的双水位，否则用服务端游标（新设备场景）
      const cur = db.getCursor(room.id, conn.userId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : cur.lastAckSeq;
      const fromEventSeq = Number.isInteger(msg.lastEventSeq)
        ? msg.lastEventSeq
        : cur.lastAckEventSeq;
      if (fromSeq < room.last_seq || fromEventSeq < room.last_event_seq) {
        replayRoom(conn, room.id, fromSeq, fromEventSeq);
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
        hub.broadcast(msg.roomId, msgFrame(message), { track: true, kind: 'msg', seq: message.seq });
      }
    },

    /**
     * 编辑消息：仅发送者本人可编辑（普通成员不能改他人消息）。
     * 必须携带所基于的 version；服务端事务内比对，旧版本操作返回 VERSION_CONFLICT。
     */
    edit(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!Number.isInteger(msg.seq) || msg.seq <= 0) fail('BAD_REQUEST', 'invalid seq');
      if (!Number.isInteger(msg.version) || msg.version <= 0) {
        fail('BAD_REQUEST', 'invalid base version');
      }
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      const target = db.getMessage(msg.roomId, msg.seq);
      if (!target) fail('NOT_FOUND', 'message not found');
      if (target.from !== conn.userId) fail('FORBIDDEN', 'you can only edit your own messages');
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');
      const reason = readReason(msg, { required: false });

      // 先落库（事务内版本检查 + 审计 + 事件），再广播 —— 与发送路径同一可靠性原则
      const res = handleMutationResult(db.editMessage({
        roomId: msg.roomId,
        seq: msg.seq,
        expectedVersion: msg.version,
        content: msg.content,
        actorId: conn.userId,
        reason,
      }));
      hub.broadcast(
        msg.roomId,
        eventFrame(msg.roomId, {
          action: 'edit', msgSeq: target.seq, clientMsgId: target.clientMsgId,
          version: res.message.version, content: res.message.content, ts: res.ts,
          actorId: conn.userId, actorName: conn.name, reason, eventSeq: res.eventSeq,
        }),
        { track: true, kind: 'event', seq: res.eventSeq }
      );
    },

    /**
     * 撤回消息：
     *  - 本人撤回自己的消息：原因可选；
     *  - 管理员撤回成员消息（违规处理）：原因必填；不能撤回其他管理员的消息。
     * 携带 version 做乐观锁；重复撤回（同版本重试）幂等，不再重复广播。
     * 撤回不清行——历史/补发里永久保留「已撤回」占位与操作时间、撤回人、原因。
     */
    recall(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!Number.isInteger(msg.seq) || msg.seq <= 0) fail('BAD_REQUEST', 'invalid seq');
      if (!Number.isInteger(msg.version) || msg.version <= 0) {
        fail('BAD_REQUEST', 'invalid base version');
      }
      const member = requireMember(conn, msg.roomId);
      const target = db.getMessage(msg.roomId, msg.seq);
      if (!target) fail('NOT_FOUND', 'message not found');

      const isOwner = target.from === conn.userId;
      const isAdmin = member.role === 'admin';
      if (!isOwner && !isAdmin) {
        fail('FORBIDDEN', 'you can only recall your own messages');
      }
      if (!isOwner && isAdmin) {
        const targetMember = db.getMember(msg.roomId, target.from);
        if (targetMember && targetMember.role === 'admin') {
          fail('FORBIDDEN', 'cannot recall an admin\'s message');
        }
      }
      const reason = readReason(msg, { required: !isOwner });

      const res = handleMutationResult(db.recallMessage({
        roomId: msg.roomId,
        seq: msg.seq,
        expectedVersion: msg.version,
        actorId: conn.userId,
        reason,
      }));
      if (res.status === 'already_recalled') return; // 幂等重试：不重复广播
      hub.broadcast(
        msg.roomId,
        eventFrame(msg.roomId, {
          action: 'recall', msgSeq: target.seq, clientMsgId: target.clientMsgId,
          version: res.message.version, ts: res.ts,
          actorId: conn.userId, actorName: conn.name, reason, eventSeq: res.eventSeq,
        }),
        { track: true, kind: 'event', seq: res.eventSeq }
      );
    },

    /**
     * 审计轨迹：消息的全部版本（创建/编辑/撤回），房间成员可查。
     * 数据库保留完整内容；但正文本仅管理员与发送者本人可见——其他成员查询时，
     * 被撤回消息的历史正文脱敏（仍可见版本链、操作人、原因与时间）。
     */
    revisions(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) {
        fail('BAD_REQUEST', 'invalid roomId/seq');
      }
      const member = requireMember(conn, msg.roomId);
      const target = db.getMessage(msg.roomId, msg.seq);
      if (!target) fail('NOT_FOUND', 'message not found');
      const canSeeContent = member.role === 'admin' || target.from === conn.userId;
      const list = db.listRevisions(msg.roomId, msg.seq).map((r) => (
        canSeeContent || !target.recalled
          ? r
          : { ...r, content: r.content == null ? null : '' }
      ));
      hub.send(conn, { type: 'revisions', roomId: msg.roomId, seq: msg.seq, revisions: list });
    },

    // 客户端累积 ACK：同时推进消息水位 seq 与状态事件水位 eventSeq；
    // 清除未确认队列 + 持久化双游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      const seq = Number.isInteger(msg.seq) ? msg.seq : 0;
      const eventSeq = Number.isInteger(msg.eventSeq) ? msg.eventSeq : 0;
      if (seq <= 0 && eventSeq <= 0) return;
      conn.ack(msg.roomId, seq, eventSeq || null);
      db.saveCursor(msg.roomId, conn.userId, seq, eventSeq);
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const cur = db.getCursor(msg.roomId, conn.userId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : cur.lastAckSeq;
      const fromEventSeq = Number.isInteger(msg.lastEventSeq)
        ? msg.lastEventSeq
        : cur.lastAckEventSeq;
      replayRoom(conn, msg.roomId, fromSeq, fromEventSeq);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      const messages = db.getMessagesBefore(msg.roomId, before, limit);
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
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
          ref: msg.clientMsgId || msg.roomId || undefined,
          seq: Number.isInteger(msg.seq) ? msg.seq : undefined,
          // 版本冲突时附最新消息视图，客户端可直接收敛而不必再拉一次
          current: err.current,
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
