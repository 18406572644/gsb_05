'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function joinRoom(client, room, lastSeq = 0, lastRev = 0) {
  client.send({ type: 'join', room, lastSeq, lastRev });
  return client.waitFor((m) => m.type === 'joined');
}

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建房后成为管理员', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'admin');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ============================================================ 编辑 / 撤回 / 审计

async function sendMsg(client, roomId, cid, content) {
  client.send({ type: 'msg', roomId, clientMsgId: cid, content });
  return client.waitFor((m) => m.type === 'ack' && m.clientMsgId === cid);
}

const editedEvents = (client, roomId) =>
  client.log.filter((m) => m.type === 'msg_edited' && m.roomId === roomId);
const revokedEvents = (client, roomId) =>
  client.log.filter((m) => m.type === 'msg_revoked' && m.roomId === roomId);

async function historyOnce(client, roomId, beforeSeq) {
  client.send({ type: 'history', roomId, beforeSeq, limit: 50 });
  return client.waitFor((m) => m.type === 'history' && m.roomId === roomId);
}

test('编辑自己的消息：版本号递增，广播 msg_edited，历史读到新内容', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', 'orig');

    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'fixed' });
    const ea = await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
    assert.equal(ea.rev, 1);
    assert.equal(ea.version, 2);

    const ev = await b.waitFor((m) => m.type === 'msg_edited' && m.seq === 1);
    assert.equal(ev.rev, 1);
    assert.equal(ev.version, 2);
    assert.equal(ev.content, 'fixed');
    assert.equal(ev.by, ua.userId);

    const h = await historyOnce(b, roomId, 100);
    const stored = h.messages.find((x) => x.seq === 1);
    assert.equal(stored.content, 'fixed');
    assert.equal(stored.version, 2);
    assert.ok(stored.editedAt > 0);
    assert.equal(stored.revokedAt, null);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：不能编辑/撤回他人消息；管理员撤回违规消息必须填原因', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm0', 'admin message'); // seq 1 是管理员 alice 的
    await sendMsg(b, roomId, 'm1', 'spammy content'); // seq 2 是 bob 的违规消息

    // 普通成员不能动他人消息（管理员的也不行）
    b.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'x1', content: 'hacked' });
    let err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');
    b.send({ type: 'revoke_msg', roomId, seq: 1, opId: 'x2' });
    err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    // 管理员可处理违规消息，但必须填写原因
    a.send({ type: 'revoke_msg', roomId, seq: 2, opId: 'x3' });
    err = await a.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'BAD_REQUEST');

    a.send({ type: 'revoke_msg', roomId, seq: 2, opId: 'x4', reason: '广告骚扰' });
    const ack = await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'x4');
    assert.equal(ack.seq, 2);
    assert.ok(ack.revokedAt);
    const ev = await b.waitFor((m) => m.type === 'msg_revoked' && m.seq === 2);
    assert.equal(ev.reason, '广告骚扰');
    assert.equal(ev.by, ua.userId);

    // 历史翻页看到「已撤回」占位：正文脱敏，撤回人与原因保留
    const h = await historyOnce(b, roomId, 100);
    const stored = h.messages.find((x) => x.seq === 2);
    assert.equal(stored.content, null);
    assert.equal(stored.revokedBy, ua.userId);
    assert.equal(stored.revokeReason, '广告骚扰');
    assert.ok(stored.revokedAt > 0);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('版本冲突：基于旧版本的编辑被拒，错误帧带回服务端当前态', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', 'v1');

    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'v2' });
    await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
    await b.waitFor((m) => m.type === 'msg_edited' && m.rev === 1);

    // 迟到的旧操作（仍以为 version=1）—— 不得覆盖新状态
    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e2', content: 'stale' });
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'VERSION_CONFLICT');
    assert.equal(err.current.version, 2);
    assert.equal(err.current.content, 'v2');

    await sleep(200);
    assert.equal(editedEvents(b, roomId).length, 1, '冲突操作不产生事件、不广播');
    const h = await historyOnce(a, roomId, 100);
    assert.equal(h.messages[0].content, 'v2', '旧版本操作没有覆盖新内容');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('撤回是终态：撤回后拒绝编辑；重复撤回幂等，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', 'bye');

    // 同 opId 重发（网络重试）：返回成功但只广播一次
    a.send({ type: 'revoke_msg', roomId, seq: 1, opId: 'r1' });
    await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r1');
    await b.waitFor((m) => m.type === 'msg_revoked' && m.seq === 1);
    a.send({ type: 'revoke_msg', roomId, seq: 1, opId: 'r1' });
    await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r1');

    // 换新 opId 撤回已撤回消息：幂等成功（rev=null 表示没有新事件），不广播
    a.send({ type: 'revoke_msg', roomId, seq: 1, opId: 'r2' });
    const ack2 = await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r2');
    assert.equal(ack2.rev, null);

    // 撤回后编辑被终态拒绝
    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'reborn' });
    const err = await a.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MESSAGE_REVOKED');

    await sleep(200);
    assert.equal(revokedEvents(b, roomId).length, 1, '撤回只广播一次');
    assert.equal(editedEvents(b, roomId).length, 0);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发事件流：离线期间的编辑与撤回按 rev 补齐，与消息快照结果一致', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', 'one');
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    // 离线期间：新消息 -> 编辑 -> 撤回
    await sendMsg(a, roomId, 'm2', 'two');
    a.send({ type: 'edit_msg', roomId, seq: 2, version: 1, opId: 'e1', content: 'two-fixed' });
    await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
    a.send({ type: 'revoke_msg', roomId, seq: 2, opId: 'r1', reason: '误发' });
    await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r1');

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1, 0); // lastSeq=1, lastRev=0
    const done = await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.equal(done.lastSeq, 2);
    assert.equal(done.lastRev, 2);
    assert.equal(done.hasMore, false);

    // 消息快照已是撤回终态（正文脱敏）
    const snap = b.log.find((m) => m.type === 'msg' && m.seq === 2);
    assert.equal(snap.content, null);
    assert.equal(snap.revokedAt > 0, true);
    assert.equal(snap.version, 2);
    // 事件流完整：rev 1 编辑、rev 2 撤回，按序到达
    assert.deepEqual(editedEvents(b, roomId).map((e) => e.rev), [1]);
    assert.deepEqual(revokedEvents(b, roomId).map((e) => e.rev), [2]);
    assert.equal(revokedEvents(b, roomId)[0].reason, '误发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('事件帧同样走未 ACK 重发，收到 rev 累积 ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', 'hi');
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);

    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'hi2' });
    await b.waitFor((m) => m.type === 'msg_edited' && m.rev === 1);
    // 不 ACK，等服务端按 rev 重发事件帧
    await b.waitFor((m) => m.type === 'msg_edited' && m.rev === 1, 2000);
    assert.ok(editedEvents(b, roomId).length >= 2, '事件帧应被重发');

    b.send({ type: 'ack', roomId, seq: 1, rev: 1 });
    await sleep(100);
    const count = editedEvents(b, roomId).length;
    await sleep(400);
    assert.equal(editedEvents(b, roomId).length, count, 'rev ACK 后事件不再重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('审计记录：msg_audit 返回历次编辑前后原文与撤回原因', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'audit');
    await sendMsg(a, roomId, 'm1', 'first');
    a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'second' });
    await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
    a.send({ type: 'edit_msg', roomId, seq: 1, version: 2, opId: 'e2', content: 'third' });
    await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e2');
    a.send({ type: 'revoke_msg', roomId, seq: 1, opId: 'r1', reason: '包含错误信息' });
    await a.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r1');

    a.send({ type: 'msg_audit', roomId, seq: 1 });
    const au = await a.waitFor((m) => m.type === 'msg_audit' && m.seq === 1);
    // 当前快照正文已脱敏
    assert.equal(au.message.content, null);
    assert.equal(au.message.revokeReason, '包含错误信息');
    // 完整事件链：两次编辑 + 撤回，按 rev 升序，原文全部保留
    assert.deepEqual(au.events.map((e) => e.type), ['edit', 'edit', 'revoke']);
    assert.deepEqual(au.events.map((e) => e.version), [2, 3, 3]);
    assert.equal(au.events[0].contentBefore, 'first');
    assert.equal(au.events[0].contentAfter, 'second');
    assert.equal(au.events[1].contentBefore, 'second');
    assert.equal(au.events[1].contentAfter, 'third');
    assert.equal(au.events[2].contentBefore, 'third', '撤回事件保留被撤回原文');
    assert.equal(au.events[2].contentAfter, null);
    assert.equal(au.events[2].reason, '包含错误信息');
    await a.close();
  } finally {
    server.stop();
  }
});

test('持久化：重启后编辑/撤回状态与事件流仍可补发', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-edit-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      await sendMsg(a, roomId, 'm1', 'before');
      a.send({ type: 'edit_msg', roomId, seq: 1, version: 1, opId: 'e1', content: 'after' });
      await a.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token);
      await joinRoom(a, roomId, 0, 0);
      await a.waitFor((m) => m.type === 'sync_done');
      const snap = a.log.find((m) => m.type === 'msg' && m.seq === 1);
      assert.equal(snap.content, 'after');
      assert.equal(snap.version, 2);
      const ev = a.log.find((m) => m.type === 'msg_edited' && m.seq === 1);
      assert.ok(ev, '事件流重启后仍可按 rev 补发');
      assert.equal(ev.content, 'after');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('旧库迁移：无版本/事件列的老数据库升级后支持编辑与撤回', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-old-'));
  const dbPath = path.join(dir, 'old.db');
  try {
    // 手工构造升级前的老 schema
    const { DatabaseSync } = require('node:sqlite');
    const old = new DatabaseSync(dbPath);
    old.exec(`
      CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, token_random TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE rooms (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_by TEXT NOT NULL, created_at INTEGER NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE members (room_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member',
        muted_until INTEGER NOT NULL DEFAULT 0, joined_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      CREATE TABLE messages (room_id TEXT NOT NULL, seq INTEGER NOT NULL, client_msg_id TEXT NOT NULL,
        sender_id TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL,
        PRIMARY KEY (room_id, seq), UNIQUE (room_id, sender_id, client_msg_id));
      CREATE TABLE cursors (room_id TEXT NOT NULL, user_id TEXT NOT NULL, last_ack_seq INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
    `);
    old.close();

    const { server, port } = await startServer({ dbPath });
    try {
      const u = await login(port, 'oliver');
      const c = await Client.connect(port, u.token);
      const roomId = await createRoom(c, 'old-room');
      const ack = await sendMsg(c, roomId, 'm1', 'legacy message');

      // 新列已通过 ALTER 补齐：编辑、撤回均可用
      c.send({ type: 'edit_msg', roomId, seq: ack.seq, version: 1, opId: 'e1', content: 'edited on migrated db' });
      const ea = await c.waitFor((m) => m.type === 'edit_ack' && m.opId === 'e1');
      assert.equal(ea.version, 2);
      c.send({ type: 'revoke_msg', roomId, seq: ack.seq, opId: 'r1' });
      const ra = await c.waitFor((m) => m.type === 'revoke_ack' && m.opId === 'r1');
      assert.ok(ra.revokedAt);

      // 老 cursors 表也已升级，双游标 ACK 不报错
      c.send({ type: 'ack', roomId, seq: ack.seq, rev: 2 });
      await sleep(100);
      await c.close();
    } finally {
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
