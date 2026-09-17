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

async function joinRoom(client, room, lastSeq = 0, lastEventSeq = 0) {
  client.send({ type: 'join', room, lastSeq, lastEventSeq });
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

// ---------------------------------------------------------------- 编辑 / 撤回 / 审计

/** 发一条消息并返回 ACK 分配的 seq */
async function sendMsg(client, roomId, clientMsgId, content) {
  client.send({ type: 'msg', roomId, clientMsgId, content });
  const ack = await client.waitFor((m) => m.type === 'ack' && m.clientMsgId === clientMsgId);
  return ack.seq;
}

test('编辑自己的消息：广播 msg_updated，版本号与编辑时间正确', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    const seq = await sendMsg(a, roomId, 'e1', 'original');
    await b.waitFor((m) => m.type === 'msg' && m.seq === seq);

    a.send({ type: 'edit', roomId, seq, version: 1, content: 'fixed' });
    const upd = await b.waitFor((m) => m.type === 'msg_updated' && m.seq === seq);
    assert.equal(upd.content, 'fixed');
    assert.equal(upd.version, 2);
    assert.equal(upd.eventSeq, 1);
    assert.ok(upd.editedAt > 0);
    assert.equal(upd.by, ua.userId);

    // 历史翻页读到的是编辑后的内容与版本
    a.send({ type: 'history', roomId, beforeSeq: seq + 1, limit: 10 });
    const h = await a.waitFor((m) => m.type === 'history');
    const row = h.messages.find((m) => m.seq === seq);
    assert.equal(row.content, 'fixed');
    assert.equal(row.version, 2);
    assert.ok(row.editedAt > 0);
    assert.equal(row.recalled, false);
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('权限：不能编辑他人消息；普通成员不能撤回他人消息；管理员撤成员消息必须带原因', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    const bobSeq = await sendMsg(b, roomId, 'b1', 'bob speaks');
    const aliceSeq = await sendMsg(a, roomId, 'a1', 'alice speaks');
    await b.waitFor((m) => m.type === 'msg' && m.seq === aliceSeq);

    // 管理员也不能编辑别人的消息
    a.send({ type: 'edit', roomId, seq: bobSeq, version: 1, content: 'hijacked' });
    const err1 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    // 普通成员不能撤回他人消息
    b.send({ type: 'recall', roomId, seq: aliceSeq, version: 1 });
    const err2 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');

    // 管理员撤回成员消息不带原因必须被拒
    a.send({ type: 'recall', roomId, seq: bobSeq, version: 1 });
    const err3 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err3.code, 'BAD_REQUEST', '管理员处理他人消息必须填写原因');

    // 带原因撤回成功并广播
    a.send({ type: 'recall', roomId, seq: bobSeq, version: 1, reason: '违规内容' });
    const rec = await b.waitFor((m) => m.type === 'msg_recalled' && m.seq === bobSeq);
    assert.equal(rec.version, 2);
    assert.equal(rec.reason, '违规内容');
    assert.equal(rec.recalledBy, ua.userId);
    assert.ok(rec.recalledAt > 0);
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('撤回后保留占位：内容屏蔽，历史/补发均带 recalled 与操作时间', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    const seq = await sendMsg(a, roomId, 'r1', 'will disappear');
    await b.waitFor((m) => m.type === 'msg' && m.seq === seq);
    await b.close();

    // 发送者离线期间撤回自己的消息
    a.send({ type: 'recall', roomId, seq, version: 1, reason: '发错了' });
    const rec = await a.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq);
    assert.equal(rec.eventSeq, 1);

    // B 重连：消息流与事件流分别补发；历史最终为撤回占位
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 0, 0);
    const done = await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.equal(done.lastEventSeq, 1);
    const msg = b.log.filter((m) => m.type === 'msg' && m.seq === seq).pop();
    assert.equal(msg.recalled, true);
    assert.equal(msg.content, '', '撤回后内容不得再下发');
    assert.ok(msg.recalledAt > 0);
    assert.equal(msg.recallReason, '发错了');
    const recalledEvt = b.log.find((m) => m.type === 'msg_recalled' && m.seq === seq);
    assert.ok(recalledEvt, '离线期间的撤回事件须经事件流补发');

    // 历史翻页同样返回占位
    b.send({ type: 'history', roomId, beforeSeq: seq + 1, limit: 10 });
    const h = await b.waitFor((m) => m.type === 'history');
    const row = h.messages.find((m) => m.seq === seq);
    assert.equal(row.recalled, true);
    assert.equal(row.content, '');
    assert.ok(row.recalledAt > 0);
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('审计轨迹：revisions 返回创建/编辑/撤回全部版本与操作人原因', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'audit');
    const seq = await sendMsg(a, roomId, 'a1', 'v1 text');

    a.send({ type: 'edit', roomId, seq, version: 1, content: 'v2 text' });
    await a.waitFor((m) => m.type === 'msg_updated' && m.seq === seq);
    a.send({ type: 'edit', roomId, seq, version: 2, content: 'v3 text' });
    await a.waitFor((m) => m.type === 'msg_updated' && m.seq === seq && m.version === 3);
    a.send({ type: 'recall', roomId, seq, version: 3, reason: '本人撤回' });
    await a.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq);

    a.send({ type: 'revisions', roomId, seq });
    const r = await a.waitFor((m) => m.type === 'revisions' && m.seq === seq);
    assert.equal(r.revisions.length, 4);
    assert.deepEqual(r.revisions.map((x) => x.action), ['create', 'edit', 'edit', 'recall']);
    assert.deepEqual(r.revisions.map((x) => x.version), [1, 2, 3, 4]);
    assert.equal(r.revisions[0].content, 'v1 text');
    assert.equal(r.revisions[2].content, 'v3 text');
    assert.equal(r.revisions[3].content, null);
    assert.equal(r.revisions[3].reason, '本人撤回');
    assert.equal(r.revisions[3].actorName, 'alice');

    // 撤回后不能再编辑
    a.send({ type: 'edit', roomId, seq, version: 4, content: 'after recall' });
    const err = await a.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'RECALLED');
    await a.close();
  } finally {
    server.stop();
  }
});

test('乐观锁：基于旧版本的编辑/撤回被拒绝并回传最新状态', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'general');
    const seq = await sendMsg(a, roomId, 'c1', 'first');

    // 第一次编辑 v1 -> v2 成功
    a.send({ type: 'edit', roomId, seq, version: 1, content: 'second' });
    await a.waitFor((m) => m.type === 'msg_updated' && m.version === 2);

    // 迟到的旧操作（仍基于 v1）必须失败
    a.send({ type: 'edit', roomId, seq, version: 1, content: 'stale edit' });
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'VERSION_CONFLICT');
    assert.equal(err.seq, seq);
    assert.equal(err.current.version, 2, '冲突错误须携带最新消息视图');
    assert.equal(err.current.content, 'second');
    assert.equal(err.current.recalled, false);

    // 撤回与编辑并发：消息尚未撤回时，基于旧版本 v1 的撤回同样必须冲突，不能覆盖新状态
    a.send({ type: 'recall', roomId, seq, version: 1, reason: 'stale recall' });
    const err2 = await a.waitFor((m) => m.type === 'error' && m.code === 'VERSION_CONFLICT');
    assert.equal(err2.current.version, 2);
    assert.equal(err2.current.recalled, false);

    // 基于当前版本 v2 的撤回成功，最终状态为撤回占位 v3
    a.send({ type: 'recall', roomId, seq, version: 2, reason: 'valid recall' });
    await a.waitFor((m) => m.type === 'msg_recalled' && m.version === 3);
    a.send({ type: 'history', roomId, beforeSeq: seq + 1, limit: 10 });
    const h = await a.waitFor((m) => m.type === 'history');
    const row = h.messages.find((m) => m.seq === seq);
    assert.equal(row.recalled, true);
    assert.equal(row.version, 3);
    assert.equal(row.recallReason, 'valid recall', '迟到撤回的原因不得覆盖先到操作');
    await a.close();
  } finally {
    server.stop();
  }
});

test('撤回幂等：同版本重复撤回不产生新版本、不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    const seq = await sendMsg(a, roomId, 'i1', 'hello');
    await b.waitFor((m) => m.type === 'msg' && m.seq === seq);

    a.send({ type: 'recall', roomId, seq, version: 1 });
    await b.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq);
    // 客户端重试同一请求
    a.send({ type: 'recall', roomId, seq, version: 1 });
    await sleep(300);
    assert.equal(
      b.log.filter((m) => m.type === 'msg_recalled' && m.seq === seq).length,
      1,
      '重复撤回不得重复广播'
    );
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('编辑后再撤回：离线成员经事件流按序收到 edit 与 recall', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    const seq = await sendMsg(a, roomId, 'er1', 'orig');
    await b.waitFor((m) => m.type === 'msg' && m.seq === seq);
    b.send({ type: 'ack', roomId, seq, eventSeq: 0 });
    await b.close();

    a.send({ type: 'edit', roomId, seq, version: 1, content: 'edited' });
    await a.waitFor((m) => m.type === 'msg_updated' && m.version === 2);
    a.send({ type: 'recall', roomId, seq, version: 2, reason: '违规' });
    await a.waitFor((m) => m.type === 'msg_recalled' && m.version === 3);

    // B 只带消息水位（seq 已看过），事件水位为 0 —— 两个事件都应补发且有序
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, seq, 0);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId && m.lastEventSeq === 2);
    const edits = b.log.filter((m) => m.type === 'msg_updated' && m.seq === seq);
    const recalls = b.log.filter((m) => m.type === 'msg_recalled' && m.seq === seq);
    assert.equal(edits.length, 1);
    assert.equal(recalls.length, 1);
    assert.equal(edits[0].eventSeq < recalls[0].eventSeq, true);
    assert.equal(recalls[0].reason, '违规');
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('事件流 ACK：未确认的 msg_recalled 由服务端重发', async () => {
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
    const seq = await sendMsg(a, roomId, 'rr1', 'hi');
    await b.waitFor((m) => m.type === 'msg' && m.seq === seq);

    a.send({ type: 'recall', roomId, seq, version: 1 });
    await b.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq);
    // 不 ACK 事件，应观察到重发
    await b.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq, 2000);
    const count = b.log.filter((m) => m.type === 'msg_recalled' && m.seq === seq).length;
    assert.ok(count >= 2, '状态事件帧同样受未 ACK 重发保护');

    // 事件水位 ACK 后停止重发（不带 seq，只推进事件水位）
    b.send({ type: 'ack', roomId, seq, eventSeq: 1 });
    await sleep(100);
    const after = b.log.filter((m) => m.type === 'msg_recalled' && m.seq === seq).length;
    await sleep(400);
    assert.equal(
      b.log.filter((m) => m.type === 'msg_recalled' && m.seq === seq).length,
      after,
      '事件 ACK 后不再重发'
    );
    await Promise.all([a.close(), b.close()]);
  } finally {
    server.stop();
  }
});

test('审计脱敏：撤回后非相关成员查 revisions 不见正文，发送者与管理员可见全文', async () => {
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

    const seq = await sendMsg(b, roomId, 'b1', 'secret content');
    await c.waitFor((m) => m.type === 'msg' && m.seq === seq);
    a.send({ type: 'recall', roomId, seq, version: 1, reason: '违规' });
    await c.waitFor((m) => m.type === 'msg_recalled' && m.seq === seq);

    // 旁观成员 carol：版本链可见，正文脱敏
    c.send({ type: 'revisions', roomId, seq });
    const rc = await c.waitFor((m) => m.type === 'revisions');
    assert.equal(rc.revisions.length, 2);
    assert.equal(rc.revisions[0].content, '');
    assert.equal(rc.revisions[0].action, 'create');
    assert.equal(rc.revisions[1].reason, '违规', '操作原因仍须可见');

    // 发送者 bob 与管理员 alice 可见完整正文
    b.send({ type: 'revisions', roomId, seq });
    const rb = await b.waitFor((m) => m.type === 'revisions');
    assert.equal(rb.revisions[0].content, 'secret content');
    a.send({ type: 'revisions', roomId, seq });
    const ra = await a.waitFor((m) => m.type === 'revisions');
    assert.equal(ra.revisions[0].content, 'secret content');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('旧库迁移：存量消息补 create 审计行，字段读取正常', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-legacy-'));
  const dbPath = path.join(dir, 'legacy.db');
  try {
    // 手工构造一张「旧版」schema 的库
    const { DatabaseSync } = require('node:sqlite');
    const legacy = new DatabaseSync(dbPath);
    legacy.exec(`
      CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, token_random TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE rooms (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_by TEXT NOT NULL, created_at INTEGER NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE members (room_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member', muted_until INTEGER NOT NULL DEFAULT 0, joined_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      CREATE TABLE messages (room_id TEXT NOT NULL, seq INTEGER NOT NULL, client_msg_id TEXT NOT NULL, sender_id TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (room_id, seq));
      CREATE TABLE cursors (room_id TEXT NOT NULL, user_id TEXT NOT NULL, last_ack_seq INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      INSERT INTO users VALUES ('u1','alice','rnd',1000);
      INSERT INTO rooms VALUES ('r1','old','u1',1000,1);
      INSERT INTO members VALUES ('r1','u1','admin',0,1000);
      INSERT INTO messages VALUES ('r1',1,'cid','u1','legacy content',1234);
    `);
    legacy.close();

    const { server, port } = await startServer({ dbPath });
    try {
      const token = require('../src/util').signToken('u1', 'rnd', server.config.authSecret);
      const c = await Client.connect(port, token);
      c.send({ type: 'revisions', roomId: 'r1', seq: 1 });
      const r = await c.waitFor((m) => m.type === 'revisions');
      assert.equal(r.revisions.length, 1);
      assert.equal(r.revisions[0].action, 'create');
      assert.equal(r.revisions[0].content, 'legacy content');

      c.send({ type: 'history', roomId: 'r1', beforeSeq: 100, limit: 10 });
      const h = await c.waitFor((m) => m.type === 'history');
      assert.equal(h.messages[0].version, 1);
      assert.equal(h.messages[0].content, 'legacy content');
      await c.close();
    } finally {
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
