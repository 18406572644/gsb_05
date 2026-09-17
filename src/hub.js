'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, {
 *   msgs:   Map<seq,      {frame, lastSent, tries}>,
 *   events: Map<eventSeq, {frame, lastSent, tries}>,
 * }>
 * 已推送但未被客户端累积 ACK 的消息帧与状态事件帧（编辑/撤回），超时重发；
 * 这是「至少一次投递」的服务端正，配合客户端按 seq/eventSeq 去重（幂等消费）
 * 达到效果上的恰好一次。两条水位互不阻塞：消息 ACK 不清事件、反之亦然。
 */
class Connection {
  constructor(ws, user) {
    this.id = nextConnId++;
    this.ws = ws;
    this.userId = user.id;
    this.name = user.name;
    this.connectedAt = now();
    this.lastPong = now(); // 最近一次收到 pong 的时间，心跳判活依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
  }

  _room(roomId) {
    let room = this.unacked.get(roomId);
    if (!room) {
      room = { msgs: new Map(), events: new Map() };
      this.unacked.set(roomId, room);
    }
    return room;
  }

  /** kind: 'msg'（按 seq）或 'event'（按 eventSeq） */
  trackUnacked(roomId, kind, key, frame) {
    const room = this._room(roomId);
    room[kind === 'event' ? 'events' : 'msgs'].set(key, { frame, lastSent: now(), tries: 0 });
    this.unackedCount++;
  }

  /**
   * 累积 ACK：清除 roomId 下所有 seq <= ackSeq 的消息帧，以及 eventSeq <= ackEventSeq
   * 的状态事件帧，返回新确认的总条数。任一水位缺省（null/undefined）表示不推进该水位。
   */
  ack(roomId, ackSeq, ackEventSeq) {
    const room = this.unacked.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    if (Number.isInteger(ackSeq)) {
      for (const seq of room.msgs.keys()) {
        if (seq <= ackSeq) { room.msgs.delete(seq); cleared++; }
      }
    }
    if (Number.isInteger(ackEventSeq)) {
      for (const eventSeq of room.events.keys()) {
        if (eventSeq <= ackEventSeq) { room.events.delete(eventSeq); cleared++; }
      }
    }
    if (room.msgs.size === 0 && room.events.size === 0) this.unacked.delete(roomId);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 摘出所有超时未确认、需要重发的条目（消息帧与事件帧） */
  *pendingResends(staleMs) {
    const t = now();
    for (const room of this.unacked.values()) {
      for (const entry of room.msgs.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
      for (const entry of room.events.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳与重发扫描。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);
  }

  remove(conn) {
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }
    for (const roomId of conn.rooms) this._leaveRoomSet(roomId, conn);
    conn.rooms.clear();
    conn.unacked.clear();
    conn.unackedCount = 0;
  }

  joinRoom(conn, roomId) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    conn.rooms.add(roomId);
  }

  leaveRoom(conn, roomId) {
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    conn.unacked.delete(roomId);
    this._recount(conn);
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  _recount(conn) {
    let n = 0;
    for (const room of conn.unacked.values()) n += room.msgs.size + room.events.size;
    conn.unackedCount = n;
  }

  /** 房间内在线用户 ID 列表（去重） */
  onlineUserIds(roomId) {
    const set = this.byRoom.get(roomId);
    if (!set) return [];
    return [...new Set([...set].map((c) => c.userId))];
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（msg/event 类帧）。
   * 背压：未确认积压超过上限时断开连接（客户端重连后走 sync 补发）。
   */
  send(conn, frame, { track = false, roomId = null, kind = 'msg', seq = null } = {}) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;
    if (track && conn.unackedCount >= this.config.maxUnackedPerConn) {
      conn.ws.close(1013, 'backpressure: too many unacked messages');
      return false;
    }
    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, kind, seq, str);
    return true;
  }

  /** 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。 */
  broadcast(roomId, frame, { track = false, kind = 'msg', seq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      if (this.send(conn, str, { track, roomId, kind, seq })) delivered++;
    }
    return delivered;
  }

  /** 心跳扫描：超时未 pong 的连接直接 terminate（触发 close 走正常清理） */
  heartbeatSweep() {
    const t = now();
    for (const conn of this.all) {
      if (t - conn.lastPong > this.config.heartbeatTimeoutMs) {
        conn.ws.terminate();
        continue;
      }
      try {
        conn.ws.ping();
      } catch { /* 连接已损坏，等待 close 事件清理 */ }
    }
  }

  /** 重发扫描：超时未 ACK 的消息/事件帧重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend } = this.config;
    for (const conn of this.all) {
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          conn.ws.close(1011, 'ack timeout');
          break;
        }
        if (conn.ws.readyState === 1) {
          try {
            conn.ws.send(entry.frame);
            entry.lastSent = now();
          } catch { /* 下一轮再处理 */ }
        }
      }
    }
  }

  stats() {
    return {
      connections: this.all.size,
      users: this.byUser.size,
      rooms: this.byRoom.size,
    };
  }
}

module.exports = { Hub, Connection };
