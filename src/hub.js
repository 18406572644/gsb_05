'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 *
 * 两个相互独立的「推送 → 累积 ACK」空间：
 * - unacked:       Map<roomId, Map<seq, entry>> 消息快照流，按 seq 累积 ACK；
 * - unackedEvents: Map<roomId, Map<rev, entry>> 编辑/撤回事件流，按 rev 累积 ACK。
 *
 * 两条流分开追踪，是因为断线补发时消息按 seq 回放、事件按 rev 回放，游标语义不同
 * （seq 有洞要补消息；rev 有洞要补事件）。超时重发、背压、断开重连等机制对两者一致：
 * 至少一次投递 + 客户端幂等消费（消息按 seq 去重、事件按 rev 去重）。
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
    this.unackedEvents = new Map();
    this.unackedCount = 0; // 两个空间的未确认条目总数（背压判定用）
  }

  _track(map, roomId, key, frame) {
    let room = map.get(roomId);
    if (!room) {
      room = new Map();
      map.set(roomId, room);
    }
    // 同键重入（重发后重新登记）不重复计数
    if (!room.has(key)) this.unackedCount++;
    room.set(key, { frame, lastSent: now(), tries: 0 });
  }

  trackUnacked(roomId, seq, frame) {
    this._track(this.unacked, roomId, seq, frame);
  }

  trackUnackedEvent(roomId, rev, frame) {
    this._track(this.unackedEvents, roomId, rev, frame);
  }

  /** 通用累积清除：清掉 room 内所有 key <= ackKey 的项，返回清除数量 */
  static _cumulativeAck(map, roomId, ackKey, counter) {
    const room = map.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const key of room.keys()) {
      if (key <= ackKey) {
        room.delete(key);
        cleared++;
      }
    }
    if (room.size === 0) map.delete(roomId);
    return cleared;
  }

  /** 累积 ACK 消息：清除 roomId 下所有 seq <= ackSeq 的未确认项，返回清除数量 */
  ack(roomId, ackSeq) {
    const cleared = Connection._cumulativeAck(this.unacked, roomId, ackSeq);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 累积 ACK 事件：清除 roomId 下所有 rev <= ackRev 的未确认项 */
  ackEvent(roomId, ackRev) {
    const cleared = Connection._cumulativeAck(this.unackedEvents, roomId, ackRev);
    this.unackedCount -= cleared;
    return cleared;
  }

  /** 摘出所有超时未确认、需要重发的条目（消息流 + 事件流） */
  *pendingResends(staleMs) {
    const t = now();
    for (const map of [this.unacked, this.unackedEvents]) {
      for (const room of map.values()) {
        for (const entry of room.values()) {
          if (t - entry.lastSent >= staleMs) yield entry;
        }
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
    conn.unackedEvents.clear();
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
    const room = conn.unacked.get(roomId);
    const evRoom = conn.unackedEvents.get(roomId);
    if (room) { conn.unackedCount -= room.size; conn.unacked.delete(roomId); }
    if (evRoom) { conn.unackedCount -= evRoom.size; conn.unackedEvents.delete(roomId); }
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  /** 房间内在线用户 ID 列表（去重） */
  onlineUserIds(roomId) {
    const set = this.byRoom.get(roomId);
    if (!set) return [];
    return [...new Set([...set].map((c) => c.userId))];
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪：
   * - seq != null：消息流未确认项（客户端用 seq 累积 ACK）；
   * - rev != null：事件流未确认项（客户端用 rev 累积 ACK）。
   * 背压：两个空间的未确认积压合计超过上限时断开连接（客户端重连后走 sync 补发）。
   */
  send(conn, frame, { track = false, roomId = null, seq = null, rev = null } = {}) {
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
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, seq, str);
    else if (track && roomId != null && rev != null) conn.trackUnackedEvent(roomId, rev, str);
    return true;
  }

  /**
   * 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。
   * seq/rev 的含义与 send 相同，决定该帧登记到哪个未确认空间。
   */
  broadcast(roomId, frame, { track = false, seq = null, rev = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      if (this.send(conn, str, { track, roomId, seq, rev })) delivered++;
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

  /** 重发扫描：超时未 ACK 的消息/事件重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
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
