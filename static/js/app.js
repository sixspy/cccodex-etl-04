const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      board: null,
      busy: false,
      msg: "",
      msgErr: false,
      addMsg: "",
      newName: "",
      draftPolicy: "",
      draftNote: "",
      dirty: false,
    };
  },
  computed: {
    orderFrozen() {
      return !!(this.board && this.board.summary && this.board.summary.in_flow);
    },
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await this.refresh();
  },
  methods: {
    tagClass(status) {
      if (status === "已落库") return "ok";
      if (status === "已抽取待加载") return "warn";
      if (status === "待对齐") return "align";
      if (status === "抽取中") return "busy";
      return "";
    },
    async jsonFetch(url, options) {
      const res = await fetch(url, options);
      let data = {};
      try {
        data = await res.json();
      } catch (e) {
        data = { ok: false, error: "响应无法解析" };
      }
      return { res, data };
    },
    async refresh() {
      const { res, data } = await this.jsonFetch("/api/pipelines");
      if (!res.ok) return;
      this.items = data.items || [];
      if (this.current) {
        await this.open(this.current.id, true);
      }
    },
    async open(id, silent) {
      const { res, data } = await this.jsonFetch(`/api/pipelines/${id}/board`);
      if (!res.ok) return;
      this.current = data.pipeline;
      this.applyBoard(data);
      if (!silent) {
        this.msg = "";
        this.msgErr = false;
        this.addMsg = "";
      }
    },
    applyBoard(payload) {
      this.board = payload;
      this.draftPolicy = payload.policy.skip_policy;
      this.draftNote = payload.policy.policy_note;
      this.dirty = false;
    },
    showError(data, fallback) {
      this.msg = data.error || fallback;
      this.msgErr = true;
    },
    async extractTable(table) {
      if (!this.board || this.busy) return;
      this.busy = true;
      this.msg = "";
      this.msgErr = false;
      try {
        const { res, data } = await this.jsonFetch(
          `/api/pipelines/${this.board.pipeline.id}/tables/${table.id}/extract`,
          { method: "POST" }
        );
        if (!res.ok) {
          this.showError(data, "抽取被拒绝");
        } else if (data.align_pending) {
          this.msg = `${data.table.name} 已抽取，但上游未齐，标待对齐并禁止落库`;
          this.msgErr = false;
        } else {
          this.msg = `${data.table.name} 抽取完成，可继续落库`;
          this.msgErr = false;
        }
        await this.afterChange();
      } finally {
        this.busy = false;
      }
    },
    async loadTable(table) {
      if (!this.board || this.busy) return;
      this.busy = true;
      this.msg = "";
      this.msgErr = false;
      try {
        const { res, data } = await this.jsonFetch(
          `/api/pipelines/${this.board.pipeline.id}/tables/${table.id}/load`,
          { method: "POST" }
        );
        if (!res.ok) {
          this.showError(data, "落库被拒绝");
        } else {
          this.msg = `${data.table.name} 落库完成`;
          if (data.released && data.released.length) {
            this.msg += `；已对齐：${data.released.join("、")}`;
          }
          this.msgErr = false;
        }
        await this.afterChange();
      } finally {
        this.busy = false;
      }
    },
    async move(table, delta) {
      if (!this.board || this.busy) return;
      const tables = this.board.tables.map((t) => t.id);
      const from = tables.indexOf(table.id);
      const to = from + delta;
      if (to < 0 || to >= tables.length) return;
      tables.splice(to, 0, tables.splice(from, 1)[0]);
      await this.saveOrder(tables);
    },
    async saveOrder(tables) {
      this.busy = true;
      this.msg = "";
      this.msgErr = false;
      try {
        const { res, data } = await this.jsonFetch(
          `/api/pipelines/${this.board.pipeline.id}/order`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tables }),
          }
        );
        if (!res.ok) {
          this.showError(data, "改序被拒绝");
        } else {
          this.msg = "表顺序已按新声明顺序更新";
          this.msgErr = false;
        }
        await this.afterChange();
      } finally {
        this.busy = false;
      }
    },
    choosePolicy(value) {
      this.draftPolicy = value;
      const opt = this.board.policy.policy_options.find((o) => o.value === value);
      if (opt) this.draftNote = opt.note;
      this.dirty = true;
    },
    async savePolicy() {
      if (!this.board || this.busy) return;
      this.busy = true;
      this.msg = "";
      this.msgErr = false;
      try {
        const { res, data } = await this.jsonFetch(
          `/api/pipelines/${this.board.pipeline.id}/policy`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              policy: this.draftPolicy,
              note: this.draftNote,
            }),
          }
        );
        if (!res.ok) {
          this.showError(data, "跳表设置保存失败");
        } else if (data.policy.policy_mismatch) {
          this.msg = "说明与所选当日口径不一致：跳表策略偏了";
          this.msgErr = true;
          this.dirty = false;
        } else {
          this.msg = `跳表策略已写为 ${data.policy.policy_title}，口径锁到 ${data.policy.policy_day}`;
          this.msgErr = false;
          this.dirty = false;
        }
        await this.afterChange();
      } finally {
        this.busy = false;
      }
    },
    async addTable() {
      if (!this.board || this.busy) return;
      this.busy = true;
      this.addMsg = "";
      try {
        const { res, data } = await this.jsonFetch(
          `/api/pipelines/${this.board.pipeline.id}/tables`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: this.newName }),
          }
        );
        if (!res.ok) {
          this.addMsg = data.error || "入清单失败";
        } else {
          this.addMsg = `已入清单：${this.newName.trim()}`;
          this.newName = "";
        }
        await this.afterChange();
      } finally {
        this.busy = false;
      }
    },
    async afterChange() {
      const board = await this.jsonFetch(
        `/api/pipelines/${this.board.pipeline.id}/board`
      );
      if (board.res.ok) {
        this.current = board.data.pipeline;
        this.applyBoardKeepDraft(board.data);
      }
      const list = await this.jsonFetch("/api/pipelines");
      if (list.res.ok) this.items = list.data.items || [];
    },
    applyBoardKeepDraft(payload) {
      this.board = payload;
      // 有未保存改动时不覆盖草稿，避免用户输入被后台刷新吞掉。
      if (!this.dirty) {
        this.draftPolicy = payload.policy.skip_policy;
        this.draftNote = payload.policy.policy_note;
      }
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
