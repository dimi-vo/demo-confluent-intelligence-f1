// F1 Pit Wall workshop — browser walkthrough.
// Every <article class="page"> is one step; the URL hash picks which one is shown.
// Works straight from file:// — no server, no build, no dependencies.

(() => {
  const $ = (sel, el = document) => el.querySelector(sel);
  const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];

  const store = {
    get(key, fallback) {
      try {
        const v = localStorage.getItem("f1pw:" + key);
        return v === null ? fallback : JSON.parse(v);
      } catch {
        return fallback;
      }
    },
    set(key, value) {
      try { localStorage.setItem("f1pw:" + key, JSON.stringify(value)); } catch { /* private mode */ }
    },
  };

  const svg = (d, extra = "") =>
    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" ${extra}>${d}</svg>`;
  const ICON = {
    menu: svg('<path d="M4 7h16M4 12h16M4 17h10"/>'),
    sun: svg('<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>'),
    moon: svg('<path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z"/>'),
    copy: svg('<rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h8"/>'),
    check: svg('<path d="M5 12.5l4.5 4.5L19 7.5"/>', 'stroke-width="2.4"'),
    arrow: svg('<path d="M5 12h14M13 6l6 6-6 6"/>'),
    help: svg('<circle cx="12" cy="12" r="9"/><path d="M9.5 9.5a2.5 2.5 0 1 1 3.5 2.3c-.6.3-1 .9-1 1.6v.6M12 17h.01"/>'),
    info: svg('<circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01"/>'),
    image: svg('<rect x="3" y="5" width="18" height="14" rx="2"/><circle cx="9" cy="10" r="1.6"/><path d="M21 16l-5-5-8 8"/>'),
    fix: svg('<path d="M14.5 6.5a4 4 0 0 0-5.3 5.3L4 17l3 3 5.2-5.2a4 4 0 0 0 5.3-5.3l-2.5 2.5-2.5-2.5 2.5-2.5Z"/>'),
    bulb: svg('<path d="M9 18h6M10 21h4M12 3a6 6 0 0 0-3.5 10.9c.6.5 1 1.2 1 2.1h5c0-.9.4-1.6 1-2.1A6 6 0 0 0 12 3Z"/>'),
    warn: svg('<path d="M12 3 2 20h20L12 3Z"/><path d="M12 10v4M12 17h.01"/>'),
  };

  // ── Theme ────────────────────────────────────────────────
  const themeBtn = $("#theme-btn");
  const paintThemeBtn = () => {
    const dark = document.documentElement.dataset.theme === "dark";
    themeBtn.innerHTML = dark ? ICON.sun : ICON.moon;
    themeBtn.setAttribute("aria-label", dark ? "Use light theme" : "Use dark theme");
  };
  themeBtn.addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    store.set("theme", next);
    paintThemeBtn();
  });
  paintThemeBtn();
  $("#menu-btn").innerHTML = ICON.menu;

  // ── Syntax colouring (deliberately tiny) ────────────────
  const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const tok = (cls, s) => `<span class="tok-${cls}">${esc(s)}</span>`;

  const SQL_KEYWORDS = new Set(
    ("SELECT FROM WHERE JOIN ON AS WITH CREATE TABLE AGENT USING MODEL PROMPT CASE WHEN THEN ELSE END AND OR NOT " +
      "IS FOR SYSTEM_TIME OF OVER PARTITION BY ORDER RANGE BETWEEN UNBOUNDED PRECEDING CURRENT ROW GROUP LATERAL " +
      "CAST SHOW TABLES MODELS AGENTS LIMIT VALUE TRUE FALSE NULL INTERVAL SECOND DESCRIPTOR INT STRING MAP").split(" "),
  );
  const highlightSQL = (src) => {
    const re = /(\/\*[\s\S]*?\*\/|--[^\n]*)|('(?:[^']|'')*')|(`[^`]*`)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)/g;
    let out = "";
    let last = 0;
    for (let m; (m = re.exec(src)); last = re.lastIndex) {
      out += esc(src.slice(last, m.index));
      const [whole, comment, str, ident, num, word] = m;
      if (comment) out += tok("c", comment);
      else if (str) out += tok("s", str);
      else if (ident) out += tok("i", ident);
      else if (num) out += tok("n", num);
      else if (SQL_KEYWORDS.has(word.toUpperCase()) && (word === word.toUpperCase() || /^(true|false|null)$/.test(word))) out += tok("k", word);
      else if (/^[A-Z][A-Z0-9_]*$/.test(word) && src[re.lastIndex] === "(") out += tok("f", word);
      else out += esc(whole);
    }
    return out + esc(src.slice(last));
  };
  const highlightShell = (src) =>
    src
      .split("\n")
      .map((line) =>
        /^\s*#/.test(line)
          ? tok("c", line)
          : esc(line)
              .replace(/^(\s*)([\w.-]+)/, '$1<span class="tok-cmd">$2</span>')
              .replace(/(\s)(--?[a-z][\w-]*)/g, '$1<span class="tok-flag">$2</span>'),
      )
      .join("\n");

  const WHERE = { sql: "SQL workspace", bash: "Terminal", powershell: "PowerShell", text: "" };

  const copyText = async (text) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      const ta = Object.assign(document.createElement("textarea"), { value: text });
      ta.style.cssText = "position:fixed;opacity:0";
      document.body.append(ta);
      ta.select();
      document.execCommand("copy");
      ta.remove();
    }
  };

  // ── Code blocks: colour, copy, collapse ─────────────────
  for (const block of $$(".code")) {
    const code = $("code", block);
    const raw = code.textContent.replace(/^\n+/, "").replace(/\s+$/, "");
    const lang = block.dataset.lang || "text";
    block.classList.add(lang);
    code.innerHTML = lang === "sql" ? highlightSQL(raw) : lang === "text" ? esc(raw) : highlightShell(raw);

    const head = document.createElement("div");
    head.className = "code-head";
    head.innerHTML = `<span class="code-where">${block.dataset.where ?? WHERE[lang] ?? ""}</span>`;
    const btn = document.createElement("button");
    btn.className = "copy";
    btn.type = "button";
    btn.innerHTML = `${ICON.copy}<span>Copy</span>`;
    btn.addEventListener("click", async () => {
      await copyText(raw);
      btn.classList.add("copied");
      btn.innerHTML = `${ICON.check}<span>Copied</span>`;
      setTimeout(() => {
        btn.classList.remove("copied");
        btn.innerHTML = `${ICON.copy}<span>Copy</span>`;
      }, 1600);
    });
    head.append(btn);
    block.prepend(head);

    const lines = raw.split("\n").length;
    if (lines > 14) {
      block.classList.add("collapsed");
      const more = document.createElement("button");
      more.className = "code-expand";
      more.type = "button";
      more.textContent = `Show all ${lines} lines`;
      more.addEventListener("click", () => {
        const collapsed = block.classList.toggle("collapsed");
        more.textContent = collapsed ? `Show all ${lines} lines` : "Show less";
      });
      block.append(more);
    }
  }

  // ── Toggle icons ────────────────────────────────────────
  for (const d of $$("details.more")) {
    const kind = ["help", "fix", "image", "bulb"].find((k) => d.classList.contains(k)) || "info";
    $("summary", d).insertAdjacentHTML("afterbegin", `<span class="ico">${ICON[kind]}</span>`);
  }
  for (const c of $$(".callout")) c.insertAdjacentHTML("afterbegin", c.classList.contains("warn") ? ICON.warn : ICON.info);

  // ── Tabs (choice remembered across the walkthrough) ─────
  const pickTab = (group, value) => {
    for (const tabs of $$(`.tabs[data-tabs="${group}"]`)) {
      for (const b of $$("[role=tab]", tabs)) b.setAttribute("aria-selected", String(b.dataset.tab === value));
      for (const p of $$(".tab-panel", tabs)) p.hidden = p.dataset.tab !== value;
    }
    store.set("tab:" + group, value);
  };
  for (const tabs of $$(".tabs")) {
    const group = tabs.dataset.tabs;
    for (const b of $$("[role=tab]", tabs)) b.addEventListener("click", () => pickTab(group, b.dataset.tab));
  }
  pickTab("os", store.get("tab:os", /Win/.test(navigator.userAgent) ? "windows" : "mac"));

  // ── Pages, contents, progress ───────────────────────────
  const pages = $$(".page");
  const toc = $("#toc");
  const done = new Set(store.get("done", []));

  let group = null;
  let groupEl = null;
  for (const page of pages) {
    if (page.dataset.group !== group) {
      group = page.dataset.group;
      groupEl = document.createElement("div");
      groupEl.className = "toc-group";
      groupEl.innerHTML = `<p class="toc-label">${group}` +
        (page.dataset.optional !== undefined ? ' <span class="tag">· optional</span>' : "") + "</p>";
      toc.append(groupEl);
    }
    const a = document.createElement("a");
    a.href = "#" + page.id;
    a.dataset.page = page.id;
    a.innerHTML = `<span class="dot">${ICON.check}</span><span>${page.dataset.title}</span>`;
    groupEl.append(a);

    const eyebrow = document.createElement("p");
    eyebrow.className = "eyebrow";
    eyebrow.innerHTML = group + (page.dataset.optional !== undefined ? ' <span class="badge">Optional</span>' : "");
    page.prepend(eyebrow);
  }

  const paintDone = () => {
    for (const a of $$("a[data-page]", toc)) a.classList.toggle("done", done.has(a.dataset.page));
    for (const b of $$(".mark-done")) {
      const on = done.has(b.closest(".page").id);
      b.setAttribute("aria-pressed", String(on));
      b.innerHTML = on ? `${ICON.check}Done` : "Mark as done";
    }
  };
  for (const cp of $$(".checkpoint")) {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "mark-done";
    b.addEventListener("click", () => {
      const id = cp.closest(".page").id;
      done.has(id) ? done.delete(id) : done.add(id);
      store.set("done", [...done]);
      paintDone();
    });
    cp.append(b);
  }
  paintDone();

  const pager = $("#pager");
  let current = -1;
  const show = (id) => {
    // The hash names a page, or an anchor inside one (e.g. #teardown on the last page).
    const anchor = id ? document.getElementById(id) : null;
    const idx = Math.max(0, pages.indexOf(anchor?.closest(".page")));
    const page = pages[idx];
    const changed = idx !== current;
    current = idx;
    for (const p of pages) p.classList.toggle("active", p === page);
    for (const a of $$("a[data-page]", toc)) {
      if (a.dataset.page === page.id) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    }
    document.title = `${page.dataset.title} · F1 Pit Wall workshop`;
    $("#step-count").textContent = `${idx + 1} of ${pages.length}`;
    $("#progress-bar").style.width = `${(idx / (pages.length - 1)) * 100}%`;

    const prev = pages[idx - 1];
    const next = pages[idx + 1];
    pager.innerHTML =
      (prev ? `<a class="prev" href="#${prev.id}"><span class="dir">← Previous</span><span class="t">${prev.dataset.title}</span></a>` : "") +
      (next ? `<a class="next" href="#${next.id}"><span class="dir">Next →</span><span class="t">${next.dataset.title}</span></a>` : "") +
      '<p class="pager-hint">Tip: use the ← and → keys to move between steps</p>';

    document.body.classList.remove("toc-open");
    if (anchor && anchor !== page) anchor.scrollIntoView({ behavior: changed ? "auto" : "smooth" });
    else if (changed) window.scrollTo({ top: 0 });
  };
  window.addEventListener("hashchange", () => show(location.hash.slice(1)));
  show(location.hash.slice(1));
  // On first load the browser jumps to the #page article after we've scrolled; undo that.
  window.addEventListener("load", () => {
    if (pages.some((p) => p.id === location.hash.slice(1))) window.scrollTo({ top: 0 });
  });

  // ── Drawer, keyboard, scroll shadow ─────────────────────
  $("#menu-btn").addEventListener("click", () => document.body.classList.toggle("toc-open"));
  $(".scrim").addEventListener("click", () => document.body.classList.remove("toc-open"));

  document.addEventListener("keydown", (e) => {
    if (e.metaKey || e.ctrlKey || e.altKey || e.target.closest("input, textarea, [contenteditable]")) return;
    if (e.key === "Escape") {
      document.body.classList.remove("toc-open");
      lightbox.classList.remove("open");
    }
    const target = e.key === "ArrowRight" ? pages[current + 1] : e.key === "ArrowLeft" ? pages[current - 1] : null;
    if (target) location.hash = target.id;
  });

  const topbar = $(".topbar");
  const onScroll = () => topbar.classList.toggle("scrolled", window.scrollY > 4);
  window.addEventListener("scroll", onScroll, { passive: true });
  onScroll();

  // ── Click a screenshot to see it full size ──────────────
  const lightbox = document.createElement("div");
  lightbox.className = "lightbox";
  lightbox.innerHTML = "<img alt=''>";
  document.body.append(lightbox);
  lightbox.addEventListener("click", () => lightbox.classList.remove("open"));
  for (const img of $$("figure img")) {
    img.addEventListener("click", () => {
      $("img", lightbox).src = img.src;
      $("img", lightbox).alt = img.alt;
      lightbox.classList.add("open");
    });
  }
})();
