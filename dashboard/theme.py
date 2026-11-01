"""
Visual style of the dashboard, inspired by moneyincheck.org: an editorial look
with grey graph paper, a light condensed serif, one deep green accent, green
handwritten notes, objects that float and follow the mouse, text that fades in
as you scroll, and blocks that rise into view.

Chart colours were checked with a colour-blindness validator on the grey
background (green / terracotta / violet pass lightness, chroma, CVD and contrast).
"""

import html
import time

import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

# ---------------------------------------------------------------- tokens
PAPER = "#E9E9E9"      # page background (graph paper)
PANEL = "#E1E1E1"      # slightly darker panels
INK = "#111111"        # main text
MUTED = "#6B6B6B"      # secondary text
LINE = "#C9C9C9"       # hairlines
GREEN = "#00846A"      # the one accent colour

# categorical chart colours, always used in this order
TERRA, VIOLET = "#C2502E", "#6B57C9"
STATUS_COLORS = {"APPROVED": GREEN, "DECLINED": TERRA, "PENDING": VIOLET}

DISPLAY = "'Instrument Serif', 'Times New Roman', serif"
BODY = "'Newsreader', Georgia, serif"
HAND = "'Liu Jian Mao Cao', cursive"

CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Liu+Jian+Mao+Cao&family=Newsreader:ital,opsz,wght@0,6..72,300..700;1,6..72,300..500&display=swap');

/* ---------- page: grey graph paper ---------- */
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {{ background-color: {PAPER}; }}
[data-testid="stAppViewContainer"] {{
  background-image: linear-gradient(rgba(0,0,0,.055) 1px, transparent 1px),
                    linear-gradient(90deg, rgba(0,0,0,.055) 1px, transparent 1px);
  background-size: 96px 96px; background-position: -1px -1px;
}}
[data-testid="stAppViewContainer"] * {{ font-family: {BODY}; }}
header[data-testid="stHeader"] {{ background: transparent; }}
[data-testid="stToolbar"], footer, #MainMenu, .stDeployButton, [data-testid="stDecoration"] {{ display: none !important; }}
.block-container {{ padding: 2.2rem 3rem 5rem; max-width: 1320px; }}
::selection {{ background: {GREEN}; color: white; }}

/* ---------- sidebar ---------- */
section[data-testid="stSidebar"] {{ background: {PAPER}; border-right: 1px solid {LINE}; }}
section[data-testid="stSidebar"] > div {{ background: transparent; }}
.brand {{ font-family: {DISPLAY} !important; font-size: 30px; line-height: 1; color: {INK}; margin: 4px 0 6px; }}
.brand b {{ font-family: {DISPLAY} !important; font-weight: 400; color: {GREEN}; }}
.dateline {{ font-size: 14px; color: {GREEN}; margin-bottom: 26px; }}
.side-label {{ font-size: 12px; letter-spacing: .08em; text-transform: uppercase; color: {MUTED}; margin: 22px 0 6px; }}

/* sidebar navigation = a radio group restyled as a list of big serif links */
section[data-testid="stSidebar"] .nav div[role="radiogroup"] {{ gap: 2px; }}
section[data-testid="stSidebar"] div[role="radiogroup"] label > div:first-child {{ display: none; }}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label {{
  padding: 6px 0; border-bottom: 1px solid {LINE}; width: 100%; cursor: pointer;
  transition: padding-left .35s cubic-bezier(.2,.7,.2,1);
}}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label p {{
  font-family: {DISPLAY} !important; font-size: 25px; color: {MUTED}; transition: color .3s;
}}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label:hover {{ padding-left: 10px; }}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label:hover p {{ color: {INK}; }}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label:has(input:checked) p {{ color: {GREEN}; }}
section[data-testid="stSidebar"] .st-key-page div[role="radiogroup"] label:has(input:checked) {{ padding-left: 10px; }}

section[data-testid="stSidebar"] div[role="radiogroup"] label {{ background: transparent !important; }}

/* time range + rule filter = green pills */
.st-key-range div[role="radiogroup"], .st-key-rule_filter div[role="radiogroup"] {{ flex-wrap: wrap; gap: 6px; }}
.st-key-range div[role="radiogroup"] label > div:first-child,
.st-key-rule_filter div[role="radiogroup"] label > div:first-child {{ display: none; }}
.st-key-range div[role="radiogroup"] label, .st-key-rule_filter div[role="radiogroup"] label {{
  border: 1px solid {GREEN}; border-radius: 999px; padding: 3px 12px; margin: 0; cursor: pointer;
  transition: background .25s, color .25s, transform .25s;
}}
.st-key-range div[role="radiogroup"] label:hover, .st-key-rule_filter div[role="radiogroup"] label:hover {{ transform: translateY(-2px); }}
.st-key-range div[role="radiogroup"] label p, .st-key-rule_filter div[role="radiogroup"] label p {{ font-size: 14px; color: {GREEN}; }}
.st-key-range div[role="radiogroup"] label:has(input:checked),
.st-key-rule_filter div[role="radiogroup"] label:has(input:checked) {{ background: {GREEN} !important; }}
.st-key-range div[role="radiogroup"] label:has(input:checked) p,
.st-key-rule_filter div[role="radiogroup"] label:has(input:checked) p {{ color: white; }}
.side-foot {{ font-style: italic; font-size: 13px; color: {MUTED}; margin-top: 30px; line-height: 1.45; }}

/* ---------- hero (overview) ---------- */
.hero {{ position: relative; text-align: center; padding: 30px 0 10px; min-height: 470px; }}
.hero .kicker {{ font-family: {DISPLAY} !important; font-size: 30px; color: {INK}; }}
.hero h1 {{ font-family: {DISPLAY} !important; font-weight: 400; color: {GREEN}; text-transform: uppercase;
            font-size: clamp(64px, 11vw, 168px); line-height: .84; letter-spacing: -0.035em; margin: 26px 0 0; padding: 0; }}
.hero h1 span {{ display: block; }}
.float {{ position: absolute; pointer-events: none; transition: translate .9s cubic-bezier(.2,.7,.2,1); }}
.float svg {{ display: block; filter: drop-shadow(0 18px 18px rgba(0,0,0,.18)); }}
.bob {{ animation: bob 6s ease-in-out infinite; }}
.bob.slow {{ animation-duration: 8s; animation-delay: -2s; }}
@keyframes bob {{ 0%,100% {{ transform: translateY(0) rotate(var(--r,0deg)); }}
                  50% {{ transform: translateY(-14px) rotate(calc(var(--r,0deg) + 4deg)); }} }}
.scribble {{ position: absolute; font-family: {HAND} !important; color: {GREEN}; font-size: 26px;
             transform: rotate(var(--r,-6deg)); opacity: .85; white-space: nowrap; }}

/* ---------- scroll-scrubbed statement ---------- */
.statement {{ font-family: {DISPLAY} !important; font-size: clamp(30px, 4.2vw, 54px); line-height: 1.12;
              text-align: center; color: {INK}; max-width: 980px; margin: 40px auto 70px; }}
.statement .w {{ font-family: {DISPLAY} !important; opacity: .16; transition: opacity .35s ease; }}
.statement .w.on {{ opacity: 1; }}

/* ---------- page header (other pages) ---------- */
.page-head {{ position: relative; border-bottom: 1px solid {INK}; padding-bottom: 18px; margin-bottom: 8px; }}
.page-head .no {{ font-size: 15px; color: {GREEN}; }}
.page-head h1 {{ font-family: {DISPLAY} !important; font-weight: 400; font-size: clamp(54px, 8vw, 104px); line-height: .9;
                 letter-spacing: -0.03em; color: {INK}; margin: 6px 0 8px; padding: 0; }}
.page-head p {{ font-size: 18px; color: {MUTED}; font-style: italic; margin: 0; max-width: 720px; }}
.page-head .scribble {{ right: 2%; top: 30px; }}

/* ---------- sections ---------- */
.section {{ display: flex; align-items: baseline; gap: 16px; margin: 44px 0 14px; }}
.section .num {{ font-size: 14px; color: {GREEN}; }}
.section h2 {{ font-family: {DISPLAY} !important; font-weight: 400; font-size: 40px; letter-spacing: -0.01em;
               color: {INK}; margin: 0; padding: 0; line-height: 1; }}
.section .note {{ margin-left: auto; font-style: italic; font-size: 14px; color: {MUTED}; }}

/* ---------- KPI grid ---------- */
.kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0; border-top: 1px solid {INK}; }}
.kpi {{ padding: 16px 18px 20px; border-bottom: 1px solid {LINE}; border-right: 1px solid {LINE}; position: relative;
        transition: background .35s, transform .35s cubic-bezier(.2,.7,.2,1); background: rgba(255,255,255,.0); }}
.kpi:last-child {{ border-right: none; }}
.kpi:hover {{ background: rgba(255,255,255,.45); transform: translateY(-3px); }}
.kpi::after {{ content: ""; position: absolute; left: 0; bottom: -1px; height: 2px; width: 0; background: {GREEN};
               transition: width .5s cubic-bezier(.2,.7,.2,1); }}
.kpi:hover::after {{ width: 100%; }}
.kpi .label {{ font-size: 15px; color: {MUTED}; }}
.kpi .value {{ font-family: {DISPLAY} !important; font-size: 52px; line-height: 1; color: {INK}; margin-top: 12px; letter-spacing: -0.02em; }}
.kpi .value * {{ font-family: {DISPLAY} !important; }}
.kpi .value small {{ font-size: 22px; color: {MUTED}; margin-left: 4px; }}
.kpi .sub {{ font-size: 13.5px; font-style: italic; color: {MUTED}; margin-top: 8px; }}
.kpi.accent .value {{ color: {GREEN}; }}

/* status pill: icon + text, never colour alone */
.pill {{ display: inline-flex; align-items: center; gap: 8px; font-size: 14px; padding: 4px 12px; border-radius: 999px;
         border: 1px solid currentColor; }}
.pill.ok {{ color: {GREEN}; }} .pill.warn {{ color: #9A5B00; }} .pill.bad {{ color: #B3261E; }}

/* chart cards */
/* only bordered containers show a border, so these rules only change the chart cards */
[data-testid="stVerticalBlockBorderWrapper"] {{ border-radius: 14px !important; border-color: {LINE} !important; }}
.card-title {{ font-family: {DISPLAY} !important; font-size: 24px; color: {INK}; line-height: 1.1; }}
.card-sub {{ font-size: 14px; font-style: italic; color: {MUTED}; margin: 2px 0 6px; }}

/* editorial tables (like the price list on the site) */
table.ed {{ width: 100%; border-collapse: collapse; background: transparent; border: none; }}
table.ed th, table.ed td {{ border: none !important; background: transparent; }}
table.ed th {{ text-align: left; font-weight: 400; font-size: 13px; color: {MUTED}; padding: 6px 8px; border-bottom: 1px solid {INK} !important; }}
table.ed td {{ font-size: 16px; padding: 9px 8px; border-bottom: 1px solid {LINE} !important; color: {INK}; }}
table.ed td.r, table.ed th.r {{ text-align: right; font-variant-numeric: tabular-nums; }}
table.ed tr {{ transition: background .25s; }}
table.ed tbody tr:hover {{ background: rgba(0,132,106,.07); }}
.tag {{ display: inline-block; font-size: 12.5px; padding: 1px 10px; border-radius: 999px; border: 1px solid currentColor; }}

/* rule cards (risk page) */
.rule {{ border: 1px solid {LINE}; border-radius: 14px; padding: 18px 20px; height: 100%; background: rgba(255,255,255,.28);
         transition: transform .4s cubic-bezier(.2,.7,.2,1), background .3s; }}
.rule:hover {{ transform: translateY(-4px) rotate(-.4deg); background: rgba(255,255,255,.55); }}
.rule .name {{ font-family: {DISPLAY} !important; font-size: 26px; }}
.rule .count {{ font-family: {DISPLAY} !important; font-size: 68px; line-height: 1; color: {GREEN}; margin: 8px 0; }}
.rule .def {{ color: {MUTED}; font-size: 15px; font-style: italic; }}
.notice {{ border-radius: 999px; background: {GREEN}; color: white; padding: 10px 22px; font-size: 15px; margin: 18px 0 6px;
           display: inline-block; }}

/* ---------- motion: blocks rise into view (classes set by the script below) ---------- */
.reveal {{ opacity: 0; transform: translateY(28px); }}
.reveal.in {{ opacity: 1; transform: none; transition: opacity .9s ease, transform .9s cubic-bezier(.2,.7,.2,1); }}
.reveal.in.instant {{ transition: none; }}
[data-testid="stPlotlyChart"] {{ animation: rise .9s cubic-bezier(.2,.7,.2,1) both; }}
@keyframes rise {{ from {{ opacity: 0; transform: translateY(24px); }} to {{ opacity: 1; transform: none; }} }}
@media (prefers-reduced-motion: reduce) {{
  .reveal {{ opacity: 1; transform: none; }} .bob, [data-testid="stPlotlyChart"] {{ animation: none; }}
  .statement .w {{ opacity: 1; }}
}}

/* small screens */
@media (max-width: 760px) {{
  .kpis {{ grid-template-columns: repeat(2, 1fr) !important; }}
  .block-container {{ padding: 1.2rem 1rem 3rem; }}
  .float, .scribble {{ display: none; }}
  .hero {{ min-height: 0; }}
}}
</style>
"""

# Runs once in the parent page (the component iframe is same-origin).
# 1. reveal: .reveal blocks fade/rise in when scrolled into view (only the first time,
#    so the 5-second refresh doesn't replay the animation) and numbers count up.
# 2. scrub: words of .statement light up one by one as you scroll.
# 3. parallax: .float objects follow the mouse and drift with scrolling.
MOTION_JS = """
<script>
(function () {
  const P = window.parent, D = P.document;
  if (P.__txnMotion) return;            // install only once
  P.__txnMotion = true;
  const seen = P.__txnSeen = P.__txnSeen || new Set();
  const reduce = P.matchMedia('(prefers-reduced-motion: reduce)').matches;

  function countUp(el) {
    const to = parseFloat(el.dataset.to), dec = parseInt(el.dataset.dec || '0');
    if (isNaN(to) || reduce) return;
    const t0 = performance.now(), dur = 1200;
    function step(t) {
      const k = Math.min(1, (t - t0) / dur), e = 1 - Math.pow(1 - k, 3);
      el.textContent = (to * e).toLocaleString('en-US', {minimumFractionDigits: dec, maximumFractionDigits: dec});
      if (k < 1) P.requestAnimationFrame(step);
    }
    P.requestAnimationFrame(step);
  }

  const io = new P.IntersectionObserver((entries) => {
    entries.forEach((en) => {
      if (!en.isIntersecting) return;
      const el = en.target;
      el.classList.add('in');
      seen.add(el.dataset.r);
      el.querySelectorAll('[data-to]').forEach(countUp);
      io.unobserve(el);
    });
  }, {threshold: 0.12});

  function scan() {
    D.querySelectorAll('.reveal:not([data-bound])').forEach((el) => {
      el.dataset.bound = '1';
      if (reduce || seen.has(el.dataset.r)) el.classList.add('in', 'instant');
      else io.observe(el);
    });
    D.querySelectorAll('.statement:not([data-split])').forEach((el) => {
      el.dataset.split = '1';
      el.innerHTML = el.textContent.trim().split(/\\s+/).map(w => '<span class="w">' + w + '</span>').join(' ');
    });
    scrub();
  }

  function scrub() {
    const vh = P.innerHeight;
    D.querySelectorAll('.statement').forEach((el) => {
      const r = el.getBoundingClientRect();
      const p = Math.max(0, Math.min(1, (vh * 0.85 - r.top) / (r.height + vh * 0.35)));
      const words = el.querySelectorAll('.w');
      words.forEach((w, i) => w.classList.toggle('on', reduce || i < p * words.length));
    });
    D.querySelectorAll('.float').forEach((el) => {
      const depth = parseFloat(el.dataset.depth || '1');
      const r = el.parentElement.getBoundingClientRect();
      el.dataset.sy = (-r.top * 0.12 * depth).toFixed(1);
      move(el);
    });
  }

  let mx = 0, my = 0;
  function move(el) {
    const depth = parseFloat(el.dataset.depth || '1');
    el.style.translate = (mx * 22 * depth).toFixed(1) + 'px ' + (my * 18 * depth + parseFloat(el.dataset.sy || 0)).toFixed(1) + 'px';
  }
  D.addEventListener('mousemove', (e) => {
    if (reduce) return;
    mx = e.clientX / P.innerWidth - 0.5; my = e.clientY / P.innerHeight - 0.5;
    D.querySelectorAll('.float').forEach(move);
  });

  let ticking = false;
  D.addEventListener('scroll', () => {
    if (ticking) return; ticking = true;
    P.requestAnimationFrame(() => { scrub(); ticking = false; });
  }, true);

  new P.MutationObserver(() => scan()).observe(D.body, {childList: true, subtree: true});
  scan();
})();
</script>
"""

# Hand-drawn style objects for the hero (instead of the site's chess piece and coins)
COIN = """<svg width="118" height="118" viewBox="0 0 120 120"><circle cx="60" cy="60" r="56" fill="#cfcfcb" stroke="#555" stroke-width="2"/>
<circle cx="60" cy="60" r="46" fill="none" stroke="#666" stroke-width="1.5" stroke-dasharray="3 4"/>
<text x="60" y="78" text-anchor="middle" font-family="Instrument Serif, serif" font-size="56" fill="#333">€</text></svg>"""
CARD = f"""<svg width="210" height="134" viewBox="0 0 210 134"><rect x="2" y="2" width="206" height="130" rx="14" fill="{GREEN}"/>
<rect x="22" y="38" width="38" height="28" rx="5" fill="#d9c37a" stroke="#8a7a3c"/>
<path d="M22 52h38M41 38v28" stroke="#8a7a3c"/>
<text x="22" y="98" font-family="Newsreader, serif" font-size="15" letter-spacing="3" fill="white">4242 •••• •••• 0001</text>
<text x="22" y="118" font-family="Newsreader, serif" font-size="11" fill="#d7efe9">SYNTHETIC · DEMO</text>
<circle cx="168" cy="108" r="12" fill="#fff" opacity=".45"/><circle cx="182" cy="108" r="12" fill="#fff" opacity=".3"/></svg>"""
RECEIPT = """<svg width="110" height="150" viewBox="0 0 110 150"><path d="M5 5h100v132l-10-7-10 7-10-7-10 7-10-7-10 7-10-7-10 7-10-7z" fill="#f6f6f2" stroke="#666" stroke-width="1.5"/>
<g stroke="#999" stroke-width="2"><path d="M18 28h60"/><path d="M18 44h74"/><path d="M18 60h50"/><path d="M18 76h66"/></g>
<path d="M18 100h74" stroke="#333" stroke-width="1"/><text x="18" y="118" font-family="Newsreader, serif" font-size="11" fill="#111">TOTAL</text>
<text x="94" y="118" text-anchor="end" font-family="Newsreader, serif" font-size="11" fill="#111">€42</text></svg>"""


def inject():
    """Add the CSS and the motion script to the page (call once per run)."""
    st.markdown(CSS, unsafe_allow_html=True)
    components.html(MOTION_JS, height=0)


def esc(text):
    """Escape text so it is safe to put inside HTML."""
    return html.escape(str(text))


def reveal(inner, key, extra_class=""):
    """Wrap HTML so it rises into view the first time it is scrolled to."""
    return f'<div class="reveal {extra_class}" data-r="{esc(key)}">{inner}</div>'


def num(value, decimals=0):
    """A number that counts up when it first appears."""
    v = float(value or 0)
    return f'<span data-to="{v}" data-dec="{decimals}">{v:,.{decimals}f}</span>'


# ---------------------------------------------------------------- building blocks
def sidebar_brand(dateline=None):
    """Project name and a date line (the current time, or the snapshot time in demo mode)."""
    dateline = dateline or time.strftime("%B %d, %Y, %H:%M") + " UTC"
    st.markdown(f'<div class="brand">Transactions <b>in&nbsp;Motion.</b></div>'
                f'<div class="dateline">{esc(dateline)}</div>', unsafe_allow_html=True)


def side_label(text):
    """Small uppercase label above a sidebar group."""
    st.markdown(f'<div class="side-label">{esc(text)}</div>', unsafe_allow_html=True)


def hero():
    """Big animated title of the Overview page, with floating objects and notes."""
    st.markdown(f"""
    <div class="hero">
      <div class="kicker">The Real-Time Transaction Data Platform</div>
      <h1><span>Payments</span><span>in motion</span></h1>
      <div class="float" data-depth="1.6" style="left:4%; top:8%"><div class="bob" style="--r:-12deg">{COIN}</div></div>
      <div class="float" data-depth="2.4" style="left:50%; top:46%; margin-left:-105px"><div class="bob slow" style="--r:-7deg">{CARD}</div></div>
      <div class="float" data-depth="1.1" style="right:3%; top:52%"><div class="bob" style="--r:8deg">{RECEIPT}</div></div>
      <div class="scribble" style="left:14%; top:4%; --r:-8deg">Kafka →</div>
      <div class="scribble" style="right:12%; top:20%; --r:6deg">Spark · 5s</div>
      <div class="scribble" style="left:8%; bottom:6%; --r:-4deg">watermark 10′</div>
      <div class="scribble" style="right:20%; bottom:2%; --r:-10deg">dedup ✓</div>
      <div class="scribble" style="left:56%; top:9%; --r:12deg">€ → dbt</div>
    </div>""", unsafe_allow_html=True)


def statement(text):
    """Big editorial sentence whose words light up as you scroll."""
    st.markdown(f'<p class="statement">{esc(text)}</p>', unsafe_allow_html=True)


def page_head(no, title, subtitle, scribble=""):
    """Large title at the top of every page except Overview."""
    s = f'<div class="scribble" style="--r:-6deg">{esc(scribble)}</div>' if scribble else ""
    st.markdown(reveal(f'<div class="page-head"><div class="no">No. {esc(no)}</div><h1>{esc(title)}</h1>'
                       f'<p>{esc(subtitle)}</p>{s}</div>', f"head-{no}"), unsafe_allow_html=True)


def section(no, title, note=""):
    """Numbered section heading, e.g. 'ii. Pipeline at a glance'."""
    st.markdown(reveal(f'<div class="section"><span class="num">{esc(no)}</span><h2>{esc(title)}</h2>'
                       f'<span class="note">{esc(note)}</span></div>', f"sec-{no}"), unsafe_allow_html=True)


def kpi_grid(items, key, cols=4):
    """items: list of (label, value_html, sub, accent?)"""
    cells = []
    for item in items:
        label, value, sub = item[:3]
        accent = " accent" if len(item) > 3 and item[3] else ""
        cells.append(f'<div class="kpi{accent}"><div class="label">{esc(label)}</div>'
                     f'<div class="value">{value}</div><div class="sub">{esc(sub)}</div></div>')
    grid = f'<div class="kpis" style="grid-template-columns: repeat({int(cols)}, 1fr)">{"".join(cells)}</div>'
    st.markdown(reveal(grid, key), unsafe_allow_html=True)


def pill(state, text):
    """Rounded status badge with an icon and text (never colour alone)."""
    icon = {"ok": "●", "warn": "▲", "bad": "■"}[state]
    return f'<span class="pill {state}">{icon}&nbsp; {esc(text)}</span>'


def card_title(title, sub=""):
    """Title and optional subtitle inside a chart card."""
    st.markdown(f'<div class="card-title">{esc(title)}</div>' + (f'<div class="card-sub">{esc(sub)}</div>' if sub else ""),
                unsafe_allow_html=True)


def table(df, columns, key):
    """Editorial table. columns: list of (field, label, kind) with kind in text/int/eur/num/tag."""
    head = "".join(f'<th class="{"r" if kind in ("int", "eur", "num") else ""}">{esc(label)}</th>'
                   for _, label, kind in columns)
    rows = []
    for _, row in df.iterrows():
        cells = []
        for field, _, kind in columns:
            v = row[field]
            if kind == "int":
                cells.append(f'<td class="r">{int(v or 0):,}</td>')
            elif kind == "eur":
                cells.append(f'<td class="r">€ {float(v or 0):,.2f}</td>')
            elif kind == "num":
                cells.append(f'<td class="r">{float(v or 0):,.4f}</td>')
            elif kind == "tag":
                color = STATUS_COLORS.get(str(v), GREEN)
                cells.append(f'<td><span class="tag" style="color:{color}">{esc(v)}</span></td>')
            else:
                cells.append(f"<td>{esc(v)}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    st.markdown(reveal(f'<table class="ed"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>', key),
                unsafe_allow_html=True)


# ---------------------------------------------------------------- charts
def rgba(hex_color, alpha):
    """Turn '#RRGGBB' into an rgba() colour with transparency."""
    r, g, b = (int(hex_color[i:i + 2], 16) for i in (1, 3, 5))
    return f"rgba({r},{g},{b},{alpha})"


def style(fig, height=280, legend=True, hover="x unified"):
    """Apply the dashboard look to a plotly figure: thin lines, quiet grey axes."""
    fig.update_layout(
        height=height, margin=dict(l=4, r=4, t=30 if legend else 8, b=4),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=BODY, size=12, color=MUTED),
        showlegend=legend, legend=dict(orientation="h", x=0, y=1.13, font=dict(color=INK, size=13), title=None,
                                       traceorder="normal"),
        hovermode=hover, bargap=0.3, barcornerradius=4,
        hoverlabel=dict(bgcolor=INK, bordercolor=INK, font=dict(family=BODY, color="white", size=13)),
        transition=dict(duration=500, easing="cubic-in-out"),
    )
    fig.update_xaxes(showgrid=False, linecolor=INK, tickcolor=LINE, ticks="outside", title=None)
    fig.update_yaxes(gridcolor="rgba(0,0,0,.09)", zeroline=False, showline=False, title=None, rangemode="tozero")
    return fig


def show(fig, key):
    """Draw a plotly figure without the plotly toolbar."""
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)


def line(x, series, height=260, area=False):
    """series: list of (name, values, colour)"""
    fig = go.Figure()
    for name, y, color in series:
        fig.add_trace(go.Scatter(x=x, y=y, name=name, mode="lines", line=dict(color=color, width=2, shape="spline", smoothing=0.6),
                                 fill="tozeroy" if area else None, fillcolor=rgba(color, 0.12) if area else None))
    return style(fig, height, legend=len(series) > 1)


def hbar(labels, values, color=GREEN, height=220, text=None):
    """Horizontal bar chart with the value written next to each bar."""
    fig = go.Figure(go.Bar(y=labels, x=values, orientation="h", marker=dict(color=color),
                           text=text, textposition="outside", textfont=dict(family=BODY, color=INK, size=13),
                           cliponaxis=False, hovertemplate="%{y}: %{x:,}<extra></extra>"))
    fig = style(fig, height, legend=False, hover="closest")
    fig.update_yaxes(autorange="reversed", showgrid=False)
    # leave room on the right for the outside labels
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,.09)", showticklabels=False, ticks="", showline=False,
                     range=[0, max(float(v) for v in values) * 1.35 if len(values) else 1])
    return fig
