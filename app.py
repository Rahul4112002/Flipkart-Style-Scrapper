"""
Flipkart PDP Scraper — FastAPI + Playwright
============================================
⚠  Educational / personal-use only.
"""
from __future__ import annotations
import asyncio, json, logging, os, random, re, sys, threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from playwright.async_api import async_playwright

app = FastAPI(title="Flipkart PDP Scraper")
templates = Jinja2Templates(directory="templates")
DOWNLOADS_DIR = Path("downloads"); DOWNLOADS_DIR.mkdir(exist_ok=True)
UPLOADS_DIR = Path("uploads"); UPLOADS_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flipkart_pdp_scraper")

scraper_state: dict[str, Any] = {
    "running": False, "stop_event": threading.Event(), "logs": [],
    "products": [], "total_items": 0, "scraped_count": 0,
    "failed_count": 0, "output_file": None, "task": None,
}
MAX_TABS = 4

def _ts(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def _log(msg):
    scraper_state["logs"].append(f"{_ts()} | {msg}")
    logger.info(msg)

def _build_url(sid):
    sid = sid.strip()
    if sid.startswith("http"): return sid
    if sid.startswith("www."): return "https://" + sid
    return f"https://www.flipkart.com/product/p/itm?pid={sid}"

EXTRACT_JS = r"""
() => {
    const r = {brand:'',fsn:'',title:'',category:'',size:'',colour:'',
        mrp:'',current_price:'',discount:'',rating:'',review_count:'',rating_count:'',reviews:[]};

    // LD+JSON
    for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
        try {
            let ld = JSON.parse(s.textContent||'');
            if (Array.isArray(ld)) ld = ld[0];
            if (ld['@type']==='Product') {
                r.title=ld.name||''; r.fsn=ld.sku||''; r.colour=ld.color||'';
                if(ld.brand) r.brand=ld.brand.name||ld.brand||'';
                if(ld.category) r.category=String(ld.category).replace(/_/g,' ');
                const o=ld.offers||{};
                if(typeof o==='object'&&!Array.isArray(o)) r.current_price=String(o.price||o.lowPrice||'');
                else if(Array.isArray(o)&&o.length) r.current_price=String(o[0].price||'');
                const a=ld.aggregateRating||{};
                if(a.ratingValue) r.rating=String(a.ratingValue);
                if(a.ratingCount) r.rating_count=String(a.ratingCount);
                if(a.reviewCount) r.review_count=String(a.reviewCount);
                if(Array.isArray(ld.review)) {
                    for(const rv of ld.review) {
                        const nm=(rv.author&&rv.author.name)?rv.author.name:'';
                        const st=(rv.reviewRating&&rv.reviewRating.ratingValue)?String(rv.reviewRating.ratingValue):'';
                        const ct=rv.reviewBody||'';
                        if(ct) r.reviews.push('★'+st+' '+nm+': '+ct);
                    }
                }
            }
            if(ld['@type']==='BreadcrumbList') {
                const items=ld.itemListElement||[];
                const cats=items.sort((a,b)=>(a.position||0)-(b.position||0))
                    .map(i=>(i.item&&i.item.name)?i.item.name:(i.name||'')).filter(Boolean);
                if(cats.length) r.category=cats.join(' > ');
            }
        } catch(e){}
    }

    // Embedded scripts for current_price, fsn, size (NOT mrp — DOM is more accurate for mrp)
    for (const s of document.querySelectorAll('script')) {
        const t=s.textContent||''; if(t.length<100) continue;
        if(!r.current_price){
            const m=t.match(/"sellingPrice"\s*:\s*(\d+)/)||t.match(/"finalPrice"\s*:\s*(\d+)/);
            if(m) r.current_price=m[1];
        }
        if(!r.fsn){ const m=t.match(/"productId"\s*:\s*"([^"]+)"/); if(m) r.fsn=m[1]; }
        if(!r.size){ const m=t.match(/"selectedSize"\s*:\s*"([^"]+)"/); if(m) r.size=m[1]; }
    }

    // DOM extraction (priority for MRP — visible strikethrough price)
    if(!r.brand){const e=document.querySelector('span.mEh187,span._2J4LW7');if(e)r.brand=e.textContent.trim();}
    if(!r.title){const e=document.querySelector('span.VU-ZEz,span.B_NuCI,h1 span');if(e)r.title=e.textContent.trim();}
    if(!r.current_price){const e=document.querySelector('div.Nx9bqj.CxhGGd,div._30jeq3');if(e)r.current_price=e.textContent.replace(/[^\d]/g,'');}

    // MRP: strikethrough price from DOM (this is the VISIBLE "was" price like ₹2,999)
    if(!r.mrp){
        const mrpSels=['div.yRaY8j','div._3I9_wc','span.yRaY8j','div.Yb_OL'];
        for(const sel of mrpSels){
            const e=document.querySelector(sel);
            if(e){const v=e.textContent.replace(/[^\d]/g,''); if(v&&v!==r.current_price){r.mrp=v;break;}}
        }
    }
    // MRP fallback: find element with CSS line-through (strikethrough styling)
    if(!r.mrp){
        const all=document.querySelectorAll('span,div');
        for(const el of all){
            const cs=window.getComputedStyle(el);
            if(cs.textDecorationLine&&cs.textDecorationLine.includes('line-through')){
                const v=el.textContent.replace(/[^\d]/g,'');
                if(v&&v.length>=2&&v!==r.current_price){r.mrp=v;break;}
            }
        }
    }
    // MRP last resort: embedded script "mrp" field (may differ from displayed price)
    if(!r.mrp){
        for(const s of document.querySelectorAll('script')){
            const t=s.textContent||''; if(t.length<100) continue;
            const m=t.match(/"mrp"\s*:\s*(\d+)/)||t.match(/"maximumRetailPrice"\s*:\s*(\d+)/)
                ||t.match(/"basePrice"\s*:\s*(\d+)/)||t.match(/"marked"\s*:\s*(\d+)/)
                ||t.match(/"listingPrice"\s*:\s*(\d+)/);
            if(m){r.mrp=m[1];break;}
        }
    }

    // Discount from DOM first (e.g. "↓77%" or "77% off")
    if(!r.discount){
        const dEls=document.querySelectorAll('div.UkUFwK span,div._3Ay6Sb span,span.UkUFwK,div.UkUFwK');
        for(const e of dEls){
            const t=e.textContent.trim();
            if(t.match(/\d+%\s*off/i)||t.match(/↓\d+%/)){r.discount=t;break;}
        }
    }

    if(!r.rating){const e=document.querySelector('div.XQDdHH,div._3LWZlK');if(e)r.rating=e.textContent.trim();}
    if(!r.rating_count||!r.review_count){
        const e=document.querySelector('span.Wphh3N,span._2_R_DZ');
        if(e){const t=e.textContent;
            const rm=t.match(/([\d,]+)\s*Ratings?/i);const rvm=t.match(/([\d,]+)\s*Reviews?/i);
            if(rm&&!r.rating_count)r.rating_count=rm[1].replace(/,/g,'');
            if(rvm&&!r.review_count)r.review_count=rvm[1].replace(/,/g,'');
        }
    }
    if(!r.size){const els=document.querySelectorAll('a.CDDksN,a._1fGeJ5');
        if(els.length)r.size=Array.from(els).map(s=>s.textContent.trim()).filter(Boolean).join(', ');}
    if(!r.colour){const els=document.querySelectorAll('div._4WELSP img,div._3V2wfe img');
        if(els.length)r.colour=[...new Set(Array.from(els).map(c=>c.alt||'').filter(Boolean))].join(', ');}

    // DOM reviews fallback
    if(!r.reviews.length){
        document.querySelectorAll('div.col,div._27M-vq,div._1AtVbE').forEach(el=>{
            const re=el.querySelector('div.XQDdHH,div._3LWZlK');
            const ne=el.querySelector('p._2NsDsF,p._2sc7ZR');
            const te=el.querySelector('div.ZmyHeo,div.t-ZTKy,div._6K-7Co');
            if(re&&(ne||te)){
                const ct=te?te.textContent.trim():'';
                if(ct) r.reviews.push('★'+(re?re.textContent.trim():'')+' '+(ne?ne.textContent.trim():'')+': '+ct);
            }
        });
    }
    // Calculate discount if we have both prices but no discount text
    if(!r.discount && r.mrp && r.current_price){
        const m=parseInt(r.mrp); const c=parseInt(r.current_price);
        if(m>c) r.discount=Math.round((1-c/m)*100)+'% off';
    }
    return r;
}
"""

# ---------------------------------------------------------------------------
# Core Playwright scraping
# ---------------------------------------------------------------------------
async def _scrape_one(context, sid, semaphore, is_retry=False):
    state = scraper_state
    if state["stop_event"].is_set(): return None
    async with semaphore:
        if state["stop_event"].is_set(): return None
        await asyncio.sleep(random.uniform(0.3, 1.0))
        url = _build_url(sid)
        for attempt in range(3):
            page = None
            try:
                page = await context.new_page()
                await page.route("**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot,mp4,webm}", lambda r: r.abort())
                await page.route("**/analytics**", lambda r: r.abort())
                await page.route("**/tracker**", lambda r: r.abort())
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                if resp and resp.status == 200:
                    await page.wait_for_timeout(random.randint(800, 1500))
                    data = await page.evaluate(EXTRACT_JS)
                    if not data.get("fsn"): data["fsn"] = sid
                    data["reviews"] = " | ".join(data.get("reviews", []))
                    data["url"] = url
                    await page.close()
                    # Log immediately so live logs update per product
                    if not is_retry:
                        state["products"].append(data)
                        state["scraped_count"] += 1
                        cnt = state["scraped_count"]
                        tot = state["total_items"]
                        label = data.get("title") or data.get("brand") or sid
                        if len(label)>50: label=label[:50]+"…"
                        rev_n = len(data["reviews"].split(" | ")) if data.get("reviews") else 0
                        _log(f"→ [{cnt}/{tot}] {label} | ⭐ {data.get('rating','-')} | 💬 {rev_n} reviews | MRP: ₹{data.get('mrp','-')}")
                    return data
                _log(f"⚠ HTTP {resp.status if resp else '?'} for {sid} (attempt {attempt+1})")
            except Exception as exc:
                _log(f"⚠ Attempt {attempt+1} error for {sid}: {exc}")
            finally:
                if page:
                    try: await page.close()
                    except: pass
            await asyncio.sleep(random.uniform(1.5, 3.0) * (attempt + 1))
        # Log failure immediately
        if not is_retry:
            state["failed_count"] += 1
            state["scraped_count"] += 1
            cnt = state["scraped_count"]
            tot = state["total_items"]
            _log(f"→ [{cnt}/{tot}] FAILED — {sid}")
        return None

async def _scrape_all(style_ids):
    state = scraper_state
    failed_ids = []
    _log(f"🌐 Launching browser ({MAX_TABS} tabs)…")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                "--disable-blink-features=AutomationControlled","--no-sandbox",
                "--disable-dev-shm-usage","--disable-gpu",
            ])
            _log("✓ Browser ready")

            async def make_ctx():
                ctx = await browser.new_context(
                    viewport={"width":1920,"height":1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    locale="en-US", timezone_id="Asia/Kolkata",
                    extra_http_headers={
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language":"en-US,en;q=0.9,hi;q=0.8",
                        "Sec-Ch-Ua":'"Chromium";v="131","Not_A Brand";v="24"',
                        "Sec-Ch-Ua-Mobile":"?0","Sec-Ch-Ua-Platform":'"Windows"',
                        "Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate",
                        "Sec-Fetch-Site":"none","Sec-Fetch-User":"?1",
                        "Upgrade-Insecure-Requests":"1",
                    },
                )
                await ctx.add_init_script("""
                    Object.defineProperty(navigator,'webdriver',{get:()=>false});
                    Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]});
                    window.chrome={runtime:{}};
                """)
                wp = await ctx.new_page()
                try:
                    await wp.goto("https://www.flipkart.com", wait_until="domcontentloaded", timeout=30000)
                    await wp.wait_for_timeout(1500)
                except: pass
                finally: await wp.close()
                return ctx

            ctx = await make_ctx()
            _log("🍪 Session cookies acquired")
            sem = asyncio.Semaphore(MAX_TABS)
            BATCH = MAX_TABS * 3
            sids = [str(s).strip() for s in style_ids]
            total_batches = (len(sids)+BATCH-1)//BATCH

            for bn, bs in enumerate(range(0, len(sids), BATCH), 1):
                if state["stop_event"].is_set(): break
                if bn > 1 and (bn-1)%5==0:
                    try: await ctx.close()
                    except: pass
                    ctx = await make_ctx()
                    _log("🔄 Context refreshed")
                batch = sids[bs:bs+BATCH]
                _log(f"📦 Batch {bn}/{total_batches} — {len(batch)} items")
                tasks = [asyncio.create_task(_scrape_one(ctx, s, sem)) for s in batch]
                results = await asyncio.gather(*tasks)
                # Collect failed IDs from this batch
                for i, r in enumerate(results):
                    if r is None:
                        failed_ids.append(batch[i])
                if bs+BATCH < len(sids):
                    await asyncio.sleep(random.uniform(1,3))

            # --- RETRY failed FSNs one by one ---
            if failed_ids and not state["stop_event"].is_set():
                _log(f"🔄 Retrying {len(failed_ids)} failed FSNs one-by-one…")
                try: await ctx.close()
                except: pass
                ctx = await make_ctx()
                _log("🍪 Fresh context for retries")
                retry_sem = asyncio.Semaphore(1)  # one at a time
                for sid in failed_ids:
                    if state["stop_event"].is_set(): break
                    await asyncio.sleep(random.uniform(1, 2))
                    r = await _scrape_one(ctx, sid, retry_sem, is_retry=True)
                    if r:
                        state["products"].append(r)
                        state["failed_count"] -= 1
                        label = r.get("title") or sid
                        if len(label)>50: label=label[:50]+"…"
                        _log(f"✓ RETRY OK: {label} | ⭐ {r.get('rating','-')} | MRP: ₹{r.get('mrp','-')}")
                    else:
                        _log(f"✗ RETRY FAILED: {sid}")

            try: await ctx.close()
            except: pass
            await browser.close()
    except Exception as exc:
        _log(f"❌ Browser error: {exc}")

    # Save Excel
    if state["products"]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fn = DOWNLOADS_DIR / f"flipkart_pdp_{ts}.xlsx"
        rows = [{
            "Brand":p.get("brand",""),"FSN":p.get("fsn",""),"Title":p.get("title",""),
            "Category":p.get("category",""),"Size":p.get("size",""),"Colour":p.get("colour",""),
            "MRP":p.get("mrp",""),"Current Selling Price":p.get("current_price",""),
            "Discount":p.get("discount",""),
            "Rating":p.get("rating",""),"Review Count":p.get("review_count",""),
            "Rating Count":p.get("rating_count",""),"Reviews":p.get("reviews",""),
            "URL":p.get("url",""),
        } for p in state["products"]]
        pd.DataFrame(rows).to_excel(fn, index=False, engine="openpyxl")
        state["output_file"] = str(fn)
        _log(f"✅ Saved {len(rows)} products → {fn.name}")
        if state["failed_count"]>0:
            _log(f"⚠ {state['failed_count']} FSNs still failed after retry.")
    else:
        _log("⚠ No products scraped.")

    if state["stop_event"].is_set(): _log("🛑 STOPPED by user.")
    else: _log("🏁 DONE")
    state["running"] = False

_executor = ThreadPoolExecutor(max_workers=1)

def _run_in_loop(style_ids):
    if sys.platform == "win32": loop = asyncio.ProactorEventLoop()
    else: loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(_scrape_all(style_ids))
    finally: loop.close()

async def scrape_task(style_ids):
    state = scraper_state
    state["running"]=True; state["stop_event"].clear()
    state["logs"].clear(); state["products"].clear()
    state["scraped_count"]=0; state["failed_count"]=0
    state["total_items"]=len(style_ids); state["output_file"]=None
    _log(f"🔍 Starting scrape for {len(style_ids)} FSNs…")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _run_in_loop, style_ids)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload_and_start(file: UploadFile = File(...)):
    if scraper_state["running"]:
        return {"status":"error","message":"Already running."}
    fname = file.filename or ""
    if not fname.lower().endswith((".xlsx",".xls")):
        return {"status":"error","message":"Upload .xlsx/.xls file."}
    save_path = UPLOADS_DIR / fname
    with open(save_path,"wb") as f: f.write(await file.read())
    try: df = pd.read_excel(save_path)
    except Exception as e: return {"status":"error","message":f"Read error: {e}"}

    style_col = None
    for col in df.columns:
        cl = str(col).lower().strip()
        if cl in ("fsn","style id","styleid","style_id","product id","productid","product_id","pid","id","sku"):
            style_col=col; break
    if not style_col:
        for col in df.columns:
            if any(k in str(col).lower() for k in ("style","product","fsn")):
                style_col=col; break
    if not style_col:
        for col in df.columns:
            if any(k in str(col).lower() for k in ("url","link")):
                style_col=col; break
    if not style_col: style_col=df.columns[0]

    sids = df[style_col].dropna().astype(str).str.strip().tolist()
    sids = [s.replace(".0","") for s in sids if s and s!="nan"]
    if not sids: return {"status":"error","message":"No FSNs found."}

    loop = asyncio.get_event_loop()
    scraper_state["task"] = loop.create_task(scrape_task(sids))
    return {"status":"started","message":f"Found {len(sids)} FSNs. Scraping!","total":len(sids)}

@app.post("/stop")
async def stop_scraping():
    if not scraper_state["running"]:
        return {"status":"info","message":"No active task."}
    scraper_state["stop_event"].set()
    return {"status":"stopped","message":"Stop signal sent."}

@app.get("/progress")
async def progress():
    async def gen():
        last=0
        while True:
            logs=scraper_state["logs"]
            while last<len(logs): yield f"data: {logs[last]}\n\n"; last+=1
            if not scraper_state["running"] and last>=len(logs):
                if scraper_state.get("output_file"): yield "data: __FILE_READY__\n\n"
                yield "data: __END__\n\n"; break
            await asyncio.sleep(0.4)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/download")
async def download():
    fp=scraper_state.get("output_file")
    if not fp or not os.path.isfile(fp):
        return {"status":"error","message":"No file."}
    return FileResponse(fp, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=os.path.basename(fp))

@app.get("/status")
async def status():
    return {"running":scraper_state["running"],"scraped":scraper_state["scraped_count"],
            "total":scraper_state["total_items"],"failed":scraper_state["failed_count"],
            "has_file":scraper_state.get("output_file") is not None}

if __name__=="__main__":
    import uvicorn
    uvicorn.run("app:app",host="0.0.0.0",port=int(os.environ.get("PORT",8002)),reload=True)
