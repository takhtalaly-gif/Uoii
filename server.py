#!/usr/bin/env python3
"""
YouKo v5 - YouTube-like Mobile-First Platform
Enhanced with Auto-Video Detection, Admin Panel, Health Monitor, Algorithm System
"""

import http.server, socketserver, json, os, uuid, time, subprocess, sys
import socket, mimetypes, urllib.parse, hashlib, threading, cgi, shutil, re
from datetime import datetime
from pathlib import Path

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
VIDS = os.path.join(DATA, "videos")
THUMBS = os.path.join(DATA, "thumbnails")
AVATARS = os.path.join(DATA, "avatars")
AUTO_VIDS = os.path.join(DATA, "auto_videos")  # Auto-detect folder
QUALITY_DIR = os.path.join(DATA, "quality")    # Transcoded qualities
DBFILE = os.path.join(DATA, "db.json")
PORT = 8080

# Create all directories
for d in [DATA, VIDS, THUMBS, AVATARS, AUTO_VIDS, QUALITY_DIR]:
    os.makedirs(d, exist_ok=True)

class DB:
    _lk = threading.Lock()
    @staticmethod
    def _e():
        return {
            "cnt": {"u": 0, "v": 0, "c": 0},
            "users": {},
            "videos": {},
            "comments": {},
            "likes": {},
            "subs": {},
            "sess": {},
            "notifs": {},
            "hist": {},
            "vlog": {},
            "reports": {},
            "settings": {"reg": True, "max_mb": 500, "maint": False},
            "algorithm": {},  # User behavior tracking
            "server_health": {"last_check": 0, "issues": [], "fixes": []},
            "playlists": {},   # User playlists
            "watchlater": {},  # Watch later lists
            "clikes": {},      # Comment likes
            "categories": {}   # Video categories
        }
    @classmethod
    def load(cls):
        with cls._lk:
            if not os.path.exists(DBFILE):
                db = cls._e(); cls._w(db); return db
            try:
                with open(DBFILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                for k, v in cls._e().items():
                    if k not in d: d[k] = v
                return d
            except:
                db = cls._e(); cls._w(db); return db
    @classmethod
    def _w(cls, db):
        with open(DBFILE, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
    @classmethod
    def save(cls, db):
        with cls._lk: cls._w(db)
    @classmethod
    def nid(cls, db, k):
        db["cnt"][k] = db["cnt"].get(k, 0) + 1
        return str(db["cnt"][k])

def hp(p): return hashlib.sha256(p.encode()).hexdigest()

def gip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except:
        return "127.0.0.1"
    finally:
        s.close()

def cfp(h):
    r = h.client_address[0] + "|" + (h.headers.get("User-Agent") or "")
    return hashlib.md5(r.encode()).hexdigest()[:16]

def ghtml():
    p = os.path.join(BASE, "index.html")
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>index.html not found</h1>"

def format_duration(seconds):
    """Format seconds to HH:MM:SS"""
    if not seconds or seconds <= 0:
        return "0:00"
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"

# ── Video Auto-Detection System ──
def scan_auto_videos():
    """Scan auto_videos folder and add new videos to database"""
    db = DB.load()
    added = 0
    
    for filename in os.listdir(AUTO_VIDS):
        filepath = os.path.join(AUTO_VIDS, filename)
        if not os.path.isfile(filepath):
            continue
            
        # Check if already in database
        already_exists = False
        for v in db["videos"].values():
            if v.get("auto_source") == filename:
                already_exists = True
                break
        
        if already_exists:
            continue
            
        # Get file extension
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ['.mp4', '.webm', '.mkv', '.mov', '.avi']:
            continue
            
        # Get video duration using ffprobe if available
        duration = 0
        try:
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
                 '-of', 'default=noprint_wrappers=1:nokey=1', filepath],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                duration = float(result.stdout.strip())
        except:
            pass
            
        # Create video entry
        vid = DB.nid(db, "v")
        vfn = vid + ext
        dest_path = os.path.join(VIDS, vfn)
        
        # Copy file
        shutil.copy2(filepath, dest_path)
        
        # Determine if short
        is_short = duration > 0 and duration < 60
        
        # Extract title from filename
        title = os.path.splitext(filename)[0].replace('_', ' ').replace('-', ' ')
        
        # Find admin user or create system user
        admin_uid = None
        for uid, u in db["users"].items():
            if u.get("is_admin"):
                admin_uid = uid
                break
        
        if not admin_uid:
            # Create a system admin
            admin_uid = "0"
            db["users"][admin_uid] = {
                "id": admin_uid, "username": "system", "password": hp("system"),
                "display_name": "System", "avatar": "", "bio": "",
                "created": time.time(), "is_admin": True, "is_banned": False, "is_verified": True
            }
        
        size_mb = os.path.getsize(dest_path) / 1048576
        
        db["videos"][vid] = {
            "id": vid, "uid": admin_uid, "title": title, "desc": "",
            "tags": [], "url": "/media/videos/" + vfn, "thumb": "",
            "views": 0, "created": time.time(), "size_mb": round(size_mb, 2),
            "hidden": False, "duration": duration, "is_short": is_short,
            "shares": 0, "auto_source": filename,
            "qualities": ["original"]  # Available qualities
        }
        added += 1
    
    if added > 0:
        DB.save(db)
    
    return added

# ── Video Quality Transcoding ──
def transcode_video(vid, source_path, quality):
    """Transcode video to specific quality"""
    quality_settings = {
        "144p": {"width": 256, "height": 144, "bitrate": "200k"},
        "240p": {"width": 426, "height": 240, "bitrate": "400k"},
        "360p": {"width": 640, "height": 360, "bitrate": "800k"},
        "480p": {"width": 854, "height": 480, "bitrate": "1200k"},
        "720p": {"width": 1280, "height": 720, "bitrate": "2500k"}
    }
    
    if quality not in quality_settings:
        return None
    
    settings = quality_settings[quality]
    output_dir = os.path.join(QUALITY_DIR, vid)
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, f"{quality}.mp4")
    
    try:
        cmd = [
            'ffmpeg', '-i', source_path, '-vf',
            f"scale={settings['width']}:{settings['height']}",
            '-b:v', settings['bitrate'], '-c:v', 'libx264',
            '-preset', 'fast', '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart', '-y', output_path
        ]
        subprocess.run(cmd, capture_output=True, timeout=300)
        
        if os.path.exists(output_path):
            return f"/media/quality/{vid}/{quality}.mp4"
    except:
        pass
    
    return None

# ── Algorithm System ──
def track_user_behavior(uid, action, data=None):
    """Track user behavior for algorithm"""
    db = DB.load()
    if "algorithm" not in db:
        db["algorithm"] = {}
    
    if uid not in db["algorithm"]:
        db["algorithm"][uid] = {
            "sessions": [],
            "video_preferences": {"short": 0, "long": 0},
            "watch_times": [],
            "last_active": time.time(),
            "afk_count": 0,
            "preferred_categories": [],
            "peak_hours": []
        }
    
    user_algo = db["algorithm"][uid]
    now = time.time()
    hour = datetime.now().hour
    
    if action == "session_start":
        user_algo["sessions"].append({"start": now, "videos": []})
    elif action == "video_watch":
        if user_algo["sessions"]:
            user_algo["sessions"][-1]["videos"].append(data.get("vid"))
        duration = data.get("duration", 0)
        if duration < 60:
            user_algo["video_preferences"]["short"] += 1
        else:
            user_algo["video_preferences"]["long"] += 1
        user_algo["watch_times"].append({"time": now, "duration": duration})
    elif action == "afk":
        user_algo["afk_count"] += 1
    elif action == "active":
        if hour not in user_algo["peak_hours"]:
            user_algo["peak_hours"].append(hour)
    
    user_algo["last_active"] = now
    
    # Keep only last 100 watch times
    user_algo["watch_times"] = user_algo["watch_times"][-100:]
    
    DB.save(db)

def get_recommendations(uid, limit=10):
    """Get personalized video recommendations"""
    db = DB.load()
    if uid not in db.get("algorithm", {}):
        return []
    
    user_algo = db["algorithm"][uid]
    prefs = user_algo.get("video_preferences", {"short": 0, "long": 0})
    
    # Determine preference
    prefer_short = prefs["short"] > prefs["long"]
    
    videos = []
    for v in db["videos"].values():
        if v.get("hidden"):
            continue
        score = v.get("views", 0) + v.get("like_count", 0) * 2
        if prefer_short and v.get("is_short"):
            score *= 1.5
        elif not prefer_short and not v.get("is_short"):
            score *= 1.5
        videos.append((v, score))
    
    videos.sort(key=lambda x: x[1], reverse=True)
    return [v[0] for v in videos[:limit]]

# ── Server Health Monitor ──
def check_server_health():
    """Check server health and return issues"""
    issues = []
    fixes = []
    
    # Check disk space
    try:
        stat = shutil.disk_usage(DATA)
        free_gb = stat.free / (1024**3)
        if free_gb < 1:
            issues.append(f"Low disk space: {free_gb:.1f}GB remaining")
    except:
        pass
    
    # Check database size
    try:
        db_size = os.path.getsize(DBFILE) / (1024**2)
        if db_size > 50:
            issues.append(f"Large database: {db_size:.1f}MB")
    except:
        pass
    
    # Check for orphaned files
    db = DB.load()
    video_files = set(os.listdir(VIDS)) if os.path.exists(VIDS) else set()
    db_files = set()
    for v in db["videos"].values():
        url = v.get("url", "")
        if url:
            db_files.add(os.path.basename(url))
    
    orphaned = video_files - db_files
    if orphaned:
        issues.append(f"Orphaned video files: {len(orphaned)}")
    
    # Check session count
    session_count = len(db.get("sess", {}))
    if session_count > 1000:
        issues.append(f"High session count: {session_count}")
    
    # Check memory (approximate via process count)
    try:
        import psutil
        mem = psutil.virtual_memory()
        if mem.percent > 90:
            issues.append(f"High memory usage: {mem.percent}%")
    except:
        pass
    
    return issues

def fix_all_issues():
    """Fix all detected issues"""
    fixes = []
    db = DB.load()
    
    # Clean old sessions
    now = time.time()
    old_sessions = {k: v for k, v in db["sess"].items() if now - v.get("cr", 0) > 86400}
    if old_sessions:
        for k in old_sessions:
            del db["sess"][k]
        fixes.append(f"Cleaned {len(old_sessions)} old sessions")
    
    # Clean old view logs
    cutoff = now - 86400
    old_logs = {k: v for k, v in db["vlog"].items() if v < cutoff}
    for k in old_logs:
        del db["vlog"][k]
    if old_logs:
        fixes.append(f"Cleaned {len(old_logs)} old view logs")
    
    # Remove orphaned files
    video_files = set(os.listdir(VIDS)) if os.path.exists(VIDS) else set()
    db_files = set()
    for v in db["videos"].values():
        url = v.get("url", "")
        if url:
            db_files.add(os.path.basename(url))
    
    orphaned = video_files - db_files
    for f in orphaned:
        try:
            os.remove(os.path.join(VIDS, f))
            fixes.append(f"Removed orphaned: {f}")
        except:
            pass
    
    # Clean old notifications
    for uid, notifs in db.get("notifs", {}).items():
        old_notifs = [n for n in notifs if now - n.get("cr", 0) > 2592000]  # 30 days
        if old_notifs:
            db["notifs"][uid] = [n for n in notifs if n not in old_notifs]
            fixes.append(f"Cleaned {len(old_notifs)} old notifications for {uid}")
    
    # Clean old history
    for uid, hist in db.get("hist", {}).items():
        old_hist = [h for h in hist if now - h.get("t", 0) > 7776000]  # 90 days
        if old_hist:
            db["hist"][uid] = [h for h in hist if h not in old_hist]
            fixes.append(f"Cleaned {len(old_hist)} old history for {uid}")
    
    db["server_health"]["last_check"] = now
    db["server_health"]["fixes"] = fixes
    DB.save(db)
    
    return fixes

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    
    def j(self, d, c=200):
        b = json.dumps(d, ensure_ascii=False).encode()
        self.send_response(c)
        self.send_header("Content-Type", "application/json;charset=utf-8")
        self.send_header("Content-Length", len(b))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(b)
    
    def htm(self, h):
        b = h.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html;charset=utf-8")
        self.send_header("Content-Length", len(b))
        self.end_headers()
        self.wfile.write(b)
    
    def sf(self, fp):
        if not os.path.exists(fp):
            self.send_response(404)
            self.end_headers()
            return
        sz = os.path.getsize(fp)
        ct = mimetypes.guess_type(fp)[0] or "application/octet-stream"
        rh = self.headers.get("Range")
        try:
            if rh and rh.startswith("bytes="):
                pts = rh[6:].split("-")
                s = int(pts[0]) if pts[0] else 0
                e = int(pts[1]) if pts[1] else sz - 1
                s, e = min(s, sz - 1), min(e, sz - 1)
                ln = e - s + 1
                self.send_response(206)
                self.send_header("Content-Range", "bytes {}-{}/{}".format(s, e, sz))
                self.send_header("Content-Length", ln)
                self.send_header("Content-Type", ct)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Cache-Control", "public,max-age=3600")
                self.end_headers()
                with open(fp, "rb") as f:
                    f.seek(s)
                    rm = ln
                    while rm > 0:
                        ch = f.read(min(65536, rm))
                        if not ch:
                            break
                        self.wfile.write(ch)
                        self.wfile.flush()
                        rm -= len(ch)
            else:
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", sz)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Cache-Control", "public,max-age=3600")
                self.end_headers()
                with open(fp, "rb") as f:
                    while True:
                        ch = f.read(65536)
                        if not ch:
                            break
                        self.wfile.write(ch)
                        self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
    
    def body(self):
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n) if n else b""
    
    def uid(self):
        ck = self.headers.get("Cookie", "")
        sid = None
        for p in ck.split(";"):
            p = p.strip()
            if p.startswith("session="):
                sid = p[8:]
                break
        if not sid:
            return None
        db = DB.load()
        s = db["sess"].get(sid)
        if not s:
            return None
        if time.time() - s.get("cr", 0) > 604800:
            del db["sess"][sid]
            DB.save(db)
            return None
        return s.get("uid")
    
    def mp(self):
        ct = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ct:
            return {}, {}
        env = {"REQUEST_METHOD": "POST", "CONTENT_TYPE": ct, "CONTENT_LENGTH": self.headers.get("Content-Length", "0")}
        fs = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=env, keep_blank_values=True)
        fld, fls = {}, {}
        for k in fs.keys():
            it = fs[k]
            if isinstance(it, list):
                it = it[0]
            if it.filename:
                fls[k] = {"nm": it.filename, "data": it.file.read(), "tp": it.type or "application/octet-stream"}
            else:
                fld[k] = it.value
        return fld, fls
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
    
    def do_GET(self):
        pr = urllib.parse.urlparse(self.path)
        p, q = pr.path, urllib.parse.parse_qs(pr.query)
        
        if p in ("/", "/index.html"):
            self.htm(ghtml())
        elif p == "/api/videos":
            self.gv(q)
        elif p == "/api/shorts":
            self.gs(q)
        elif p.startswith("/api/videos/"):
            self.gv1(p.split("/")[3])
        elif p == "/api/me":
            self.gme()
        elif p == "/api/feed":
            self.gfeed()
        elif p == "/api/search":
            self.gsrch(q)
        elif p == "/api/trending":
            self.gtrend()
        elif p == "/api/notifications":
            self.gnot()
        elif p == "/api/history":
            self.ghist()
        elif p == "/api/subscriptions":
            self.gsubs()
        elif p == "/api/recommendations":
            self.grec()
        elif p == "/api/health":
            self.ghealth()
        elif p == "/api/watchlater":
            self.gwl()
        elif p == "/api/playlists":
            self.gplaylists()
        elif p.startswith("/api/playlist/"):
            self.gplaylist(p.split("/")[3])
        elif p == "/api/analytics":
            self.ganalytics(q)
        elif p.startswith("/api/user/"):
            pts = p.split("/")
            if len(pts) == 4:
                self.gusr(pts[3])
            elif len(pts) == 5 and pts[4] == "videos":
                self.guvids(pts[3])
            elif len(pts) == 5 and pts[4] == "playlists":
                self.gupls(pts[3])
            else:
                self.j({"error": "?"}, 404)
        elif p.startswith("/api/comments/"):
            self.gcm(p.split("/")[3])
        elif p.startswith("/media/videos/"):
            self.sf(os.path.join(VIDS, p.split("/")[-1]))
        elif p.startswith("/media/thumbnails/"):
            self.sf(os.path.join(THUMBS, p.split("/")[-1]))
        elif p.startswith("/media/avatars/"):
            self.sf(os.path.join(AVATARS, p.split("/")[-1]))
        elif p.startswith("/media/quality/"):
            self.sf(os.path.join(QUALITY_DIR, p.split("/")[-2], p.split("/")[-1]))
        else:
            self.j({"error": "?"}, 404)
    
    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path
        rt = {
            "/api/register": self.preg,
            "/api/login": self.plog,
            "/api/logout": self.plout,
            "/api/upload": self.pup,
            "/api/like": self.plk,
            "/api/comment": self.pcm,
            "/api/subscribe": self.psub,
            "/api/view": self.pvw,
            "/api/update_profile": self.pprf,
            "/api/delete_video": self.pdv,
            "/api/delete_comment": self.pdc,
            "/api/report": self.prep,
            "/api/share": self.pshare,
            "/api/edit_video": self.pev,
            "/api/update_video": self.pev,
            "/api/transcode": self.ptr,
            "/api/scan_videos": self.pscan,
            "/api/fix_server": self.pfix,
            "/api/algorithm": self.palgo,
            "/api/comment_like": self.pclk,
            "/api/comment_reply": self.pcreply,
            "/api/watchlater": self.pwl,
            "/api/playlist_create": self.pplc,
            "/api/playlist_add": self.ppla,
            "/api/playlist_remove": self.pplr,
            "/api/change_password": self.pchpw,
            "/api/pin_comment": self.ppincm,
        }
        fn = rt.get(p)
        if fn:
            fn()
        else:
            self.j({"error": "?"}, 404)
    
    def preg(self):
        db = DB.load()
        if not db["settings"].get("reg", True):
            return self.j({"error": "ثبت نام غیرفعال"}, 403)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        un = b.get("username", "").strip()
        pw = b.get("password", "").strip()
        dn = b.get("display_name", "").strip() or un
        if not un or not pw:
            return self.j({"error": "نام کاربری و رمز الزامی"}, 400)
        if len(un) < 3:
            return self.j({"error": "نام کاربری حداقل 3 کاراکتر"}, 400)
        if len(pw) < 4:
            return self.j({"error": "رمز حداقل 4 کاراکتر"}, 400)
        for u in db["users"].values():
            if u["username"].lower() == un.lower():
                return self.j({"error": "تکراری"}, 400)
        uid = DB.nid(db, "u")
        first = len(db["users"]) == 0
        db["users"][uid] = {
            "id": uid, "username": un, "password": hp(pw), "display_name": dn,
            "avatar": "", "bio": "", "created": time.time(),
            "is_admin": first, "is_banned": False, "is_verified": False
        }
        sid = str(uuid.uuid4())
        db["sess"][sid] = {"uid": uid, "cr": time.time()}
        DB.save(db)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Set-Cookie", "session={};Path=/;HttpOnly".format(sid))
        r = json.dumps({"ok": True, "user_id": uid}).encode()
        self.send_header("Content-Length", len(r))
        self.end_headers()
        self.wfile.write(r)
    
    def plog(self):
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        un = b.get("username", "").strip()
        pw = b.get("password", "").strip()
        db = DB.load()
        found = None
        for u in db["users"].values():
            if u["username"].lower() == un.lower() and u["password"] == hp(pw):
                found = u
                break
        if not found:
            return self.j({"error": "اشتباه"}, 401)
        if found.get("is_banned"):
            return self.j({"error": "مسدود"}, 403)
        sid = str(uuid.uuid4())
        db["sess"][sid] = {"uid": found["id"], "cr": time.time()}
        DB.save(db)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Set-Cookie", "session={};Path=/;HttpOnly".format(sid))
        r = json.dumps({"ok": True}).encode()
        self.send_header("Content-Length", len(r))
        self.end_headers()
        self.wfile.write(r)
    
    def plout(self):
        ck = self.headers.get("Cookie", "")
        for p in ck.split(";"):
            p = p.strip()
            if p.startswith("session="):
                db = DB.load()
                db["sess"].pop(p[8:], None)
                DB.save(db)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Set-Cookie", "session=;Path=/;HttpOnly;Max-Age=0")
        r = b'{"ok":true}'
        self.send_header("Content-Length", len(r))
        self.end_headers()
        self.wfile.write(r)
    
    def gme(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        usr = db["users"].get(u)
        if not usr:
            return self.j({"error": "?"}, 404)
        s = {k: v for k, v in usr.items() if k != "password"}
        s["subs_count"] = sum(1 for x in db["subs"].values() if x["ch"] == u)
        s["vid_count"] = sum(1 for x in db["videos"].values() if x["uid"] == u)
        s["unread"] = sum(1 for n in db["notifs"].get(u, []) if not n.get("read"))
        self.j(s)
    
    def pup(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        if db["users"].get(u, {}).get("is_banned"):
            return self.j({"error": "مسدود"}, 403)
        fld, fls = self.mp()
        if "video" not in fls:
            return self.j({"error": "فایل الزامی"}, 400)
        mx = db["settings"].get("max_mb", 500)
        vf = fls["video"]
        sz = len(vf["data"]) / 1048576
        if sz > mx:
            return self.j({"error": "حجم بیش از {} MB".format(mx)}, 400)
        
        title = fld.get("title", "بدون عنوان").strip()
        desc = fld.get("description", "").strip()
        tags = fld.get("tags", "").strip()
        quality = fld.get("quality", "720p").strip()  # Default quality
        dur = 0
        try:
            dur = float(fld.get("duration", "0"))
        except:
            pass
        
        vid = DB.nid(db, "v")
        ext = os.path.splitext(vf["nm"])[1] or ".mp4"
        vfn = vid + ext
        
        with open(os.path.join(VIDS, vfn), "wb") as f:
            f.write(vf["data"])
        
        thu = ""
        if "thumbnail" in fls:
            tf = fls["thumbnail"]
            te = os.path.splitext(tf["nm"])[1] or ".jpg"
            tfn = vid + te
            with open(os.path.join(THUMBS, tfn), "wb") as f:
                f.write(tf["data"])
            thu = "/media/thumbnails/" + tfn
        
        is_short = dur > 0 and dur < 60
        
        # Determine available qualities based on upload setting
        available_qualities = ["original"]
        quality_order = ["144p", "240p", "360p", "480p", "720p"]
        if quality in quality_order:
            idx = quality_order.index(quality)
            available_qualities = quality_order[:idx+1]
        
        db["videos"][vid] = {
            "id": vid, "uid": u, "title": title, "desc": desc,
            "tags": [t.strip() for t in tags.split(",") if t.strip()],
            "url": "/media/videos/" + vfn, "thumb": thu, "views": 0,
            "created": time.time(), "size_mb": round(sz, 2),
            "hidden": False, "duration": dur, "is_short": is_short,
            "shares": 0, "qualities": available_qualities,
            "max_quality": quality
        }
        
        # Notify subscribers
        for s in db["subs"].values():
            if s["ch"] == u:
                sid = s["sub"]
                if sid not in db["notifs"]:
                    db["notifs"][sid] = []
                db["notifs"][sid].append({
                    "id": str(uuid.uuid4())[:8], "type": "new_video",
                    "vid": vid, "from": u, "msg": "ویدیوی جدید: " + title,
                    "cr": time.time(), "read": False
                })
        
        DB.save(db)
        self.j({"ok": True, "video_id": vid})
    
    def _enrich(self, db, v):
        usr = db["users"].get(v["uid"], {})
        v["author_name"] = usr.get("display_name", "?")
        v["author_avatar"] = usr.get("avatar", "")
        v["author_verified"] = usr.get("is_verified", False)
        v["author_username"] = usr.get("username", "")
        v["like_count"] = sum(1 for l in db["likes"].values() if l["vid"] == v["id"] and l["val"] == 1)
        v["dislike_count"] = sum(1 for l in db["likes"].values() if l["vid"] == v["id"] and l["val"] == -1)
        v["comment_count"] = sum(1 for c in db["comments"].values() if c["vid"] == v["id"])
        v["user_id"] = v["uid"]
        v["video_url"] = v.get("url", "")
        v["thumbnail_url"] = v.get("thumb", "")
        v["description"] = v.get("desc", "")
        v["formatted_duration"] = format_duration(v.get("duration", 0))
    
    def gv(self, q):
        db = DB.load()
        sort = q.get("sort", ["newest"])[0]
        pg = int(q.get("page", ["1"])[0])
        pp = int(q.get("per_page", ["12"])[0])
        vs = [v for v in db["videos"].values() if not v.get("hidden") and not v.get("is_short")]
        for v in vs:
            self._enrich(db, v)
        if sort == "popular":
            vs.sort(key=lambda x: x["views"], reverse=True)
        elif sort == "liked":
            vs.sort(key=lambda x: x["like_count"], reverse=True)
        else:
            vs.sort(key=lambda x: x["created"], reverse=True)
        s = (pg - 1) * pp
        self.j({"videos": vs[s:s+pp], "total": len(vs), "page": pg})
    
    def gs(self, q):
        db = DB.load()
        vs = [v for v in db["videos"].values() if not v.get("hidden") and v.get("is_short")]
        for v in vs:
            self._enrich(db, v)
        import random
        random.shuffle(vs)
        self.j({"videos": vs[:20]})
    
    def gv1(self, vid):
        db = DB.load()
        v = db["videos"].get(vid)
        if not v:
            return self.j({"error": "یافت نشد"}, 404)
        self._enrich(db, v)
        v["channel_subs"] = sum(1 for s in db["subs"].values() if s["ch"] == v["uid"])
        cu = self.uid()
        v["user_liked"] = 0
        v["user_subscribed"] = False
        if cu:
            for l in db["likes"].values():
                if l["vid"] == vid and l["uid"] == cu:
                    v["user_liked"] = l["val"]
                    break
            for s in db["subs"].values():
                if s["sub"] == cu and s["ch"] == v["uid"]:
                    v["user_subscribed"] = True
                    break
        rel = [x for x in db["videos"].values() if x["id"] != vid and not x.get("hidden")]
        rel.sort(key=lambda x: x["views"], reverse=True)
        for rv in rel[:10]:
            self._enrich(db, rv)
        v["related"] = rel[:10]
        self.j(v)
    
    def plk(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        val = b.get("value", 1)
        db = DB.load()
        if vid not in db["videos"]:
            return self.j({"error": "یافت نشد"}, 404)
        lk = None
        for lid, l in db["likes"].items():
            if l["uid"] == u and l["vid"] == vid:
                lk = lid
                break
        if lk:
            if db["likes"][lk]["val"] == val:
                del db["likes"][lk]
                val = 0
            else:
                db["likes"][lk]["val"] = val
        elif val in (1, -1):
            db["likes"][str(uuid.uuid4())[:8]] = {"uid": u, "vid": vid, "val": val}
        DB.save(db)
        lc = sum(1 for l in db["likes"].values() if l["vid"] == vid and l["val"] == 1)
        dc = sum(1 for l in db["likes"].values() if l["vid"] == vid and l["val"] == -1)
        self.j({"ok": True, "like_count": lc, "dislike_count": dc, "user_liked": val})
    
    def pcm(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        txt = b.get("text", "").strip()
        if not txt:
            return self.j({"error": "خالی"}, 400)
        db = DB.load()
        if vid not in db["videos"]:
            return self.j({"error": "یافت نشد"}, 404)
        cid = DB.nid(db, "c")
        db["comments"][cid] = {"id": cid, "vid": vid, "uid": u, "text": txt, "cr": time.time()}
        vo = db["videos"][vid]["uid"]
        if vo != u:
            if vo not in db["notifs"]:
                db["notifs"][vo] = []
            db["notifs"][vo].append({
                "id": str(uuid.uuid4())[:8], "type": "comment", "vid": vid,
                "from": u, "msg": "کامنت جدید", "cr": time.time(), "read": False
            })
        DB.save(db)
        usr = db["users"].get(u, {})
        cd = db["comments"][cid].copy()
        cd["author_name"] = usr.get("display_name", "?")
        cd["author_avatar"] = usr.get("avatar", "")
        cd["author_verified"] = usr.get("is_verified", False)
        cd["video_id"] = vid
        cd["user_id"] = u
        cd["created"] = cd["cr"]
        self.j({"ok": True, "comment": cd})
    
    def gcm(self, vid):
        db = DB.load()
        u = self.uid()
        # Get top-level comments first
        cms = [c for c in db["comments"].values() if c["vid"] == vid and not c.get("parent_id")]
        for c in cms:
            usr = db["users"].get(c["uid"], {})
            c["author_name"] = usr.get("display_name", "?")
            c["author_avatar"] = usr.get("avatar", "")
            c["author_verified"] = usr.get("is_verified", False)
            c["video_id"] = c["vid"]
            c["user_id"] = c["uid"]
            c["created"] = c["cr"]
            c["like_count"] = sum(1 for k in db.get("clikes", {}) if k.endswith("_" + c["id"]))
            c["user_liked"] = (u + "_" + c["id"]) in db.get("clikes", {}) if u else False
            # Get replies
            replies = [r for r in db["comments"].values() if r.get("parent_id") == c["id"]]
            for r in replies:
                ru = db["users"].get(r["uid"], {})
                r["author_name"] = ru.get("display_name", "?")
                r["author_avatar"] = ru.get("avatar", "")
                r["author_verified"] = ru.get("is_verified", False)
                r["video_id"] = r["vid"]
                r["user_id"] = r["uid"]
                r["created"] = r["cr"]
                r["like_count"] = sum(1 for k in db.get("clikes", {}) if k.endswith("_" + r["id"]))
                r["user_liked"] = (u + "_" + r["id"]) in db.get("clikes", {}) if u else False
            replies.sort(key=lambda x: x["cr"])
            c["replies"] = replies
        # Sort: pinned first, then by time
        cms.sort(key=lambda x: (not x.get("pinned", False), -x["cr"]))
        self.j({"comments": cms})
    
    def pdc(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        cid = b.get("comment_id", "")
        db = DB.load()
        c = db["comments"].get(cid)
        if not c:
            return self.j({"error": "یافت نشد"}, 404)
        usr = db["users"].get(u, {})
        vo = db["videos"].get(c["vid"], {}).get("uid")
        if c["uid"] != u and vo != u and not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        del db["comments"][cid]
        DB.save(db)
        self.j({"ok": True})
    
    def psub(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        ch = b.get("channel_id", "")
        db = DB.load()
        if ch not in db["users"]:
            return self.j({"error": "یافت نشد"}, 404)
        if ch == u:
            return self.j({"error": "!"}, 400)
        ek = None
        for sid, s in db["subs"].items():
            if s["sub"] == u and s["ch"] == ch:
                ek = sid
                break
        if ek:
            del db["subs"][ek]
            sub = False
        else:
            db["subs"][str(uuid.uuid4())[:8]] = {"sub": u, "ch": ch, "cr": time.time()}
            sub = True
            if ch not in db["notifs"]:
                db["notifs"][ch] = []
            db["notifs"][ch].append({
                "id": str(uuid.uuid4())[:8], "type": "subscribe",
                "from": u, "msg": "مشترک جدید", "cr": time.time(), "read": False
            })
        sc = sum(1 for s in db["subs"].values() if s["ch"] == ch)
        DB.save(db)
        self.j({"ok": True, "subscribed": sub, "count": sc})
    
    def pvw(self):
        try:
            b = json.loads(self.body())
        except:
            return self.j({"ok": True})
        vid = b.get("video_id", "")
        db = DB.load()
        if vid not in db["videos"]:
            return self.j({"ok": True})
        fp = cfp(self)
        vk = fp + "_" + vid
        now = time.time()
        if now - db["vlog"].get(vk, 0) < 300:
            return self.j({"ok": True, "counted": False})
        db["vlog"][vk] = now
        db["videos"][vid]["views"] += 1
        cut = now - 3600
        db["vlog"] = {k: v for k, v in db["vlog"].items() if v > cut}
        u = self.uid()
        if u:
            if u not in db["hist"]:
                db["hist"][u] = []
            if db["videos"][vid]["uid"] != u:
                db["hist"][u] = [h for h in db["hist"][u] if h["v"] != vid]
                db["hist"][u].insert(0, {"v": vid, "t": now})
                db["hist"][u] = db["hist"][u][:100]
                # Track for algorithm
                track_user_behavior(u, "video_watch", {"vid": vid, "duration": db["videos"][vid].get("duration", 0)})
        DB.save(db)
        self.j({"ok": True, "counted": True})
    
    def gsrch(self, q):
        qs = q.get("q", [""])[0].lower().strip()
        if not qs:
            return self.j({"videos": []})
        db = DB.load()
        res = []
        for v in db["videos"].values():
            if v.get("hidden"):
                continue
            sc = 0
            if qs in v["title"].lower():
                sc += 10
            if qs in v.get("desc", "").lower():
                sc += 5
            for t in v.get("tags", []):
                if qs in t.lower():
                    sc += 7
            if sc > 0:
                vc = v.copy()
                vc["score"] = sc
                self._enrich(db, vc)
                res.append(vc)
        res.sort(key=lambda x: x["score"], reverse=True)
        self.j({"videos": res[:30]})
    
    def gtrend(self):
        db = DB.load()
        now = time.time()
        vs = [v for v in db["videos"].values() if not v.get("hidden")]
        for v in vs:
            h = max(1, (now - v["created"]) / 3600)
            self._enrich(db, v)
            v["trend"] = (v["views"] + v["like_count"] * 3 + v["comment_count"] * 2) / h
        vs.sort(key=lambda x: x["trend"], reverse=True)
        self.j({"videos": vs[:20]})
    
    def gfeed(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        chs = [s["ch"] for s in db["subs"].values() if s["sub"] == u]
        vs = [v for v in db["videos"].values() if v["uid"] in chs and not v.get("hidden")]
        for v in vs:
            self._enrich(db, v)
        vs.sort(key=lambda x: x["created"], reverse=True)
        self.j({"videos": vs[:30]})
    
    def grec(self):
        """Get personalized recommendations"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        recs = get_recommendations(u, 20)
        db = DB.load()
        for v in recs:
            self._enrich(db, v)
        self.j({"videos": recs})
    
    def ghealth(self):
        """Get server health status"""
        u = self.uid()
        db = DB.load()
        if not u or not db["users"].get(u, {}).get("is_admin"):
            return self.j({"error": "unauthorized"}, 401)
        
        issues = check_server_health()
        db["server_health"]["last_check"] = time.time()
        db["server_health"]["issues"] = issues
        DB.save(db)
        
        # Get system stats
        stats = {
            "users": len(db["users"]),
            "videos": len(db["videos"]),
            "comments": len(db["comments"]),
            "likes": len(db["likes"]),
            "sessions": len(db["sess"]),
            "issues": issues,
            "disk_free_gb": 0
        }
        
        try:
            stat = shutil.disk_usage(DATA)
            stats["disk_free_gb"] = round(stat.free / (1024**3), 2)
        except:
            pass
        
        self.j(stats)
    
    def gnot(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        ns = db["notifs"].get(u, [])
        for n in ns:
            fu = db["users"].get(n.get("from", ""), {})
            n["from_name"] = fu.get("display_name", "سیستم")
            n["from_avatar"] = fu.get("avatar", "")
            n["created"] = n.get("cr", 0)
            n["message"] = n.get("msg", "")
            n["video_id"] = n.get("vid", "")
        ns.sort(key=lambda x: x.get("cr", 0), reverse=True)
        for n in ns:
            n["read"] = True
        DB.save(db)
        self.j({"notifications": ns[:50]})
    
    def ghist(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        hs = db["hist"].get(u, [])
        res = []
        for h in hs:
            v = db["videos"].get(h["v"])
            if v and not v.get("hidden"):
                vc = v.copy()
                self._enrich(db, vc)
                res.append(vc)
        self.j({"videos": res})
    
    def gsubs(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        chs = []
        for s in db["subs"].values():
            if s["sub"] == u:
                ch = db["users"].get(s["ch"])
                if ch:
                    sf = {k: v for k, v in ch.items() if k != "password"}
                    sf["subs_count"] = sum(1 for ss in db["subs"].values() if ss["ch"] == ch["id"])
                    sf["vid_count"] = sum(1 for v in db["videos"].values() if v["uid"] == ch["id"])
                    chs.append(sf)
        self.j({"channels": chs})
    
    def gusr(self, uid):
        db = DB.load()
        u = db["users"].get(uid)
        if not u:
            return self.j({"error": "یافت نشد"}, 404)
        s = {k: v for k, v in u.items() if k != "password"}
        s["subs_count"] = sum(1 for x in db["subs"].values() if x["ch"] == uid)
        s["vid_count"] = sum(1 for v in db["videos"].values() if v["uid"] == uid and not v.get("hidden"))
        s["total_views"] = sum(v["views"] for v in db["videos"].values() if v["uid"] == uid)
        cu = self.uid()
        s["is_subscribed"] = False
        if cu:
            for x in db["subs"].values():
                if x["sub"] == cu and x["ch"] == uid:
                    s["is_subscribed"] = True
                    break
        self.j(s)
    
    def guvids(self, uid):
        db = DB.load()
        cu = self.uid()
        vs = [v for v in db["videos"].values() if v["uid"] == uid]
        if cu != uid:
            vs = [v for v in vs if not v.get("hidden")]
        for v in vs:
            self._enrich(db, v)
        vs.sort(key=lambda x: x["created"], reverse=True)
        self.j({"videos": vs})
    
    def gupls(self, uid):
        """Get user's public playlists"""
        db = DB.load()
        cu = self.uid()
        pls = []
        for plid, pl in db["playlists"].items():
            if pl["owner"] == uid:
                # Only show public playlists unless viewing own
                if pl.get("public", True) or cu == uid:
                    pls.append({
                        "id": pl["id"],
                        "name": pl["name"],
                        "desc": pl.get("desc", ""),
                        "video_count": len(pl.get("videos", [])),
                        "owner_id": pl["owner"],
                        "thumb": pl.get("videos", [None])[0] if pl.get("videos") else None
                    })
        self.j({"playlists": pls})
    
    def pprf(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        fld, fls = self.mp()
        db = DB.load()
        usr = db["users"].get(u)
        if not usr:
            return self.j({"error": "?"}, 404)
        if "display_name" in fld and fld["display_name"].strip():
            usr["display_name"] = fld["display_name"].strip()
        if "bio" in fld:
            usr["bio"] = fld["bio"].strip()
        if "avatar" in fls:
            av = fls["avatar"]
            ext = os.path.splitext(av["nm"])[1] or ".jpg"
            afn = u + ext
            with open(os.path.join(AVATARS, afn), "wb") as f:
                f.write(av["data"])
            usr["avatar"] = "/media/avatars/" + afn
        if "banner" in fls:
            bn = fls["banner"]
            ext = os.path.splitext(bn["nm"])[1] or ".jpg"
            bfn = "banner_" + u + ext
            with open(os.path.join(AVATARS, bfn), "wb") as f:
                f.write(bn["data"])
            usr["banner"] = "/media/avatars/" + bfn
        DB.save(db)
        self.j({"ok": True, "banner": usr.get("banner",""), "avatar": usr.get("avatar","")})
    
    def pdv(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        db = DB.load()
        v = db["videos"].get(vid)
        if not v:
            return self.j({"error": "یافت نشد"}, 404)
        usr = db["users"].get(u, {})
        if v["uid"] != u and not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        vp = os.path.join(VIDS, os.path.basename(v.get("url", "")))
        if os.path.exists(vp):
            os.remove(vp)
        if v.get("thumb"):
            tp = os.path.join(THUMBS, os.path.basename(v["thumb"]))
            if os.path.exists(tp):
                os.remove(tp)
        # Remove quality files
        qual_dir = os.path.join(QUALITY_DIR, vid)
        if os.path.exists(qual_dir):
            shutil.rmtree(qual_dir)
        del db["videos"][vid]
        db["comments"] = {k: c for k, c in db["comments"].items() if c["vid"] != vid}
        db["likes"] = {k: l for k, l in db["likes"].items() if l["vid"] != vid}
        DB.save(db)
        self.j({"ok": True})
    
    def pev(self):
        """Edit video"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        usr = db["users"].get(u, {})
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        v = db["videos"].get(vid)
        if not v:
            return self.j({"error": "یافت نشد"}, 404)
        
        # Check if user owns this video or is admin
        if v["uid"] != u and not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        
        # Update fields
        if "title" in b:
            v["title"] = b["title"].strip()
        if "description" in b:
            v["desc"] = b["description"].strip()
        if "tags" in b:
            if isinstance(b["tags"], list):
                v["tags"] = [t.strip() for t in b["tags"] if t.strip()]
            else:
                v["tags"] = [t.strip() for t in str(b["tags"]).split(",") if t.strip()]
        if "hidden" in b and usr.get("is_admin"):
            v["hidden"] = b["hidden"]
        if "is_short" in b and usr.get("is_admin"):
            v["is_short"] = b["is_short"]
        
        DB.save(db)
        self.j({"ok": True})
    
    def ptr(self):
        """Transcode video to different qualities"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        usr = db["users"].get(u, {})
        if not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        quality = b.get("quality", "720p")
        v = db["videos"].get(vid)
        if not v:
            return self.j({"error": "یافت نشد"}, 404)
        
        source_path = os.path.join(VIDS, os.path.basename(v.get("url", "")))
        if not os.path.exists(source_path):
            return self.j({"error": "فایل یافت نشد"}, 404)
        
        # Start transcoding in background
        def do_transcode():
            result = transcode_video(vid, source_path, quality)
            if result:
                db = DB.load()
                if vid in db["videos"]:
                    if quality not in db["videos"][vid]["qualities"]:
                        db["videos"][vid]["qualities"].append(quality)
                    DB.save(db)
        
        threading.Thread(target=do_transcode, daemon=True).start()
        self.j({"ok": True, "message": "ترانسکدینگ شروع شد"})
    
    def pscan(self):
        """Scan auto videos folder"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        usr = db["users"].get(u, {})
        if not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        
        added = scan_auto_videos()
        self.j({"ok": True, "added": added})
    
    def pfix(self):
        """Fix all server issues"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        usr = db["users"].get(u, {})
        if not usr.get("is_admin"):
            return self.j({"error": "دسترسی نیست"}, 403)
        
        fixes = fix_all_issues()
        self.j({"ok": True, "fixes": fixes})
    
    def palgo(self):
        """Algorithm tracking endpoint"""
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        
        action = b.get("action", "")
        data = b.get("data", {})
        track_user_behavior(u, action, data)
        self.j({"ok": True})
    
    def prep(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        db = DB.load()
        rid = str(uuid.uuid4())[:8]
        db["reports"][rid] = {
            "reporter": u, "type": b.get("type", "video"),
            "target": b.get("target_id", ""), "reason": b.get("reason", ""),
            "cr": time.time(), "resolved": False
        }
        DB.save(db)
        self.j({"ok": True})
    
    def pshare(self):
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        db = DB.load()
        if vid in db["videos"]:
            db["videos"][vid]["shares"] = db["videos"][vid].get("shares", 0) + 1
            DB.save(db)
        self.j({"ok": True})

    # ── Comment Like ──
    def pclk(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        cid = b.get("comment_id", "")
        db = DB.load()
        if cid not in db["comments"]:
            return self.j({"error": "یافت نشد"}, 404)
        key = u + "_" + cid
        if key in db.get("clikes", {}):
            del db["clikes"][key]
            liked = False
        else:
            if "clikes" not in db:
                db["clikes"] = {}
            db["clikes"][key] = {"uid": u, "cid": cid, "cr": time.time()}
            liked = True
        lc = sum(1 for k in db.get("clikes", {}) if k.endswith("_" + cid))
        DB.save(db)
        self.j({"ok": True, "liked": liked, "count": lc})

    # ── Comment Reply ──
    def pcreply(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        parent_id = b.get("parent_id", "")
        txt = b.get("text", "").strip()
        if not txt:
            return self.j({"error": "خالی"}, 400)
        db = DB.load()
        if vid not in db["videos"]:
            return self.j({"error": "یافت نشد"}, 404)
        if parent_id and parent_id not in db["comments"]:
            return self.j({"error": "کامنت والد یافت نشد"}, 404)
        cid = DB.nid(db, "c")
        db["comments"][cid] = {
            "id": cid, "vid": vid, "uid": u, "text": txt,
            "cr": time.time(), "parent_id": parent_id, "pinned": False
        }
        # Notify parent comment owner
        if parent_id:
            parent = db["comments"].get(parent_id, {})
            po = parent.get("uid")
            if po and po != u:
                if po not in db["notifs"]:
                    db["notifs"][po] = []
                db["notifs"][po].append({
                    "id": str(uuid.uuid4())[:8], "type": "reply", "vid": vid,
                    "from": u, "msg": "پاسخ به نظر شما", "cr": time.time(), "read": False
                })
        DB.save(db)
        usr = db["users"].get(u, {})
        cd = db["comments"][cid].copy()
        cd["author_name"] = usr.get("display_name", "?")
        cd["author_avatar"] = usr.get("avatar", "")
        cd["author_verified"] = usr.get("is_verified", False)
        cd["like_count"] = 0
        self.j({"ok": True, "comment": cd})

    # ── Watch Later ──
    def gwl(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        wl = db.get("watchlater", {}).get(u, [])
        res = []
        for vid in wl:
            v = db["videos"].get(vid)
            if v and not v.get("hidden"):
                vc = v.copy()
                self._enrich(db, vc)
                res.append(vc)
        self.j({"videos": res})

    def pwl(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        vid = b.get("video_id", "")
        db = DB.load()
        if vid not in db["videos"]:
            return self.j({"error": "یافت نشد"}, 404)
        if "watchlater" not in db:
            db["watchlater"] = {}
        wl = db["watchlater"].get(u, [])
        if vid in wl:
            wl.remove(vid)
            added = False
        else:
            wl.insert(0, vid)
            if len(wl) > 200:
                wl = wl[:200]
            added = True
        db["watchlater"][u] = wl
        DB.save(db)
        self.j({"ok": True, "added": added, "count": len(wl)})

    # ── Playlists ──
    def gplaylists(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        pls = [p for p in db.get("playlists", {}).values() if p["uid"] == u]
        for pl in pls:
            pl["video_count"] = len(pl.get("videos", []))
            # Get first video thumbnail
            if pl["videos"]:
                fv = db["videos"].get(pl["videos"][0], {})
                pl["thumb"] = fv.get("thumb", "")
            else:
                pl["thumb"] = ""
        self.j({"playlists": pls})

    def gplaylist(self, plid):
        db = DB.load()
        pl = db.get("playlists", {}).get(plid)
        if not pl:
            return self.j({"error": "یافت نشد"}, 404)
        res = []
        for vid in pl.get("videos", []):
            v = db["videos"].get(vid)
            if v and not v.get("hidden"):
                vc = v.copy()
                self._enrich(db, vc)
                res.append(vc)
        pl_copy = pl.copy()
        pl_copy["videos"] = res
        owner = db["users"].get(pl["uid"], {})
        pl_copy["owner_name"] = owner.get("display_name", "?")
        self.j(pl_copy)

    def pplc(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        name = b.get("name", "").strip()
        if not name:
            return self.j({"error": "نام الزامی"}, 400)
        db = DB.load()
        plid = str(uuid.uuid4())[:8]
        if "playlists" not in db:
            db["playlists"] = {}
        db["playlists"][plid] = {
            "id": plid, "uid": u, "name": name,
            "desc": b.get("desc", "").strip(),
            "videos": [], "created": time.time(),
            "public": b.get("public", True)
        }
        DB.save(db)
        self.j({"ok": True, "playlist_id": plid})

    def ppla(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        plid = b.get("playlist_id", "")
        vid = b.get("video_id", "")
        db = DB.load()
        pl = db.get("playlists", {}).get(plid)
        if not pl or pl["uid"] != u:
            return self.j({"error": "دسترسی نیست"}, 403)
        if vid not in db["videos"]:
            return self.j({"error": "ویدیو یافت نشد"}, 404)
        if vid not in pl["videos"]:
            pl["videos"].append(vid)
            DB.save(db)
        self.j({"ok": True, "count": len(pl["videos"])})

    def pplr(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        plid = b.get("playlist_id", "")
        vid = b.get("video_id", "")
        db = DB.load()
        pl = db.get("playlists", {}).get(plid)
        if not pl or pl["uid"] != u:
            return self.j({"error": "دسترسی نیست"}, 403)
        if vid in pl["videos"]:
            pl["videos"].remove(vid)
            DB.save(db)
        self.j({"ok": True})

    # ── Change Password ──
    def pchpw(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        old = b.get("old_password", "")
        new = b.get("new_password", "")
        db = DB.load()
        usr = db["users"].get(u)
        if not usr:
            return self.j({"error": "یافت نشد"}, 404)
        if usr["password"] != hp(old):
            return self.j({"error": "رمز فعلی اشتباه"}, 400)
        if len(new) < 4:
            return self.j({"error": "رمز جدید حداقل 4 کاراکتر"}, 400)
        usr["password"] = hp(new)
        DB.save(db)
        self.j({"ok": True})

    # ── Pin Comment ──
    def ppincm(self):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        try:
            b = json.loads(self.body())
        except:
            return self.j({"error": "خطا"}, 400)
        cid = b.get("comment_id", "")
        db = DB.load()
        c = db["comments"].get(cid)
        if not c:
            return self.j({"error": "یافت نشد"}, 404)
        # Only video owner can pin
        vid = c.get("vid", "")
        vo = db["videos"].get(vid, {}).get("uid")
        if vo != u:
            return self.j({"error": "دسترسی نیست"}, 403)
        # Unpin others in same video
        for cc in db["comments"].values():
            if cc.get("vid") == vid:
                cc["pinned"] = False
        c["pinned"] = not c.get("pinned", False)
        DB.save(db)
        self.j({"ok": True, "pinned": c["pinned"]})

    # ── Analytics ──
    def ganalytics(self, q):
        u = self.uid()
        if not u:
            return self.j({"error": "unauthorized"}, 401)
        db = DB.load()
        my_vids = [v for v in db["videos"].values() if v["uid"] == u]
        total_views = sum(v.get("views", 0) for v in my_vids)
        total_likes = 0
        total_comments = 0
        for v in my_vids:
            total_likes += sum(1 for l in db["likes"].values() if l["vid"] == v["id"] and l["val"] == 1)
            total_comments += sum(1 for c in db["comments"].values() if c["vid"] == v["id"])
        subs_count = sum(1 for s in db["subs"].values() if s["ch"] == u)
        top_vids = sorted(my_vids, key=lambda x: x.get("views", 0), reverse=True)[:5]
        for v in top_vids:
            self._enrich(db, v)
        self.j({
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "subs_count": subs_count,
            "video_count": len(my_vids),
            "top_videos": top_vids
        })


class Srv(socketserver.TCPServer):
    allow_reuse_address = True
    request_queue_size = 128  # Increased queue size

# ── Admin Panel with Nested Navigation ──
def admin():
    time.sleep(1.5)
    
    # ANSI color codes
    R  = "[0m"     # Reset
    B  = "[1m"     # Bold
    CY = "[96m"    # Cyan
    GR = "[92m"    # Green
    YL = "[93m"    # Yellow
    MG = "[95m"    # Magenta
    RD = "[91m"    # Red
    BL = "[94m"    # Blue
    WH = "[97m"    # White
    
    def inp(l):
        try:
            return input(CY + "  ❯ " + R + l).strip()
        except:
            return ""
    
    def clear():
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def server_header():
        ip = gip()
        db = DB.load()
        uc = len(db["users"])
        vc = len(db["videos"])
        print(B + CY + "  ╔══════════════════════════════════════════════╗" + R)
        print(B + CY + "  ║  " + WH + "YouKo v5.5" + CY + "  -  شبکه ویدیویی WiFi          ║" + R)
        print(B + CY + "  ╠══════════════════════════════════════════════╣" + R)
        print(B + CY + "  ║  " + GR + "محلی:" + WH + " http://127.0.0.1:{}".format(PORT) + CY + "            ║" + R)
        print(B + CY + "  ║  " + GR + "WiFi: " + WH + " http://{}:{}".format(ip, PORT) + CY + "   ║" + R)
        print(B + CY + "  ║  " + YL + "کاربر: " + WH + "{:<4}".format(uc) + YL + "  ویدیو: " + WH + "{:<4}".format(vc) + CY + "                      ║" + R)
        print(B + CY + "  ╚══════════════════════════════════════════════╝" + R)
        print()
    
    def show_main_menu():
        clear()
        server_header()
        print(B + MG + "  ┌─────────────────────────────────┐" + R)
        print(B + MG + "  │" + WH + "       پنل مدیریت YouKo v5.5    " + MG + "│" + R)
        print(B + MG + "  ├─────────────────────────────────┤" + R)
        print(B + MG + "  │" + GR + "  1" + R + "  مدیریت کاربران            " + MG + "│" + R)
        print(B + MG + "  │" + BL + "  2" + R + "  مدیریت ویدیوها            " + MG + "│" + R)
        print(B + MG + "  │" + YL + "  3" + R + "  مدیریت سیستم              " + MG + "│" + R)
        print(B + MG + "  │" + CY + "  4" + R + "  الگوریتم و آمار           " + MG + "│" + R)
        print(B + MG + "  │" + GR + "  5" + R + "  سلامت سرور                " + MG + "│" + R)
        print(B + MG + "  │" + RD + "  0" + R + "  پاک کردن صفحه             " + MG + "│" + R)
        print(B + MG + "  └─────────────────────────────────┘" + R)
    
    def user_menu():
        while True:
            clear()
            server_header()
            print(B + GR + "  ┌─────────────────────────────────┐" + R)
            print(B + GR + "  │" + WH + "        مدیریت کاربران          " + GR + "│" + R)
            print(B + GR + "  ├─────────────────────────────────┤" + R)
            print(B + GR + "  │" + CY + "  1" + R + "  لیست کاربران              " + GR + "│" + R)
            print(B + GR + "  │" + RD + "  2" + R + "  بن / آزاد کردن            " + GR + "│" + R)
            print(B + GR + "  │" + YL + "  3" + R + "  تایید / لغو تایید         " + GR + "│" + R)
            print(B + GR + "  │" + MG + "  4" + R + "  ادمین کردن                " + GR + "│" + R)
            print(B + GR + "  │" + RD + "  5" + R + "  حذف کاربر                 " + GR + "│" + R)
            print(B + GR + "  │" + BL + "  6" + R + "  تغییر رمز                 " + GR + "│" + R)
            print(B + GR + "  │" + YL + "  0" + R + "  برگشت                     " + GR + "│" + R)
            print(B + GR + "  └─────────────────────────────────┘" + R)
            
            ch = inp("انتخاب: ")
            db = DB.load()
            
            if ch == "0":
                return
            elif ch == "1":
                print()
                for uid, u in db["users"].items():
                    flags = ""
                    if u.get("is_admin"): flags += " [ادمین]"
                    if u.get("is_verified"): flags += " [تایید]"
                    if u.get("is_banned"): flags += " [بن]"
                    vc = sum(1 for v in db["videos"].values() if v["uid"] == uid)
                    sc = sum(1 for s in db["subs"].values() if s["ch"] == uid)
                    print("  شناسه:{:<3} کاربری:{:<12} نام:{:<14} ویدیو:{} مشترک:{}{}".format(
                        uid, u["username"], u["display_name"], vc, sc, flags))
                print("  مجموع: {}".format(len(db["users"])))
                inp("\n  Enter برای ادامه...")
            elif ch == "2":
                uid = inp("شناسه کاربر: ")
                if uid in db["users"]:
                    db["users"][uid]["is_banned"] = not db["users"][uid].get("is_banned", False)
                    if db["users"][uid]["is_banned"]:
                        db["sess"] = {k: v for k, v in db["sess"].items() if v["uid"] != uid}
                    DB.save(db)
                    print("  {} -> {}".format(db["users"][uid]["username"], 
                          "بن شد" if db["users"][uid]["is_banned"] else "آزاد شد"))
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "3":
                uid = inp("شناسه: ")
                if uid in db["users"]:
                    db["users"][uid]["is_verified"] = not db["users"][uid].get("is_verified", False)
                    DB.save(db)
                    print("  {}".format("تایید شد" if db["users"][uid]["is_verified"] else "لغو تایید"))
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "4":
                uid = inp("شناسه: ")
                if uid in db["users"]:
                    db["users"][uid]["is_admin"] = not db["users"][uid].get("is_admin", False)
                    DB.save(db)
                    print("  {}".format("ادمین شد" if db["users"][uid]["is_admin"] else "عادی شد"))
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "5":
                uid = inp("شناسه: ")
                if uid in db["users"]:
                    un = db["users"][uid]["username"]
                    del db["users"][uid]
                    db["sess"] = {k: v for k, v in db["sess"].items() if v["uid"] != uid}
                    vids = [v for v, d in db["videos"].items() if d["uid"] == uid]
                    for vid in vids:
                        v = db["videos"][vid]
                        vp = os.path.join(VIDS, os.path.basename(v.get("url", "")))
                        if os.path.exists(vp):
                            os.remove(vp)
                        del db["videos"][vid]
                    db["comments"] = {k: c for k, c in db["comments"].items() if c["uid"] != uid}
                    db["likes"] = {k: l for k, l in db["likes"].items() if l["uid"] != uid}
                    db["subs"] = {k: s for k, s in db["subs"].items() if s["sub"] != uid and s["ch"] != uid}
                    db["notifs"].pop(uid, None)
                    db["hist"].pop(uid, None)
                    DB.save(db)
                    print("  {} حذف شد".format(un))
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "6":
                uid = inp("شناسه: ")
                if uid in db["users"]:
                    pw = inp("رمز جدید: ")
                    if len(pw) >= 4:
                        db["users"][uid]["password"] = hp(pw)
                        DB.save(db)
                        print("  تغییر کرد")
                    else:
                        print("  کوتاه")
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
    
    def video_menu():
        while True:
            clear()
            server_header()
            print(B + BL + "  ┌─────────────────────────────────┐" + R)
            print(B + BL + "  │" + WH + "        مدیریت ویدیوها          " + BL + "│" + R)
            print(B + BL + "  ├─────────────────────────────────┤" + R)
            print(B + BL + "  │" + CY + "  1" + R + "  لیست ویدیوها              " + BL + "│" + R)
            print(B + BL + "  │" + RD + "  2" + R + "  حذف ویدیو                 " + BL + "│" + R)
            print(B + BL + "  │" + YL + "  3" + R + "  مخفی / نمایان             " + BL + "│" + R)
            print(B + BL + "  │" + GR + "  4" + R + "  ویرایش ویدیو              " + BL + "│" + R)
            print(B + BL + "  │" + MG + "  5" + R + "  اسکن پوشه خودکار          " + BL + "│" + R)
            print(B + BL + "  │" + RD + "  6" + R + "  حذف کامنت‌های ویدیو      " + BL + "│" + R)
            print(B + BL + "  │" + YL + "  0" + R + "  برگشت                     " + BL + "│" + R)
            print(B + BL + "  └─────────────────────────────────┘" + R)
            
            ch = inp("انتخاب: ")
            db = DB.load()
            
            if ch == "0":
                return
            elif ch == "1":
                print()
                for vid, v in db["videos"].items():
                    fl = ""
                    if v.get("hidden"): fl += " [مخفی]"
                    if v.get("is_short"): fl += " [کوتاه]"
                    own = db["users"].get(v["uid"], {}).get("username", "?")
                    print("  شناسه:{:<3} عنوان:{:<22} مالک:{:<10} بازدید:{} حجم:{:.1f}MB{}".format(
                        vid, v["title"][:21], own, v["views"], v.get("size_mb", 0), fl))
                print("  مجموع: {}".format(len(db["videos"])))
                inp("\n  Enter برای ادامه...")
            elif ch == "2":
                vid = inp("شناسه ویدیو: ")
                if vid in db["videos"]:
                    v = db["videos"][vid]
                    vp = os.path.join(VIDS, os.path.basename(v.get("url", "")))
                    if os.path.exists(vp):
                        os.remove(vp)
                    if v.get("thumb"):
                        tp = os.path.join(THUMBS, os.path.basename(v["thumb"]))
                        if os.path.exists(tp):
                            os.remove(tp)
                    del db["videos"][vid]
                    db["comments"] = {k: c for k, c in db["comments"].items() if c["vid"] != vid}
                    db["likes"] = {k: l for k, l in db["likes"].items() if l["vid"] != vid}
                    DB.save(db)
                    print("  حذف شد")
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "3":
                vid = inp("شناسه ویدیو: ")
                if vid in db["videos"]:
                    db["videos"][vid]["hidden"] = not db["videos"][vid].get("hidden", False)
                    DB.save(db)
                    print("  {}".format("مخفی" if db["videos"][vid]["hidden"] else "نمایان"))
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "4":
                vid = inp("شناسه ویدیو: ")
                if vid in db["videos"]:
                    v = db["videos"][vid]
                    print("  عنوان فعلی: {}".format(v["title"]))
                    new_title = inp("عنوان جدید (خالی برای عوض نشدن): ")
                    if new_title.strip():
                        v["title"] = new_title.strip()
                    print("  توضیحات فعلی: {}".format(v.get("desc", "")))
                    new_desc = inp("توضیحات جدید (خالی برای عوض نشدن): ")
                    if new_desc.strip():
                        v["desc"] = new_desc.strip()
                    DB.save(db)
                    print("  ذخیره شد")
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "5":
                added = scan_auto_videos()
                print("  {} ویدیو اضافه شد".format(added))
                inp("\n  Enter برای ادامه...")
            elif ch == "6":
                vid = inp("شناسه ویدیو: ")
                cnt = sum(1 for c in db["comments"].values() if c["vid"] == vid)
                db["comments"] = {k: c for k, c in db["comments"].items() if c["vid"] != vid}
                DB.save(db)
                print("  {} کامنت حذف".format(cnt))
                inp("\n  Enter برای ادامه...")
    
    def system_menu():
        while True:
            clear()
            server_header()
            print(B + YL + "  ┌─────────────────────────────────┐" + R)
            print(B + YL + "  │" + WH + "        مدیریت سیستم            " + YL + "│" + R)
            print(B + YL + "  ├─────────────────────────────────┤" + R)
            print(B + YL + "  │" + GR + "  1" + R + "  ثبت نام فعال/غیرفعال      " + YL + "│" + R)
            print(B + YL + "  │" + RD + "  2" + R + "  حالت تعمیر                " + YL + "│" + R)
            print(B + YL + "  │" + CY + "  3" + R + "  حداکثر حجم آپلود          " + YL + "│" + R)
            print(B + YL + "  │" + MG + "  4" + R + "  پاک کردن نشست‌ها          " + YL + "│" + R)
            print(B + YL + "  │" + BL + "  5" + R + "  اعلان عمومی               " + YL + "│" + R)
            print(B + YL + "  │" + GR + "  6" + R + "  پشتیبان‌گیری             " + YL + "│" + R)
            print(B + YL + "  │" + YL + "  0" + R + "  برگشت                     " + YL + "│" + R)
            print(B + YL + "  └─────────────────────────────────┘" + R)
            
            ch = inp("انتخاب: ")
            db = DB.load()
            
            if ch == "0":
                return
            elif ch == "1":
                db["settings"]["reg"] = not db["settings"].get("reg", True)
                DB.save(db)
                print("  ثبت نام: {}".format("فعال" if db["settings"]["reg"] else "غیرفعال"))
                inp("\n  Enter برای ادامه...")
            elif ch == "2":
                db["settings"]["maint"] = not db["settings"].get("maint", False)
                DB.save(db)
                print("  تعمیر: {}".format("روشن" if db["settings"]["maint"] else "خاموش"))
                inp("\n  Enter برای ادامه...")
            elif ch == "3":
                cur = db["settings"].get("max_mb", 500)
                print("  فعلی: {} MB".format(cur))
                try:
                    n = int(inp("حداکثر جدید (MB): "))
                    db["settings"]["max_mb"] = n
                    DB.save(db)
                    print("  تنظیم: {} MB".format(n))
                except:
                    print("  نامعتبر")
                inp("\n  Enter برای ادامه...")
            elif ch == "4":
                db["sess"] = {}
                DB.save(db)
                print("  پاک شد")
                inp("\n  Enter برای ادامه...")
            elif ch == "5":
                msg = inp("متن: ")
                if msg:
                    for uid in db["users"]:
                        if uid not in db["notifs"]:
                            db["notifs"][uid] = []
                        db["notifs"][uid].append({
                            "id": str(uuid.uuid4())[:8], "type": "system",
                            "from": "", "msg": msg, "cr": time.time(), "read": False
                        })
                    DB.save(db)
                    print("  ارسال به {} کاربر".format(len(db["users"])))
                inp("\n  Enter برای ادامه...")
            elif ch == "6":
                bn = "backup_{}.json".format(int(time.time()))
                shutil.copy2(DBFILE, os.path.join(DATA, bn))
                print("  ذخیره: " + bn)
                inp("\n  Enter برای ادامه...")
    
    def algorithm_menu():
        while True:
            clear()
            server_header()
            print(B + CY + "  ┌─────────────────────────────────┐" + R)
            print(B + CY + "  │" + WH + "        الگوریتم و آمار         " + CY + "│" + R)
            print(B + CY + "  ├─────────────────────────────────┤" + R)
            print(B + CY + "  │" + GR + "  1" + R + "  آمار کلی                  " + CY + "│" + R)
            print(B + CY + "  │" + YL + "  2" + R + "  گزارش‌ها                  " + CY + "│" + R)
            print(B + CY + "  │" + RD + "  3" + R + "  حل گزارش                  " + CY + "│" + R)
            print(B + CY + "  │" + BL + "  4" + R + "  رفتار کاربران             " + CY + "│" + R)
            print(B + CY + "  │" + YL + "  0" + R + "  برگشت                     " + CY + "│" + R)
            print(B + CY + "  └─────────────────────────────────┘" + R)
            
            ch = inp("انتخاب: ")
            db = DB.load()
            
            if ch == "0":
                return
            elif ch == "1":
                tv = sum(v["views"] for v in db["videos"].values())
                ts = sum(v.get("size_mb", 0) for v in db["videos"].values())
                sh = sum(1 for v in db["videos"].values() if v.get("is_short"))
                print("\n  کاربران: {} | ویدیو: {} | کوتاه: {} | کامنت: {} | لایک: {}".format(
                    len(db["users"]), len(db["videos"]), sh, len(db["comments"]), len(db["likes"])))
                print("  بازدید کل: {} | حجم: {:.1f}MB | نشست: {} | گزارش: {}".format(
                    tv, ts, len(db["sess"]), len(db["reports"])))
                print("  ثبت نام: {} | تعمیر: {} | حداکثر: {}MB".format(
                    "فعال" if db["settings"].get("reg") else "غیرفعال",
                    "روشن" if db["settings"].get("maint") else "خاموش",
                    db["settings"].get("max_mb", 500)))
                inp("\n  Enter برای ادامه...")
            elif ch == "2":
                for rid, r in db["reports"].items():
                    rp = db["users"].get(r["reporter"], {}).get("username", "?")
                    print("  شناسه:{} نوع:{} هدف:{} از:{} وضعیت:{}".format(
                        rid, r["type"], r["target"], rp, "حل شده" if r.get("resolved") else "بررسی نشده"))
                    if r.get("reason"):
                        print("    دلیل: {}".format(r["reason"][:40]))
                inp("\n  Enter برای ادامه...")
            elif ch == "3":
                rid = inp("شناسه گزارش: ")
                if rid in db["reports"]:
                    db["reports"][rid]["resolved"] = True
                    DB.save(db)
                    print("  حل شد")
                else:
                    print("  یافت نشد")
                inp("\n  Enter برای ادامه...")
            elif ch == "4":
                algo = db.get("algorithm", {})
                print("\n  کاربران با رفتار ثبت شده: {}".format(len(algo)))
                for uid, data in list(algo.items())[:5]:
                    user = db["users"].get(uid, {})
                    prefs = data.get("video_preferences", {})
                    print("  {}: کوتاه={}, بلند={}, AFK={}".format(
                        user.get("username", "?"), prefs.get("short", 0), prefs.get("long", 0),
                        data.get("afk_count", 0)))
                inp("\n  Enter برای ادامه...")
    
    def health_menu():
        while True:
            clear()
            server_header()
            print(B + GR + "  ┌─────────────────────────────────┐" + R)
            print(B + GR + "  │" + WH + "        سلامت سرور               " + GR + "│" + R)
            print(B + GR + "  ├─────────────────────────────────┤" + R)
            print(B + GR + "  │" + CY + "  1" + R + "  بررسی وضعیت               " + GR + "│" + R)
            print(B + GR + "  │" + YL + "  2" + R + "  رفع همه مشکلات            " + GR + "│" + R)
            print(B + GR + "  │" + BL + "  3" + R + "  مشاهده آخرین رفع‌ها       " + GR + "│" + R)
            print(B + GR + "  │" + YL + "  0" + R + "  برگشت                     " + GR + "│" + R)
            print(B + GR + "  └─────────────────────────────────┘" + R)
            
            ch = inp("انتخاب: ")
            
            if ch == "0":
                return
            elif ch == "1":
                issues = check_server_health()
                db = DB.load()
                db["server_health"]["last_check"] = time.time()
                db["server_health"]["issues"] = issues
                DB.save(db)
                
                print("\n  وضعیت سلامت سرور:")
                if issues:
                    for issue in issues:
                        print("  " + RD + "[!]" + R + " {}".format(issue))
                else:
                    print("  " + GR + "[OK]" + R + " هیچ مشکلی یافت نشد")
                
                # Show stats
                try:
                    stat = shutil.disk_usage(DATA)
                    print("\n  فضای دیسک: {:.1f}GB آزاد".format(stat.free / (1024**3)))
                except:
                    pass
                print("  تعداد نشست‌ها: {}".format(len(db["sess"])))
                print("  لاگ بازدید: {}".format(len(db["vlog"])))
                inp("\n  Enter برای ادامه...")
            elif ch == "2":
                print("\n  در حال رفع مشکلات...")
                fixes = fix_all_issues()
                if fixes:
                    for fix in fixes:
                        print("  " + GR + "[OK]" + R + " {}".format(fix))
                else:
                    print("  " + GR + "[OK]" + R + " هیچ مشکلی برای رفع وجود نداشت")
                inp("\n  Enter برای ادامه...")
            elif ch == "3":
                db = DB.load()
                fixes = db.get("server_health", {}).get("fixes", [])
                if fixes:
                    print("\n  آخرین رفع‌ها:")
                    for fix in fixes[-10:]:
                        print("  " + GR + "[OK]" + R + " {}".format(fix))
                else:
                    print("\n  هنوز رفعی انجام نشده")
                inp("\n  Enter برای ادامه...")
    
    # Main loop
    while True:
        show_main_menu()
        ch = inp("انتخاب: ")
        
        if ch == "0":
            clear()
        elif ch == "1":
            user_menu()
        elif ch == "2":
            video_menu()
        elif ch == "3":
            system_menu()
        elif ch == "4":
            algorithm_menu()
        elif ch == "5":
            health_menu()

def main():
    ip = gip()
    srv = Srv(("0.0.0.0", PORT), H)
    B  = "[1m"; CY = "[96m"; GR = "[92m"; WH = "[97m"; R = "[0m"; YL = "[93m"
    print(B + CY + "\n  ╔══════════════════════════════════════════════╗" + R)
    print(B + CY + "  ║  " + WH + "YouKo v5.5" + CY + "  -  شبکه ویدیویی WiFi          ║" + R)
    print(B + CY + "  ╠══════════════════════════════════════════════╣" + R)
    print(B + CY + "  ║  " + GR + "محلی:" + WH + " http://127.0.0.1:{}".format(PORT) + CY + "            ║" + R)
    print(B + CY + "  ║  " + GR + "WiFi: " + WH + " http://{}:{}".format(ip, PORT) + CY + "   ║" + R)
    print(B + CY + "  ║  " + YL + "Ctrl+C = خاموش                             " + CY + "║" + R)
    print(B + CY + "  ╚══════════════════════════════════════════════╝\n" + R)
    t = threading.Thread(target=admin, daemon=True)
    t.start()
    
    # Start auto-scan thread
    def auto_scan_loop():
        while True:
            time.sleep(300)  # Scan every 5 minutes
            try:
                scan_auto_videos()
            except:
                pass
    
    scan_thread = threading.Thread(target=auto_scan_loop, daemon=True)
    scan_thread.start()
    
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  خاموش شد.")
        srv.server_close()

if __name__ == "__main__":
    main()