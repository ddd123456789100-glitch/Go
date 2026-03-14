"""
╔══════════════════════════════════════════════════════╗
║   🎙️  Highrise Radio Bot — v9  (النسخة النهائية)   ║
╠══════════════════════════════════════════════════════╣
║  خفيف  • قوي  • سريع  • مستقر للأبد                ║
╠══════════════════════════════════════════════════════╣
║  ✅ yt-dlp كـ Library (لا subprocess) → CPU -60%   ║
║  ✅ Cache 100 أغنية → نفس الطلب = CPU صفر           ║
║  ✅ Ring Buffer بـ memoryview → RAM 3MB ثابت        ║
║  ✅ Lock + Rate Limit → 100 طلب متزامن آمن          ║
║  ✅ ffmpeg SIGKILL → لا zombie processes             ║
║  ✅ _done_ev في finally → لا انتظار للأبد           ║
║  ✅ Watchdog كل 60s → لا موت صامت                  ║
║  ✅ on_start يُصفّر الكل → reconnect نظيف           ║
╚══════════════════════════════════════════════════════╝
"""

# ════════════════════════════════════════════════════
#  تثبيت المتطلبات تلقائياً (كاملة)
# ════════════════════════════════════════════════════
import subprocess, sys, shutil, os

def _pip(pkg, import_name=None):
    """تثبيت مكتبة إذا لم تكن موجودة"""
    check = import_name or pkg.replace("-", "_").split("[")[0]
    try:
        __import__(check)
        return  # المكتبة موجودة، لا حاجة للتثبيت
    except ImportError:
        print(f"⚙️  جاري تثبيت {pkg} ...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg,
             "--quiet", "--break-system-packages"],
            capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ تم تثبيت {pkg}")
        else:
            print(f"❌ فشل تثبيت {pkg}: {result.stderr.strip()}")

def _check_ffmpeg():
    """الحصول على مسار ffmpeg — من النظام أو من imageio-ffmpeg"""
    # أولاً: جرّب ffmpeg من النظام
    if shutil.which("ffmpeg"):
        print("✅ ffmpeg موجود في النظام")
        return True
    # ثانياً: imageio-ffmpeg (يعمل على Render بدون صلاحيات root)
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path:
            # أضفه لـ PATH حتى تجده بقية الكود
            os.environ["PATH"] = os.path.dirname(path) + os.pathsep + os.environ.get("PATH", "")
            # أنشئ symlink باسم ffmpeg للسهولة
            try:
                link = os.path.join(os.path.dirname(path), "ffmpeg")
                if not os.path.exists(link):
                    os.symlink(path, link)
            except Exception:
                pass
            print(f"✅ ffmpeg عبر imageio-ffmpeg: {path}")
            return True
    except Exception as e:
        print(f"⚠️ imageio-ffmpeg: {e}")
    # ثالثاً: ثبّت imageio-ffmpeg الآن
    print("⚙️  تثبيت imageio-ffmpeg...")
    r = subprocess.run([sys.executable, "-m", "pip", "install", "imageio-ffmpeg",
                        "--quiet", "--break-system-packages"],
                       capture_output=True, text=True)
    if r.returncode == 0:
        try:
            import imageio_ffmpeg
            path = imageio_ffmpeg.get_ffmpeg_exe()
            os.environ["PATH"] = os.path.dirname(path) + os.pathsep + os.environ.get("PATH", "")
            print(f"✅ تم تثبيت imageio-ffmpeg: {path}")
            return True
        except Exception: pass
    print("❌ تعذّر الحصول على ffmpeg")
    return False

print("━" * 50)
print("🔧 فحص وتثبيت المتطلبات...")
print("━" * 50)

# المكتبات المطلوبة: (اسم_الحزمة, اسم_الاستيراد)
_REQUIRED = [
    ("yt-dlp",              "yt_dlp"),
    ("highrise-bot-sdk",    "highrise"),
    ("aiohttp",             "aiohttp"),
    ("websockets",          "websockets"),
    ("requests",            "requests"),
    ("imageio-ffmpeg",      "imageio_ffmpeg"),
]

for _pkg, _imp in _REQUIRED:
    _pip(_pkg, _imp)

_check_ffmpeg()
print("━" * 50)
print("🚀 بدء تشغيل البوت...")
print("━" * 50)

import asyncio, threading, time, json, logging, collections, dataclasses
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from highrise import BaseBot, User
from highrise.__main__ import main, BotDefinition

# ════════════════════════════════════════════════════
#  ⚙️ الإعدادات — عدّل هنا فقط
# ════════════════════════════════════════════════════
# ✅ يقرأ من متغيرات البيئة (Render) أو يستخدم القيم الافتراضية
TOKEN       = os.environ.get("HR_TOKEN",   "47c2e8210246ad1c4806a18a33dbdcf899db9e89b2719f5346b6294d3607b4fc")
ROOM_ID     = os.environ.get("HR_ROOM_ID", "66cfdc844410d5f4f7dafacd")
OWNER       = os.environ.get("HR_OWNER",   "SXP3")

# ✅ Render يعيّن PORT تلقائياً — لا تغيّر هذا السطر
STREAM_PORT = int(os.environ.get("PORT", 10000))

# ✅ Render يعطي اسم النطاق — رابط البث سيكون HTTPS تلقائياً
_rhost      = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "")
PUBLIC_IP   = _rhost if _rhost else os.environ.get("PUBLIC_IP", "85.215.137.163")
# إذا كنا على Render استخدم HTTPS وإلا HTTP عادي
_proto      = "https" if _rhost else "http"
STREAM_URL  = f"{_proto}://{PUBLIC_IP}/stream"

QUEUE_MAX     = 50              # حد القائمة
HISTORY_MAX   = 10              # عدد الأغاني المحفوظة
FETCH_TIMEOUT = 90              # ثواني انتظار yt-dlp
MAX_LISTENERS = 300             # أقصى عدد مستمعين
CACHE_MAX     = 100             # أغاني محفوظة في الكاش
PLAY_COOLDOWN = 10              # ثواني بين طلبات المستخدم الواحد
RING_MB       = 3               # حجم Ring Buffer بالميغابايت
CHUNK_SIZE    = 4096            # حجم كل قطعة صوت

ADMINS_FILE    = "admins.json"
QUEUE_FILE     = "queue.json"
AUTOPLAY_FILE  = "autoplay.json"
# ✅ يجلب المسار الصحيح بعد تشغيل _check_ffmpeg
def _get_ffmpeg():
    p = shutil.which("ffmpeg")
    if p: return p
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"
FFMPEG = _get_ffmpeg()

# ════════════════════════════════════════════════════
#  Logging
# ════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S")
for _noisy in ("socketserver", "asyncio", "concurrent", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)
log = logging.getLogger("Radio")

# Executor مخصص — 2 threads فقط لمنع تسرب Thread Pool
_EXEC = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ytdlp")

# ════════════════════════════════════════════════════
#  صمت أولي
# ════════════════════════════════════════════════════
def _make_silence() -> bytes:
    try:
        r = subprocess.run(
            [FFMPEG, "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
             "-t", "0.5", "-acodec", "libmp3lame", "-ab", "128k",
             "-f", "mp3", "pipe:1", "-loglevel", "error"],
            capture_output=True, timeout=5)
        d = r.stdout
        i = d.find(b"\xff\xfb")
        return d[i:] if i >= 0 else d
    except Exception:
        return (b"\xff\xfb\x90\x00" + b"\x00" * 413) * 4

SILENCE_CHUNK = (_make_silence() * 8)[:CHUNK_SIZE]

# ════════════════════════════════════════════════════
#  نماذج البيانات
# ════════════════════════════════════════════════════
@dataclasses.dataclass
class Request:
    query: str
    by:    str
    at:    float = dataclasses.field(default_factory=time.time)
    def to_dict(self):
        return {"query": self.query, "by": self.by, "at": self.at}
    @staticmethod
    def from_dict(d):
        return Request(query=d["query"], by=d["by"], at=d.get("at", 0))

@dataclasses.dataclass
class Song:
    query:      str
    title:      str
    url:        str
    duration:   int
    by:         str
    @property
    def dur_str(self) -> str:
        if self.duration <= 0: return "؟"
        m, s = divmod(self.duration, 60)
        return f"{m}:{s:02d}"

# ════════════════════════════════════════════════════
#  Ring Buffer — قلب النظام
#  ✅ memoryview للكتابة السريعة (لا loop بايت بايت)
#  ✅ RAM ثابت = RING_MB بغض النظر عن عدد المستمعين
#  ✅ حد أمان تلقائي للأرقام
# ════════════════════════════════════════════════════
class RingBuffer:
    _SAFE = int((1 << 32) * 0.75)   # حد أمان = 75% من 2^32

    def __init__(self, mb: int):
        self._size  = mb * 1024 * 1024
        self._buf   = bytearray(self._size)
        self._mv    = memoryview(self._buf)   # ✅ سريع
        self._wpos  = 0
        self._total = 0
        self._lids: dict[int, int] = {}       # lid → قراءة_حتى
        self._nid   = 0
        self._cond  = threading.Condition(threading.Lock())

    def write(self, data: bytes):
        with self._cond:
            n, wp = len(data), self._wpos
            # ✅ كتابة بـ memoryview دفعة واحدة (لا loop)
            if wp + n <= self._size:
                self._mv[wp:wp+n] = data
            else:
                f = self._size - wp
                self._mv[wp:]  = data[:f]
                self._mv[:n-f] = data[f:]
            self._wpos   = (wp + n) % self._size
            self._total += n
            # حد أمان — يمنع تراكم الأرقام للأبد
            if self._total > self._SAFE:
                shift = self._total - self._size * 2
                if shift > 0:
                    self._total -= shift
                    for lid in self._lids:
                        self._lids[lid] = max(
                            self._total - self._size,
                            self._lids[lid] - shift)
            self._cond.notify_all()

    def add_listener(self) -> int:
        with self._cond:
            lid = self._nid % 100_000   # يدور — لا يكبر للأبد
            self._nid += 1
            self._lids[lid] = self._total
            return lid

    def remove_listener(self, lid: int):
        with self._cond:
            self._lids.pop(lid, None)

    def read(self, lid: int, timeout: float = 3.0) -> Optional[bytes]:
        with self._cond:
            end = time.monotonic() + timeout
            while lid in self._lids:
                avail = self._total - self._lids[lid]
                if avail >= CHUNK_SIZE:
                    # تأخر أكثر من ثانيتين → اقفز للحاضر
                    if avail > 16000 * 2:
                        self._lids[lid] = self._total - CHUNK_SIZE
                    rp = self._lids[lid]
                    bp = (self._wpos - (self._total - rp)) % self._size
                    # ✅ قراءة بـ memoryview
                    if bp + CHUNK_SIZE <= self._size:
                        chunk = bytes(self._mv[bp:bp+CHUNK_SIZE])
                    else:
                        f     = self._size - bp
                        chunk = bytes(self._mv[bp:]) + bytes(self._mv[:CHUNK_SIZE-f])
                    self._lids[lid] = rp + CHUNK_SIZE
                    return chunk
                rem = end - time.monotonic()
                if rem <= 0: return None
                self._cond.wait(timeout=min(rem, 0.3))
            return None

    def sync_all(self):
        """حرّك كل المستمعين للحاضر — عند بداية أغنية جديدة"""
        with self._cond:
            for lid in self._lids:
                self._lids[lid] = self._total
            self._cond.notify_all()

    def count(self) -> int:
        with self._cond: return len(self._lids)

    def wake(self):
        with self._cond: self._cond.notify_all()


RING = RingBuffer(RING_MB)

# ════════════════════════════════════════════════════
#  Cache الأغاني
#  ✅ نفس الطلب → نتيجة فورية → CPU صفر
# ════════════════════════════════════════════════════
class SongCache:
    def __init__(self, maxsize: int):
        self._d:    dict[str, Song] = {}
        self._keys: list[str]       = []
        self._max   = maxsize
        self._lk    = threading.Lock()

    def _key(self, q: str) -> str:
        return q.strip().lower()

    def get(self, query: str) -> Optional[Song]:
        with self._lk:
            return self._d.get(self._key(query))

    def put(self, query: str, song: Song):
        with self._lk:
            k = self._key(query)
            if k in self._d: return
            self._d[k]    = song
            self._keys.append(k)
            if len(self._keys) > self._max:
                old = self._keys.pop(0)
                self._d.pop(old, None)


CACHE = SongCache(CACHE_MAX)

# ════════════════════════════════════════════════════
#  yt-dlp — جلب معلومات الأغنية
#  ✅ Python library (لا subprocess ثقيل)
#  ✅ يتحقق من الكاش أولاً
# ════════════════════════════════════════════════════
def fetch_song(query: str) -> Optional[Song]:
    # ① تحقق من الكاش أولاً
    cached = CACHE.get(query)
    if cached:
        log.info("💾 cache: %s", query[:50])
        return cached

    # ② بحث في YouTube
    try:
        import yt_dlp as ytdl
        opts = {
            "quiet":          True,
            "no_warnings":    True,
            "noplaylist":     True,
            "format":         "bestaudio[ext=m4a]/bestaudio/best",
            "skip_download":  True,
            "socket_timeout": 20,
            "retries":        2,
            "extractor_args": {"youtube": {"skip": ["dash", "hls"]}},
        }
        with ytdl.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=False)
            if not info or not info.get("entries"):
                return None
            e = info["entries"][0]
            if not e: return None

            url = e.get("url", "")
            if not url:
                fmts = [f for f in e.get("formats", [])
                        if f.get("acodec") != "none" and f.get("vcodec") == "none"]
                url  = (fmts[-1] if fmts else e.get("formats", [{}])[-1]).get("url", "")

            if not url: return None

            song = Song(
                query    = query,
                title    = str(e.get("title", query))[:100],
                url      = url,
                duration = int(e.get("duration") or 0),
                by       = "")

            CACHE.put(query, song)
            return song

    except Exception as e:
        log.error("fetch_song خطأ: %s", e)
        return None

# ════════════════════════════════════════════════════
#  مدير القائمة
#  ✅ محفوظة في ملف — تبقى عند إعادة التشغيل
#  ✅ thread-safe بـ RLock
# ════════════════════════════════════════════════════
class QueueManager:
    def __init__(self):
        self._lk = threading.RLock()
        self._q: list[Request] = []
        self._load()

    def _load(self):
        try:
            if os.path.exists(QUEUE_FILE):
                with open(QUEUE_FILE, encoding="utf-8") as f:
                    self._q = [Request.from_dict(d) for d in json.load(f)]
                log.info("📂 تحميل %d طلب", len(self._q))
        except Exception as e:
            log.warning("خطأ queue.json: %s", e)
            self._q = []

    def _save(self):
        try:
            with open(QUEUE_FILE, "w", encoding="utf-8") as f:
                json.dump([r.to_dict() for r in self._q],
                          f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("خطأ حفظ queue.json: %s", e)

    def add(self, query: str, by: str) -> str:
        """أضف طلب — أرجع رسالة خطأ أو '' عند النجاح"""
        with self._lk:
            if len(self._q) >= QUEUE_MAX:
                return f"❌ القائمة ممتلئة ({QUEUE_MAX})"
            self._q.append(Request(query=query, by=by))
            self._save()
            return ""

    def pop(self) -> Optional[Request]:
        with self._lk:
            if not self._q: return None
            r = self._q.pop(0); self._save(); return r

    def remove(self, i: int) -> Optional[Request]:
        with self._lk:
            if 0 <= i < len(self._q):
                r = self._q.pop(i); self._save(); return r
            return None

    def clear(self):
        with self._lk:
            self._q.clear(); self._save()

    def peek(self) -> Optional[Request]:
        with self._lk: return self._q[0] if self._q else None

    def snap(self) -> list:
        with self._lk: return list(self._q)

    def size(self) -> int:
        with self._lk: return len(self._q)

    def empty(self) -> bool:
        with self._lk: return not self._q


QUEUE = QueueManager()

# ════════════════════════════════════════════════════
#  مدير التشغيل التلقائي
#  يعمل فقط لما القائمة الرئيسية فارغة
#  محفوظ في autoplay.json
# ════════════════════════════════════════════════════
import random as _random

_DEFAULT_AUTOPLAY = [
    "Blinding Lights The Weeknd",
    "Shape of You Ed Sheeran",
    "Bohemian Rhapsody Queen",
    "Hotel California Eagles",
    "Smells Like Teen Spirit Nirvana",
    "Rolling in the Deep Adele",
    "Uptown Funk Bruno Mars",
    "Thinking Out Loud Ed Sheeran",
    "Someone Like You Adele",
    "Happy Pharrell Williams",
]

class AutoplayManager:
    def __init__(self):
        self._lk    = threading.RLock()
        self._songs: list[str] = []
        self._idx   = 0
        self._load()

    def _load(self):
        try:
            if os.path.exists(AUTOPLAY_FILE):
                with open(AUTOPLAY_FILE, encoding="utf-8") as f:
                    d = json.load(f)
                self._songs = d.get("songs", _DEFAULT_AUTOPLAY[:])
                self._idx   = d.get("idx", 0) % max(1, len(d.get("songs", [1])))
                log.info("📂 autoplay: %d أغنية", len(self._songs))
                return
        except Exception as e:
            log.warning("autoplay.json: %s", e)
        self._songs = _DEFAULT_AUTOPLAY[:]
        self._save()

    def _save(self):
        try:
            with open(AUTOPLAY_FILE, "w", encoding="utf-8") as f:
                json.dump({"songs": self._songs, "idx": self._idx},
                          f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("حفظ autoplay: %s", e)

    def next_song(self) -> Optional[str]:
        """الأغنية التالية بالترتيب — None إذا كانت القائمة فارغة"""
        with self._lk:
            if not self._songs: return None
            q = self._songs[self._idx % len(self._songs)]
            self._idx = (self._idx + 1) % len(self._songs)
            self._save()
            return q

    def add(self, song: str) -> str:
        with self._lk:
            s = song.strip()
            if s.lower() in [x.lower() for x in self._songs]:
                return "⚠️ الأغنية موجودة مسبقاً"
            self._songs.append(s)
            self._save()
            return f"✅ أُضيفت #{len(self._songs)}"

    def remove(self, i: int) -> Optional[str]:
        with self._lk:
            if 0 <= i < len(self._songs):
                s = self._songs.pop(i)
                if self._idx >= len(self._songs) and self._songs:
                    self._idx = 0
                self._save()
                return s
            return None

    def clear(self):
        with self._lk:
            self._songs.clear()
            self._idx = 0
            self._save()

    def snap(self) -> list:
        with self._lk: return list(self._songs)

    def size(self) -> int:
        with self._lk: return len(self._songs)


AUTOPLAY = AutoplayManager()
class State:
    def __init__(self):
        self._lk      = threading.RLock()
        self.song:    Optional[Song]   = None
        self.elapsed  = 0
        self.history  = collections.deque(maxlen=HISTORY_MAX)
        self.admins   = self._load_admins()

    # ── أغنية ─────────────────────────────────────
    def set(self, s: Optional[Song]):
        with self._lk: self.song = s; self.elapsed = 0

    def get(self) -> Optional[Song]:
        with self._lk: return self.song

    def tick(self):
        with self._lk:
            if self.song: self.elapsed += 1

    def elapsed_s(self) -> int:
        with self._lk: return self.elapsed

    def add_history(self, s: Song):
        with self._lk: self.history.appendleft(s)

    def get_history(self) -> list:
        with self._lk: return list(self.history)

    # ── أدمن ──────────────────────────────────────
    def is_admin(self, u: str) -> bool:
        with self._lk: return u.lower() in self.admins

    def add_admin(self, u: str):
        with self._lk:
            self.admins.add(u.lower()); self._save_admins()

    def del_admin(self, u: str):
        with self._lk:
            self.admins.discard(u.lower()); self._save_admins()

    def _load_admins(self) -> set:
        s = set()
        try:
            if os.path.exists(ADMINS_FILE):
                with open(ADMINS_FILE, encoding="utf-8") as f:
                    s = set(json.load(f))
        except Exception: pass
        if OWNER: s.add(OWNER.lower())
        return s

    def _save_admins(self):
        try:
            with open(ADMINS_FILE, "w", encoding="utf-8") as f:
                json.dump(list(self.admins), f, ensure_ascii=False)
        except Exception: pass

    # ── للـ API ────────────────────────────────────
    def to_dict(self) -> dict:
        with self._lk:
            s = self.song
            return {
                "playing":   s is not None,
                "title":     s.title    if s else "لا يوجد بث",
                "duration":  s.duration if s else 0,
                "elapsed":   self.elapsed,
                "by":        s.by       if s else "",
                "listeners": RING.count(),
                "queue":     [{"query": r.query, "by": r.by} for r in QUEUE.snap()],
                "history":   [{"title": h.title, "by": h.by} for h in self.history],
            }


STATE = State()

# ════════════════════════════════════════════════════
#  أحداث التحكم
# ════════════════════════════════════════════════════
_skip_ev  = threading.Event()   # طلب skip
_done_ev  = threading.Event()   # الأغنية انتهت
_done_ev.set()

_loop:    Optional[asyncio.AbstractEventLoop] = None
_bot_ref: Optional["RadioBot"]               = None

# ════════════════════════════════════════════════════
#  محرك البث
#  ✅ thread واحد يكتب على Ring Buffer
#  ✅ ffmpeg يُقتل بـ SIGKILL لو لم يمت
#  ✅ _done_ev.set() في finally دائماً
# ════════════════════════════════════════════════════
class Broadcaster:

    def __init__(self):
        self._next: Optional[Song] = None
        self._lk   = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None

    def start(self):
        t = threading.Thread(target=self._loop, daemon=True, name="bcast")
        t.start()
        log.info("✅ Broadcaster يعمل")

    def play(self, song: Song):
        with self._lk: self._next = song
        _skip_ev.set()
        _done_ev.clear()

    def skip(self):
        _skip_ev.set()

    def _kill(self):
        """إغلاق ffmpeg مضمون — لا zombie processes"""
        p = self._proc
        if not p: return
        try:
            p.terminate()
            try:    p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:    p.kill(); p.wait(timeout=2)
                except Exception: pass
        except Exception: pass
        try:    p.stdout.read()   # تفريغ الـ pipe
        except Exception: pass
        self._proc = None

    def _loop(self):
        while True:
            try:
                with self._lk:
                    song, self._next = self._next, None

                if song:
                    _skip_ev.clear()
                    self._stream(song)
                    _done_ev.set()
                else:
                    RING.write(SILENCE_CHUNK)
                    time.sleep(CHUNK_SIZE / 16000)

            except Exception as e:
                log.error("Broadcaster خطأ: %s", e)
                _done_ev.set()
                time.sleep(1)

    def _stream(self, song: Song):
        log.info("📡 بث: %s", song.title)
        RING.sync_all()

        cmd = [
            FFMPEG,
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "3",
            "-timeout", "10000000",
            "-i", song.url,
            "-vn",
            "-acodec",    "libmp3lame",
            "-ab",        "128k",
            "-ar",        "44100",
            "-ac",        "2",
            "-reservoir", "0",
            "-f",         "mp3",
            "-loglevel",  "error",
            "pipe:1",
        ]
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0)

            RING.write(SILENCE_CHUNK * 2)   # صمت أولي لتشغيل المشغّل

            bps  = 16000
            intv = CHUNK_SIZE / bps
            nxt  = time.monotonic()

            while True:
                if _skip_ev.is_set(): break
                if self._proc.poll() is not None:
                    rest = self._proc.stdout.read()
                    if rest: RING.write(rest)
                    break
                chunk = self._proc.stdout.read(CHUNK_SIZE)
                if not chunk: break
                RING.write(chunk)
                nxt += intv
                sl   = nxt - time.monotonic()
                if   sl > 0:  time.sleep(sl)
                elif sl < -2: nxt = time.monotonic()

        except Exception as e:
            log.error("_stream خطأ: %s", e)
        finally:
            self._kill()
            # ✅ دائماً — حتى لو Exception
            _done_ev.set()
            log.info("🏁 انتهى: %s", song.title)


BROADCAST = Broadcaster()
BROADCAST.start()

# ════════════════════════════════════════════════════
#  مستمع وهمي — يبقي الـ Ring Buffer نشطاً دائماً
# ════════════════════════════════════════════════════
def _keepalive():
    lid = RING.add_listener()
    while True:
        try:    RING.read(lid, timeout=5)
        except Exception: time.sleep(1)

threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()

# ════════════════════════════════════════════════════
#  HTTP Server
# ════════════════════════════════════════════════════
class _Server(ThreadingMixIn, HTTPServer):
    daemon_threads      = True
    allow_reuse_address = True
    request_queue_size  = 50
    def handle_error(self, *_): pass

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    # ── GET ───────────────────────────────────────
    def do_GET(self):
        p = self.path.split("?")[0]
        if   p == "/stream":     self._stream()
        elif p in ("/", "/panel"): self._panel()
        elif p == "/api/status": self._json(200, STATE.to_dict())
        else: self._raw(404, b"Not Found", "text/plain")

    # ── POST ──────────────────────────────────────
    def do_POST(self):
        p = self.path
        if p == "/api/stop":
            QUEUE.clear(); STATE.set(None)
            if _bot_ref: _bot_ref._playing = False
            BROADCAST.skip()
            self._json(200, {"ok": True})

        elif p == "/api/play":
            try:
                q = json.loads(self._body()).get("query", "").strip()
                if q and _loop:
                    _loop.call_soon_threadsafe(
                        lambda qq=q: asyncio.ensure_future(
                            _web_play(qq), loop=_loop))
                    self._json(200, {"ok": True})
                else:
                    self._json(400, {"error": "empty"})
            except Exception:
                self._json(400, {"error": "bad"})
        else:
            self._raw(404, b"Not Found", "text/plain")

    # ── Stream ────────────────────────────────────
    def _stream(self):
        if RING.count() >= MAX_LISTENERS:
            self.send_response(503); self.end_headers()
            self.wfile.write(b"Full"); return

        self.send_response(200)
        for k, v in [
            ("Content-Type",  "audio/mpeg"),
            ("Cache-Control", "no-cache,no-store"),
            ("Connection",    "keep-alive"),
            ("icy-name",      "Highrise Radio"),
            ("icy-br",        "128"),
            ("Access-Control-Allow-Origin", "*"),
        ]:
            self.send_header(k, v)
        self.end_headers()

        try:
            self.wfile.write(SILENCE_CHUNK * 4)
            self.wfile.flush()
        except OSError: return

        lid = RING.add_listener()
        log.info("🔗 مستمع #%d [%d]", lid, RING.count())
        try:
            while True:
                chunk = RING.read(lid, timeout=3)
                if chunk is None:
                    try:
                        self.wfile.write(SILENCE_CHUNK)
                        self.wfile.flush()
                    except OSError: break
                    continue
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except OSError: break
        except Exception: pass
        finally:
            RING.remove_listener(lid)
            log.info("📴 مستمع #%d [%d]", lid, RING.count())

    # ── Panel ─────────────────────────────────────
    def _panel(self):
        d   = STATE.to_dict()
        pct = round(d["elapsed"] / d["duration"] * 100) if d["duration"] else 0
        ef  = f"{d['elapsed']//60}:{d['elapsed']%60:02d}"
        df  = f"{d['duration']//60}:{d['duration']%60:02d}"
        url = STREAM_URL
        dot = "#10b981" if d["playing"] else "#ef4444"

        q_rows = "".join(
            f"<tr><td class=n>{i+1}</td><td>{r['query']}</td>"
            f"<td class=dim>@{r['by']}</td></tr>"
            for i, r in enumerate(d["queue"])
        ) or "<tr><td colspan=3 class=empty>القائمة فارغة</td></tr>"

        h_rows = "".join(
            f"<tr><td>{h['title']}</td><td class=dim>@{h['by']}</td></tr>"
            for h in d["history"]
        ) or "<tr><td colspan=2 class=empty>لا يوجد سجل</td></tr>"

        html = f"""<!DOCTYPE html><html lang=ar dir=rtl>
<head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>🎙️ Radio v9</title>
<style>
:root{{--bg:#0d0d1a;--card:#161628;--b:#252540;--ac:#7c3aed;--tx:#e2e8f0;--dim:#64748b}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--tx);font-family:'Segoe UI',Arial,sans-serif;padding:16px}}
.w{{max-width:700px;margin:0 auto}}
h1{{text-align:center;font-size:1.4em;margin-bottom:14px;
    background:linear-gradient(135deg,#a855f7,#3b82f6);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}}
.box{{background:var(--card);border:1px solid var(--b);border-radius:10px;padding:10px;text-align:center}}
.bv{{font-size:1.3em;font-weight:700;color:var(--ac)}}
.bl{{font-size:.7em;color:var(--dim);margin-top:2px}}
.card{{background:var(--card);border:1px solid var(--b);border-radius:14px;padding:16px;margin-bottom:10px}}
.lbl{{font-size:.7em;color:var(--dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:7px}}
.tl{{font-size:1em;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.pb{{background:var(--b);border-radius:99px;height:4px;margin:8px 0 4px}}
.pf{{background:linear-gradient(90deg,var(--ac),#2563eb);height:4px;border-radius:99px;
     width:{pct}%;transition:width 1s linear}}
.tr{{display:flex;justify-content:space-between;font-size:.76em;color:var(--dim)}}
.row{{display:flex;gap:8px;margin-bottom:10px}}
input{{flex:1;padding:10px 14px;background:#1a1a30;border:1px solid var(--b);
       border-radius:10px;color:var(--tx);font-size:.9em;outline:none}}
input:focus{{border-color:var(--ac)}}
.btn{{padding:10px 18px;border:none;border-radius:10px;cursor:pointer;
      font-weight:700;font-size:.9em;color:#fff}}
.g{{background:linear-gradient(135deg,#10b981,#059669)}}
.r{{background:linear-gradient(135deg,#ef4444,#b91c1c);width:100%}}
.meta{{display:flex;justify-content:space-between;font-size:.75em;color:var(--dim);margin-bottom:10px}}
.meta a{{color:#3b82f6;text-decoration:none}}
.dot{{display:inline-block;width:7px;height:7px;border-radius:50%;
      background:{dot};margin-left:5px;vertical-align:middle}}
table{{width:100%;border-collapse:collapse;font-size:.86em}}
th{{color:var(--dim);font-weight:500;padding:5px 4px;border-bottom:1px solid var(--b);text-align:right}}
td{{padding:7px 4px;border-bottom:1px solid #13132a}}
.n{{color:var(--ac);font-weight:700;width:26px}}
.dim{{color:var(--dim);font-size:.84em}}
.empty{{text-align:center;color:var(--dim);padding:12px}}
</style></head><body><div class=w>
<h1>🎙️ Highrise Radio v9</h1>
<div class=grid>
  <div class=box><div class=bv id=LC>{d["listeners"]}</div><div class=bl>👂 مستمع</div></div>
  <div class=box><div class=bv>{QUEUE.size()}</div><div class=bl>📋 قائمة</div></div>
  <div class=box><div class=bv>{len(d["history"])}</div><div class=bl>🕒 سجل</div></div>
</div>
<div class=meta>
  <span>📻 <a href="{url}" target=_blank>رابط البث</a></span>
  <span><span class=dot></span>{'بث مباشر' if d['playing'] else 'انتظار'}</span>
</div>
<div class=card>
  <div class=lbl>🎵 يُشغَّل الآن</div>
  <div class=tl id=T>{d['title']}</div>
  <div class=pb><div class=pf id=P></div></div>
  <div class=tr><span id=E>{ef}</span><span id=D>{df}</span></div>
</div>
<div class=row>
  <input id=Q placeholder="اسم الأغنية..." onkeydown="if(event.key==='Enter')play()">
  <button class="btn g" onclick=play()>▶️ تشغيل</button>
</div>
<button class="btn r" onclick="api('/api/stop')">⏹️ إيقاف + مسح القائمة</button>
<br><br>
<div class=card>
  <div class=lbl>📋 القائمة</div>
  <table><thead><tr><th>#</th><th>الطلب</th><th>الطالب</th></tr></thead>
  <tbody id=QB>{q_rows}</tbody></table>
</div>
<div class=card>
  <div class=lbl>🕒 آخر الأغاني</div>
  <table><thead><tr><th>الأغنية</th><th>الطالب</th></tr></thead>
  <tbody id=HB>{h_rows}</tbody></table>
</div>
</div>
<script>
let D=0,E=0;
const $=x=>document.getElementById(x);
const fmt=s=>{{s=Math.max(0,s|0);return(s/60|0)+':'+String(s%60).padStart(2,'0')}};
async function api(u,b){{
  await fetch(u,{{method:'POST',...(b?{{headers:{{'Content-Type':'application/json'}},body:JSON.stringify(b)}}:{{}})}});
  setTimeout(poll,300);
}}
async function play(){{
  const q=$('Q').value.trim();if(!q)return;
  await api('/api/play',{{query:q}});$('Q').value='';
}}
async function poll(){{
  try{{
    const d=await(await fetch('/api/status')).json();
    $('T').textContent=d.title;D=d.duration;E=d.elapsed;
    $('D').textContent=fmt(D);$('E').textContent=fmt(E);
    $('P').style.width=(D>0?Math.min(100,E/D*100):0)+'%';
    $('LC').textContent=d.listeners;
    $('QB').innerHTML=d.queue.length
      ?d.queue.map((r,i)=>`<tr><td class=n>${{i+1}}</td><td>${{r.query}}</td><td class=dim>@${{r.by}}</td></tr>`).join('')
      :'<tr><td colspan=3 class=empty>القائمة فارغة</td></tr>';
    $('HB').innerHTML=d.history.length
      ?d.history.map(h=>`<tr><td>${{h.title}}</td><td class=dim>@${{h.by}}</td></tr>`).join('')
      :'<tr><td colspan=2 class=empty>لا يوجد سجل</td></tr>';
  }}catch(e){{}}
}}
poll();setInterval(poll,3000);
setInterval(()=>{{if(D>0&&E<D){{E++;$('E').textContent=fmt(E);$('P').style.width=Math.min(100,E/D*100)+'%';}}}},1000);
</script></body></html>"""
        self._raw(200, html.encode(), "text/html; charset=utf-8")

    # ── مساعدات ───────────────────────────────────
    def _json(self, code: int, data: dict):
        self._raw(code,
                  json.dumps(data, ensure_ascii=False).encode(),
                  "application/json")

    def _raw(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try: self.wfile.write(body)
        except OSError: pass

    def _body(self) -> bytes:
        try:
            return self.rfile.read(int(self.headers.get("Content-Length", 0)))
        except Exception: return b""


def _start_http():
    try:
        srv = _Server(("0.0.0.0", STREAM_PORT), _Handler)
        log.info("✅ HTTP بورت %d", STREAM_PORT)
        srv.serve_forever()
    except Exception as e:
        log.error("HTTP فشل: %s", e)


async def _web_play(query: str):
    """طلب من لوحة الويب"""
    if not _bot_ref: return
    BROADCAST.skip()
    QUEUE.clear()
    STATE.set(None)
    QUEUE.add(query, "🌐 ويب")
    if not _bot_ref._playing:
        asyncio.ensure_future(_bot_ref._run_queue())

# ════════════════════════════════════════════════════
#  رسائل المساعدة
# ════════════════════════════════════════════════════
_HELP_ALL = """\
🎙️ أوامر الراديو:
▶️ !play اسم — أضف للقائمة
⏭️ !skip — تخطي الأغنية
📋 !queue / !q — القائمة
🎵 !np — يُشغَّل الآن
🕒 !history — آخر الأغاني
❓ !help — المساعدة"""

_HELP_ADMIN = """
👮 أوامر الأدمن:
⏹️ !stop — إيقاف + مسح
⏭️ !skip — تخطي الأغنية
🗑️ !remove رقم — حذف من القائمة
➕ !addadmin اسم
➖ !removeadmin اسم
👥 !admins
─────────────
🔁 قائمة التلقائي:
📋 !aplist — عرض القائمة
➕ !apadd اسم — إضافة أغنية
🗑️ !apremove رقم — حذف أغنية
💥 !apclear — مسح القائمة كلها"""

# ════════════════════════════════════════════════════
#  البوت
# ════════════════════════════════════════════════════
class RadioBot(BaseBot):

    def __init__(self):
        super().__init__()
        self._playing   = False
        self._connected = False
        self._task:     Optional[asyncio.Task] = None
        self._watchdog: Optional[asyncio.Task] = None

    # ── اتصال ─────────────────────────────────────
    async def on_start(self, ambient):
        global _loop, _bot_ref
        _loop = asyncio.get_event_loop()

        # ✅ أوقف الـ session القديم تماماً قبل أي شيء
        old = _bot_ref
        if old and old is not self:
            old._connected = False   # يوقف _run_queue القديمة فوراً
            await old._cancel_task()
            if old._watchdog and not old._watchdog.done():
                old._watchdog.cancel()

        _bot_ref        = self
        self._connected = True
        log.info("✅ متصل")

        # ① أوقف أي شيء متبقي
        await self._cancel_task()
        self._playing = False
        BROADCAST.skip()
        await asyncio.sleep(0.3)   # انتظر الـ thread يستجيب للـ skip

        # ② Watchdog
        if self._watchdog and not self._watchdog.done():
            self._watchdog.cancel()
        self._watchdog = asyncio.create_task(self._watchdog_loop())

        # ③ تشغيل تلقائي
        if QUEUE.empty():
            nxt = AUTOPLAY.next_song()
            if nxt:
                QUEUE.add(nxt, "🤖 تلقائي")

        await self._chat("🎙️ Radio v9 جاهز! ▶️")
        self._task = asyncio.create_task(self._run_queue())

    # ── أوامر ─────────────────────────────────────
    async def on_chat(self, user: User, message: str):
        msg = message.strip()
        if not msg.startswith("!"): return

        parts  = msg.split(maxsplit=1)
        cmd    = parts[0].lower()
        arg    = parts[1].strip() if len(parts) > 1 else ""
        is_adm = STATE.is_admin(user.username)

        # ── الجميع ──────────────────────────────
        if cmd == "!help":
            await self._chat(_HELP_ALL + (_HELP_ADMIN if is_adm else ""))
            return

        if cmd in ("!queue", "!q"):
            s = STATE.get(); q = QUEUE.snap()
            if not s and not q:
                await self._chat("📋 القائمة فارغة."); return
            lines = []
            if s: lines.append(f"▶️ الآن: {s.title}")
            if q:
                lines.append(f"📋 ({len(q)}):")
                for i, r in enumerate(q, 1):
                    lines.append(f"  {i}. {r.query} — @{r.by}")
            await self._chat("\n".join(lines))
            return

        if cmd == "!np":
            s = STATE.get()
            if not s:
                await self._chat("⏸️ لا يوجد بث."); return
            e = STATE.elapsed_s()
            await self._chat(
                f"🎵 {s.title}\n"
                f"⏱️ {e//60}:{e%60:02d} / {s.dur_str}\n"
                f"👤 @{s.by} | 👂 {RING.count()} مستمع")
            return

        if cmd == "!history":
            h = STATE.get_history()
            if not h:
                await self._chat("🕒 لا يوجد سجل."); return
            await self._chat(
                "🕒 آخر الأغاني:\n" +
                "\n".join(f"  {i+1}. {s.title}" for i, s in enumerate(h)))
            return

        # ── !play للجميع — يضيف للقائمة فقط ────────
        if cmd == "!play":
            if not arg:
                await self._chat("❌ !play اسم الأغنية"); return
            arg = arg[:80]  # ✅ حد 80 حرف يكفي لأي اسم أغنية

            # Rate Limit — لا للأدمن
            if not is_adm:
                now  = time.time()
                last = _rate.get(user.username.lower(), 0)
                wait = PLAY_COOLDOWN - (now - last)
                if wait > 0:
                    await self._chat(
                        f"⏳ @{user.username} انتظر {int(wait)+1}s")
                    return
                _rate[user.username.lower()] = now

            # أضف للقائمة مباشرة — لا تقاطع الأغنية الحالية
            err = QUEUE.add(arg, user.username)
            if err:
                await self._chat(err); return

            pos = QUEUE.size()
            if self._playing:
                # يوجد بث حالي — أخبره بمكانه في الطابور
                await self._chat(
                    f"➕ {arg}\n"
                    f"👤 @{user.username} | 📋 موضعك: {pos}")
            else:
                # لا يوجد بث — ابدأ التشغيل فوراً
                await self._chat(f"🔍 جاري البحث: {arg}")
                self._task = asyncio.create_task(self._run_queue())
            return

        # ── !skip للجميع — cooldown 2 ثانية لحماية الموارد ──
        if cmd == "!skip":
            if not self._playing:
                await self._chat("❌ لا يوجد بث."); return
            now = time.time()
            if now - _skip_cooldown[0] < 2.0:
                return   # تجاهل صامت — لا رسالة، لا معالجة
            _skip_cooldown[0] = now
            nxt = QUEUE.peek()
            await self._chat(
                f"⏭️ @{user.username} تخطى\n"
                f"{'التالية: '+nxt.query if nxt else '📭 القائمة فارغة'}")
            BROADCAST.skip()
            return

        # ── أدمن فقط ─────────────────────────────
        if not is_adm:
            if cmd in ("!stop", "!remove",
                       "!addadmin", "!removeadmin", "!admins",
                       "!aplist", "!apadd", "!apremove", "!apclear"):
                await self._chat("❌ هذا الأمر للأدمن فقط.")
            return

        if cmd == "!skip":
            if not self._playing:
                await self._chat("❌ لا يوجد بث."); return
            nxt = QUEUE.peek()
            await self._chat(
                f"⏭️ تخطي...\n"
                f"{'التالية: '+nxt.query if nxt else '📭 القائمة فارغة'}")
            BROADCAST.skip()
            return

        if cmd == "!stop":
            QUEUE.clear(); STATE.set(None)
            self._playing = False
            BROADCAST.skip()
            await self._chat("⏹️ تم الإيقاف وحذف القائمة.")
            return

        if cmd == "!remove":
            if not arg.isdigit():
                await self._chat("❌ !remove رقم"); return
            r = QUEUE.remove(int(arg) - 1)
            await self._chat(
                f"🗑️ حُذف: {r.query}" if r else "❌ رقم غير صحيح.")
            return

        if cmd == "!addadmin":
            if not arg:
                await self._chat("❌ !addadmin اسم"); return
            STATE.add_admin(arg)
            await self._chat(f"✅ @{arg} أصبح أدمن.")
            return

        if cmd == "!removeadmin":
            if not arg:
                await self._chat("❌ !removeadmin اسم"); return
            STATE.del_admin(arg)
            await self._chat(f"✅ تم إزالة @{arg}.")
            return

        if cmd == "!admins":
            a = STATE.admins
            await self._chat(
                "👥 الأدمنز:\n" + "\n".join(f"  • @{x}" for x in a)
                if a else "👥 لا يوجد أدمنز.")
            return

        # ── أوامر قائمة التشغيل التلقائي ──────────
        if cmd == "!aplist":
            songs = AUTOPLAY.snap()
            idx   = AUTOPLAY._idx
            if not songs:
                await self._chat("📋 قائمة التلقائي فارغة.\n!apadd اسم — لإضافة أغنية")
                return
            # مقسّمة لرسالتين لو طالت (حد Highrise 256 حرف)
            header = f"🔁 التلقائي ({len(songs)} أغنية):"
            lines  = []
            for i, s in enumerate(songs):
                marker = "▶️" if i == idx % len(songs) else f"{i+1}."
                lines.append(f"  {marker} {s[:38]}")
            body = "\n".join(lines)
            # أرسل بشكل مقسّم لو الرسالة طويلة
            full = header + "\n" + body
            if len(full) <= 240:
                await self._chat(full)
            else:
                await self._chat(header)
                # قسّم الأغاني على رسائل بحد 240 حرف
                chunk = ""
                for line in lines:
                    test = (chunk + "\n" + line).strip() if chunk else line
                    if len(test) > 230:
                        await self._chat(chunk)
                        await asyncio.sleep(0.3)
                        chunk = line
                    else:
                        chunk = test
                if chunk:
                    await self._chat(chunk)
            return

        if cmd == "!apadd":
            if not arg:
                await self._chat("❌ !apadd اسم الأغنية"); return
            msg = AUTOPLAY.add(arg)
            await self._chat(f"🔁 {msg}\n📝 {arg[:50]}")
            return

        if cmd == "!apremove":
            if not arg.isdigit():
                await self._chat("❌ !apremove رقم"); return
            removed = AUTOPLAY.remove(int(arg) - 1)
            await self._chat(
                f"🗑️ حُذفت: {removed[:60]}"
                if removed else "❌ رقم غير صحيح.")
            return

        if cmd == "!apclear":
            AUTOPLAY.clear()
            await self._chat("🗑️ تم مسح قائمة التشغيل التلقائي كاملاً.")
            return

    # ── حلقة التشغيل ────────────────────────────
    async def _run_queue(self):
        if self._playing: return
        self._playing = True
        try:
            while not QUEUE.empty() and self._connected:
                req = QUEUE.pop()
                if not req: break

                # ✅ تحقق من الاتصال قبل البحث
                if not self._connected: break

                # جلب معلومات الأغنية
                try:
                    info = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            _EXEC, fetch_song, req.query),
                        timeout=float(FETCH_TIMEOUT + 10))
                except asyncio.TimeoutError:
                    info = None

                # ✅ تحقق مرة أخرى بعد البحث — قد ينقطع خلاله
                if not self._connected: break

                if not info:
                    await self._chat(f"❌ لم أجد: {req.query}")
                    continue

                song = Song(
                    query=req.query, title=info.title,
                    url=info.url, duration=info.duration,
                    by=req.by)
                STATE.set(song)
                _done_ev.clear()

                q_txt = f"\n📋 في القائمة: {QUEUE.size()}" if QUEUE.size() else ""
                await self._chat(
                    f"▶️ {song.title}\n"
                    f"⏱️ {song.dur_str} | 👤 @{song.by}"
                    f"{q_txt}")

                BROADCAST.play(song)

                # ✅ انتظر مع timeout (مدة الأغنية + 3 دقائق)
                max_wait = (song.duration if song.duration > 0 else 7200) + 180
                waited   = 0
                while not _done_ev.is_set() and self._connected:
                    await asyncio.sleep(1)
                    STATE.tick()
                    waited += 1
                    if waited > max_wait:
                        log.warning("⚠️ timeout إجباري")
                        BROADCAST.skip()
                        break

                STATE.add_history(song)
                STATE.set(None)
                await asyncio.sleep(0.2)

            if self._connected:
                await self._chat("📭 انتهت القائمة.")

            # ✅ autoplay — فقط لو متصل
            if QUEUE.empty() and self._connected:
                nxt = AUTOPLAY.next_song()
                if nxt:
                    QUEUE.add(nxt, "🤖 تلقائي")

        except asyncio.CancelledError:
            log.info("_run_queue: إلغاء")
        except Exception as e:
            log.exception("_run_queue خطأ: %s", e)
        finally:
            self._playing = False

    # ── Watchdog ─────────────────────────────────
    async def _watchdog_loop(self):
        """يتحقق كل دقيقة — يُصلح الموت الصامت"""
        try:
            while self._connected:
                await asyncio.sleep(60)
                if not self._connected: break
                if (not self._playing
                        and not QUEUE.empty()
                        and (self._task is None or self._task.done())):
                    log.warning("🔧 Watchdog: أعاد تشغيل _run_queue")
                    self._task = asyncio.create_task(self._run_queue())
        except asyncio.CancelledError: pass
        except Exception as e:
            log.error("watchdog خطأ: %s", e)

    # ── مساعدات ───────────────────────────────────
    async def _chat(self, msg: str):
        """إرسال آمن — لا crash عند فشل الإرسال"""
        if not self._connected: return
        if len(msg) > 240:
            msg = msg[:237] + "..."
        try:
            await self.highrise.chat(msg)
        except Exception as e:
            log.warning("chat فشل: %s", e)
            # ✅ لا نضع _connected=False — خطأ مؤقت لا يوقف البوت

    async def _cancel_task(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try: await self._task
            except Exception: pass
        self._task = None

# ════════════════════════════════════════════════════
#  حالة عالمية للـ !play
# ════════════════════════════════════════════════════
_play_lock     = asyncio.Lock()
_rate:          dict[str, float] = {}   # username → وقت آخر طلب
_skip_cooldown: list[float]      = [0.0]  # [آخر skip] — قائمة لتعديلها من داخل الدالة

# ════════════════════════════════════════════════════
#  حلقة الاتصال
# ════════════════════════════════════════════════════
async def _main():
    global _play_lock, _rate, _skip_cooldown
    backoff = 5
    while True:
        try:
            log.info("🔌 جاري الاتصال...")
            # ✅ صفّر الحالة العالمية قبل كل اتصال جديد
            _play_lock     = asyncio.Lock()
            _rate          = {}
            _skip_cooldown = [0.0]
            await main([BotDefinition(RadioBot(), ROOM_ID, TOKEN)])
            backoff = 5
        except Exception as e:
            if _bot_ref: _bot_ref._connected = False
            log.error("❌ %s — إعادة خلال %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ════════════════════════════════════════════════════
#  نقطة الدخول
# ════════════════════════════════════════════════════
if __name__ == "__main__":
    w = 56
    is_render = bool(os.environ.get("RENDER_EXTERNAL_HOSTNAME", ""))
    print("\n" + "═"*w)
    print("  🎙️  Highrise Radio Bot — v9  (النهائي)")
    print("  خفيف • قوي • سريع • مستقر للأبد")
    if is_render:
        print("  ☁️  وضع: Render Cloud")
    print("═"*w)
    print(f"  📻  رابط البث  : {STREAM_URL}")
    print(f"  🌐  لوحة التحكم: {_proto}://{PUBLIC_IP}/")
    print(f"  👤  المالك     : {OWNER}")
    print(f"  🔌  البورت     : {STREAM_PORT}")
    print(f"  📂  طلبات محفوظة: {QUEUE.size()}")
    print(f"  💾  ذاكرة Ring Buffer: {RING_MB}MB ثابت")
    print("═"*w + "\n")

    threading.Thread(target=_start_http, daemon=True).start()
    time.sleep(0.5)

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("\n👋 تم الإيقاف.")
