import os
import re
import io
import asyncio
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, unquote

import aiohttp

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

# ===================== Config =====================

BOT_TOKEN = os.getenv("BOT_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip() or None
TELEGRAM_MAX_BYTES = 2 * 1024 * 1024 * 1024 - 5 * 1024 * 1024  # ~2GB safety

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise SystemExit("BOT_TOKEN looks missing or malformed. Set BOT_TOKEN and rerun.")

# ===================== Bot Setup =====================

router = Router()

# ===================== Helpers =====================

GH_API = "https://api.github.com"

GITHUB_URL_RE = re.compile(
    r"(https?://(?:raw\.githubusercontent\.com|github\.com|codeload\.github\.com)/\S+)",
    re.IGNORECASE,
)

def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_") or "file"

def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def split_path(url: str):
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    return p, parts

async def aiohttp_session():
    headers = {"User-Agent": "TelegramGitHubFetcher/1.0 (+bot)"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    timeout = aiohttp.ClientTimeout(total=900)  # 15 min
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    return aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector)

async def download_stream(session: aiohttp.ClientSession, url: str, dest_path: Path):
    async with session.get(url, allow_redirects=True) as r:
        if r.status >= 400:
            text = await r.text()
            raise RuntimeError(f"HTTP {r.status} while GET {url}: {text[:200]}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with dest_path.open("wb") as f:
            async for chunk in r.content.iter_chunked(1 << 20):  # 1MB
                f.write(chunk)
    return dest_path

async def get_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as r:
        if r.status >= 400:
            text = await r.text()
            raise RuntimeError(f"HTTP {r.status} at {url}: {text[:200]}")
        return await r.json()

async def get_default_branch(session: aiohttp.ClientSession, owner: str, repo: str) -> str:
    data = await get_json(session, f"{GH_API}/repos/{owner}/{repo}")
    return data.get("default_branch") or "main"

async def list_branches(session: aiohttp.ClientSession, owner: str, repo: str, per_page: int = 10, page: int = 1):
    url = f"{GH_API}/repos/{owner}/{repo}/branches?per_page={per_page}&page={page}"
    return await get_json(session, url)

def build_repo_zip_name(owner: str, repo: str, branch: str) -> str:
    return safe_name(f"{owner}-{repo}-{branch}-{now_stamp()}.zip")

def zip_directory(src_dir: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file in src_dir.rglob("*"):
            if file.is_file():
                zf.write(file, file.relative_to(src_dir))
    return zip_path

async def send_file(m: Message, file_path: Path, caption: str):
    size = file_path.stat().st_size
    if size > TELEGRAM_MAX_BYTES:
        raise RuntimeError(f"File is too big for Telegram ({size/1024/1024:.1f} MB).")
    await m.answer_document(document=FSInputFile(str(file_path)), caption=caption)

# ===================== Core downloaders =====================

async def fetch_repo_zip(m: Message, session: aiohttp.ClientSession, owner: str, repo: str, branch: str | None):
    if not branch:
        branch = await get_default_branch(session, owner, repo)
    zip_url = f"https://codeload.github.com/{owner}/{repo}/zip/refs/heads/{branch}"
    tmp = Path(tempfile.mkdtemp(prefix="ghrepo_"))
    out = tmp / build_repo_zip_name(owner, repo, branch)
    await download_stream(session, zip_url, out)
    await send_file(m, out, caption=f"<b>{owner}/{repo}</b> (branch: <code>{branch}</code>)")
    # cleanup
    try:
        for p in tmp.rglob("*"):
            if p.is_file():
                p.unlink()
        tmp.rmdir()
    except Exception:
        pass

async def fetch_single_file(m: Message, session: aiohttp.ClientSession, raw_url: str, filename_hint: str):
    tmp = Path(tempfile.mkdtemp(prefix="ghfile_"))
    name = safe_name(filename_hint or Path(urlparse(raw_url).path).name or f"file-{now_stamp()}")
    out = tmp / name
    await download_stream(session, raw_url, out)
    await send_file(m, out, caption=f"<code>{name}</code>")
    try:
        for p in tmp.rglob("*"):
            if p.is_file():
                p.unlink()
        tmp.rmdir()
    except Exception:
        pass

async def fetch_folder(m: Message, session: aiohttp.ClientSession, owner: str, repo: str, branch: str, folder_path: str):
    base_url = f"{GH_API}/repos/{owner}/{repo}/contents"
    tmp_root = Path(tempfile.mkdtemp(prefix="ghfold_"))
    dl_root = tmp_root / safe_name(f"{repo}-{folder_path or 'root'}-{branch}")
    dl_root.mkdir(parents=True, exist_ok=True)

    async def walk(path_rel: str):
        url = f"{base_url}/{path_rel}" if path_rel else f"{base_url}"
        url = f"{url}?ref={branch}"
        items = await get_json(session, url)
        if isinstance(items, dict) and items.get("type") == "file":
            dest = dl_root / Path(path_rel).name
            await download_stream(session, items["download_url"], dest)
            return
        if not isinstance(items, list):
            raise RuntimeError("Unexpected API response for folder listing.")
        for it in items:
            it_type = it.get("type")
            it_path = it.get("path", "")
            if it_type == "dir":
                await walk(it_path)
            elif it_type == "file":
                rel = Path(it_path)
                dest = dl_root / rel.relative_to(folder_path) if folder_path else dl_root / rel
                await download_stream(session, it["download_url"], dest)

    await walk(folder_path)

    zip_out = tmp_root / safe_name(f"{owner}-{repo}-{folder_path or 'root'}-{branch}-{now_stamp()}.zip")
    zip_directory(dl_root, zip_out)
    await send_file(m, zip_out, caption=f"<b>{owner}/{repo}</b> / <code>{folder_path or ''}</code> (branch: <code>{branch}</code>)")

    try:
        for p in tmp_root.rglob("*"):
            if p.is_file():
                p.unlink()
        for d in sorted([d for d in tmp_root.rglob("*") if d.is_dir()], reverse=True):
            d.rmdir()
        tmp_root.rmdir()
    except Exception:
        pass

# ===================== URL parsing & routing =====================

class AskFilePath(StatesGroup):
    waiting_path = State()

# chat_id -> (owner, repo, branch)
last_repo_ctx: dict[int, tuple[str, str, str]] = {}

def main_menu_kb(repo_id: str, has_tree: bool, has_blob: bool) -> InlineKeyboardMarkup:
    # repo_id format: owner|repo|branch|path(optional)
    buttons = []
    if has_blob:
        buttons.append([InlineKeyboardButton(text="⬇️ Download File", callback_data=f"file|{repo_id}")])
    if has_tree:
        buttons.append([InlineKeyboardButton(text="🗂️ ZIP Folder/Repo", callback_data=f"zip|{repo_id}")])
        buttons.append([InlineKeyboardButton(text="🎯 Get File by Path", callback_data=f"askpath|{repo_id}")])
        buttons.append([InlineKeyboardButton(text="🔀 Choose Branch", callback_data=f"branches|{repo_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def handle_github_link(m: Message, url: str):
    p, parts = split_path(url)
    host = p.netloc.lower()

    async with aiohttp_session() as session:
        if host == "raw.githubusercontent.com":
            filename = Path(p.path).name
            await fetch_single_file(m, session, url, filename)
            return

        if host == "codeload.github.com":
            name = safe_name(Path(p.path).name or f"archive-{now_stamp()}.zip")
            tmp = Path(tempfile.mkdtemp(prefix="ghzip_"))
            out = tmp / name
            await download_stream(session, url, out)
            await send_file(m, out, caption=f"<code>{name}</code>")
            try:
                out.unlink()
                tmp.rmdir()
            except Exception:
                pass
            return

        if host.endswith("github.com") and len(parts) >= 2:
            owner, repo = parts[0], parts[1].replace(".git", "")

            # Repo root
            if len(parts) == 2:
                branch = await get_default_branch(session, owner, repo)
                last_repo_ctx[m.chat.id] = (owner, repo, branch)
                text = f"<b>{owner}/{repo}</b>\nBranch: <code>{branch}</code>\nChoose:"
                kb = main_menu_kb(f"{owner}|{repo}|{branch}|", has_tree=True, has_blob=False)
                await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                return

            # Releases asset direct
            if "releases" in parts and "download" in parts:
                name = safe_name(Path(p.path).name or f"{repo}-asset-{now_stamp()}")
                tmp = Path(tempfile.mkdtemp(prefix="ghrel_"))
                out = tmp / name
                await download_stream(session, url, out)
                await send_file(m, out, caption=f"<b>{owner}/{repo}</b> release asset: <code>{name}</code>")
                try:
                    out.unlink()
                    tmp.rmdir()
                except Exception:
                    pass
                return

            # archive link
            if "archive" in parts:
                name = safe_name(Path(p.path).name or f"{repo}-archive-{now_stamp()}.zip")
                tmp = Path(tempfile.mkdtemp(prefix="gharch_"))
                out = tmp / name
                await download_stream(session, url, out)
                await send_file(m, out, caption=f"<b>{owner}/{repo}</b> archive: <code>{name}</code>")
                try:
                    out.unlink()
                    tmp.rmdir()
                except Exception:
                    pass
                return

            # blob (single file)
            if "blob" in parts:
                i = parts.index("blob")
                if len(parts) >= i + 3:
                    branch = parts[i + 1]
                    rel_path = "/".join(parts[i + 2 : ])
                    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rel_path}"
                    filename = unquote(Path(rel_path).name)
                    last_repo_ctx[m.chat.id] = (owner, repo, branch)
                    text = f"<b>{owner}/{repo}</b>\nFile: <code>{rel_path}</code>\nBranch: <code>{branch}</code>"
                    kb = main_menu_kb(f"{owner}|{repo}|{branch}|{rel_path}", has_tree=True, has_blob=True)
                    await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                    return

            # tree (folder) or tree root
            if "tree" in parts:
                i = parts.index("tree")
                if len(parts) == i + 2:
                    branch = parts[i + 1]
                    last_repo_ctx[m.chat.id] = (owner, repo, branch)
                    text = f"<b>{owner}/{repo}</b>\nBranch: <code>{branch}</code>\nChoose:"
                    kb = main_menu_kb(f"{owner}|{repo}|{branch}|", has_tree=True, has_blob=False)
                    await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                    return
                elif len(parts) > i + 2:
                    branch = parts[i + 1]
                    rel_path = "/".join(parts[i + 2 : ])
                    last_repo_ctx[m.chat.id] = (owner, repo, branch)
                    text = f"<b>{owner}/{repo}</b>\nFolder: <code>{rel_path}</code>\nBranch: <code>{branch}</code>"
                    kb = main_menu_kb(f"{owner}|{repo}|{branch}|{rel_path}", has_tree=True, has_blob=False)
                    await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
                    return

            # fallback: repo root
            branch = await get_default_branch(session, owner, repo)
            last_repo_ctx[m.chat.id] = (owner, repo, branch)
            text = f"<b>{owner}/{repo}</b>\nBranch: <code>{branch}</code>\nChoose:"
            kb = main_menu_kb(f"{owner}|{repo}|{branch}|", has_tree=True, has_blob=False)
            await m.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
            return

        raise RuntimeError("Unsupported or malformed GitHub URL.")

# ===================== Handlers =====================

@router.message(Command("start", "help"))
async def cmd_start(m: Message):
    text = (
        "Send me any <b>GitHub link</b> and I’ll fetch it.\n"
        "• <code>https://github.com/owner/repo</code> → menu to ZIP repo\n"
        "• <code>.../tree/&lt;branch&gt;/path/</code> → menu to ZIP this folder\n"
        "• <code>.../blob/&lt;branch&gt;/file.py</code> → option to download that file\n"
        "• <code>.../releases/download/...</code> → release asset\n"
        "• <code>raw.githubusercontent.com/...</code> → direct file\n\n"
        "Buttons are inline. Use 'Get File by Path' to request a specific file."
    )
    await m.answer(text, parse_mode=ParseMode.HTML)

@router.message(F.text.regexp(GITHUB_URL_RE.pattern))
async def on_github_link(m: Message):
    url = GITHUB_URL_RE.search(m.text).group(1)
    status = await m.answer("🔎 Parsing link…")
    try:
        await handle_github_link(m, url)
        await status.delete()
    except Exception as e:
        msg = str(e)
        await status.edit_text(
            "❌ Failed.\n"
            f"<code>{msg[:800]}</code>\n\n"
            "• Check the link and access.\n"
            "• For private repos, set <code>GITHUB_TOKEN</code>."
        )

# ---- Callbacks ----

def parse_repo_id(repo_id: str):
    # owner|repo|branch|path?
    parts = repo_id.split("|")
    owner = parts[0]
    repo = parts[1]
    branch = parts[2]
    path = parts[3] if len(parts) > 3 and parts[3] else ""
    return owner, repo, branch, path

@router.callback_query(F.data.startswith("zip|"))
async def cb_zip(cq: CallbackQuery):
    _, repo_id = cq.data.split("|", 1)
    owner, repo, branch, rel_path = parse_repo_id(repo_id)
    await cq.answer("Preparing ZIP…")
    async with aiohttp_session() as session:
        if rel_path:
            await fetch_folder(cq.message, session, owner, repo, branch, rel_path)
        else:
            await fetch_repo_zip(cq.message, session, owner, repo, branch)
    await cq.message.answer("✅ Done.")
    await cq.answer()

@router.callback_query(F.data.startswith("file|"))
async def cb_file(cq: CallbackQuery):
    _, repo_id = cq.data.split("|", 1)
    owner, repo, branch, rel_path = parse_repo_id(repo_id)
    if not rel_path:
        await cq.answer()
        await cq.message.answer("No file selected. Use 'Get File by Path' to specify a file.")
        return
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rel_path}"
    async with aiohttp_session() as session:
        await fetch_single_file(cq.message, session, raw_url, Path(rel_path).name)
    await cq.message.answer("✅ Done.")
    await cq.answer()

@router.callback_query(F.data.startswith("askpath|"))
async def cb_askpath(cq: CallbackQuery, state: FSMContext):
    _, repo_id = cq.data.split("|", 1)
    owner, repo, branch, _ = parse_repo_id(repo_id)
    await state.update_data(repo_id=repo_id)
    await state.set_state(AskFilePath.waiting_path)
    await cq.message.answer(
        f"Send the <b>relative file path</b> to download from <code>{owner}/{repo}</code> "
        f"(branch <code>{branch}</code>), e.g. <code>src/main.py</code>.",
        parse_mode=ParseMode.HTML
    )
    await cq.answer()

@router.message(AskFilePath.waiting_path)
async def on_path_input(m: Message, state: FSMContext):
    data = await state.get_data()
    repo_id = data.get("repo_id")
    if not repo_id:
        await m.answer("Context lost. Please send the GitHub link again.")
        await state.clear()
        return
    owner, repo, branch, _ = parse_repo_id(repo_id)
    rel_path = m.text.strip().lstrip("/")

    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{rel_path}"
    status = await m.answer(f"⬇️ Downloading <code>{rel_path}</code>…", parse_mode=ParseMode.HTML)
    async with aiohttp_session() as session:
        try:
            await fetch_single_file(m, session, raw_url, Path(rel_path).name)
            await status.edit_text("✅ Done.")
        except Exception as e:
            await status.edit_text(f"❌ Failed: <code>{str(e)[:800]}</code>", parse_mode=ParseMode.HTML)
    await state.clear()

@router.callback_query(F.data.startswith("branches|"))
async def cb_branches(cq: CallbackQuery):
    _, repo_id = cq.data.split("|", 1)
    owner, repo, branch, rel_path = parse_repo_id(repo_id)
    async with aiohttp_session() as session:
        try:
            branches = await list_branches(session, owner, repo, per_page=10, page=1)
            if isinstance(branches, list) and branches:
                rows = []
                for b in branches:
                    bname = b.get("name")
                    rows.append([InlineKeyboardButton(text=bname, callback_data=f"setbranch|{owner}|{repo}|{bname}|{rel_path}")])
                kb = InlineKeyboardMarkup(inline_keyboard=rows)
                await cq.message.answer("Select branch:", reply_markup=kb)
            else:
                await cq.message.answer("No branches found, using default.")
        except Exception as e:
            await cq.message.answer(f"Failed to list branches: <code>{str(e)[:200]}</code>", parse_mode=ParseMode.HTML)
    await cq.answer()

@router.callback_query(F.data.startswith("setbranch|"))
async def cb_setbranch(cq: CallbackQuery):
    _, owner, repo, new_branch, rel_path = cq.data.split("|", 4)
    text = ""
    if rel_path:
        text = f"<b>{owner}/{repo}</b>\nPath: <code>{rel_path}</code>\nBranch: <code>{new_branch}</code>"
    else:
        text = f"<b>{owner}/{repo}</b>\nBranch: <code>{new_branch}</code>\nChoose:"
    kb = main_menu_kb(f"{owner}|{repo}|{new_branch}|{rel_path}", has_tree=True, has_blob=bool(rel_path))
    await cq.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cq.answer("Branch set.")

# ===================== Entrypoint =====================

async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
