#!/usr/bin/env python3
"""
АФЛ-бот: по рабочим дням в 19:05 (МСК) отправляет в Telegram сводку по акциям
ПАО «Аэрофлот» (AFLT, режим TQBR) и Индексу МосБиржи.

Данные берутся из бесплатного ISS API Московской биржи (iss.moex.com). Без подписки
они идут с задержкой около 15 минут, поэтому в 19:05 бот начинает проверять, появились ли
итоги аукциона закрытия, и отправляет сообщение сразу после этого.

Запуск бота:              python afl_bot.py
Проверка без Telegram:    python afl_bot.py --print
Проверка за прошлый день: python afl_bot.py --print --date 2026-09-10
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import ssl
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from telegram import InputFile, Update
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes

# ═══════════════════════════════ НАСТРОЙКИ ═══════════════════════════════

MESSAGE_TEMPLATE = (
    "Андрей Юрьевич, добрый вечер!\n"
    "Направляю данные по акциям АФЛ\n"
    "{date}\n"
    "-Цена закрытия {close} руб.\n"
    "-Изменение {change}\n"
    "-Объем торгов {volume} млрд руб.\n"
    "-Индекс МосБиржи {index_change}"
)

MSK = ZoneInfo("Europe/Moscow")
SEND_TIME = dtime(19, 5, tzinfo=MSK)  # когда начинать проверку и отправку
SEND_DAYS = (1, 2, 3, 4, 5)           # пн–пт (в python-telegram-bot 0 = воскресенье)
GIVE_UP_AT = dtime(19, 45)            # если к этому времени данных нет, сообщение не уходит
POLL_SECONDS = 60                     # как часто проверять появление итогов
SETTLE_SECONDS = 90                   # пауза после появления аукциона, чтобы догрузились индекс и объем

TICKER, BOARD, INDEX = "AFLT", "TQBR", "IMOEX"
AUCTION_CANDLE = dtime(18, 50)        # 10-минутная свеча, в которую попадает аукцион закрытия
INDEX_LAST_CANDLE = dtime(18, 40)     # последняя свеча индекса в основной сессии начинается не раньше
DAY_CUTOFF = dtime(19, 0)             # учитываются сделки до 19:00, вечерняя сессия (с 19:05) — нет

PRICE_DECIMALS = 1
PCT_DECIMALS = 1
VOLUME_DECIMALS = 1

# Защита от ошибочных цифр: при срабатывании сообщение уходит вам как черновик, а не получателю
MAX_SHARE_MOVE_PCT = 25.0
MAX_INDEX_MOVE_PCT = 15.0
MAX_CLOSE_VS_LAST_TRADE_PCT = 1.0

# ═════════════════════════════ ДАННЫЕ БИРЖИ ══════════════════════════════

ISS_URL = "https://iss.moex.com/iss"
SHARE_PATH = f"engines/stock/markets/shares/boards/{BOARD}/securities/{TICKER}"
INDEX_PATH = f"engines/stock/markets/index/securities/{INDEX}"
MARKET_MAP_URL = "https://smart-lab.ru/q/map1/"

log = logging.getLogger("afl_bot")
market_map_lock = asyncio.Lock()


class NoTrading(Exception):
    """За день нет сделок: выходной или праздник."""


def iss_client() -> httpx.AsyncClient:
    tls = ssl.create_default_context()
    # Official Russian Trusted Root CA from https://www.gosuslugi.ru/crt.
    tls.load_verify_locations(cafile=Path(__file__).with_name("moex-root-ca.pem"))
    return httpx.AsyncClient(
        base_url=ISS_URL,
        verify=tls,
        timeout=30,
        headers={"User-Agent": "afl-daily-bot/1.0"},
    )


async def iss_table(client: httpx.AsyncClient, path: str, block: str, params: dict | None = None) -> list[dict]:
    """Запрос к ISS. Возвращает таблицу `block` как список словарей."""
    query = {"iss.meta": "off", "iss.only": block, **(params or {})}
    resp = await client.get(f"{path}.json", params=query)
    resp.raise_for_status()
    table = resp.json()[block]
    return [dict(zip(table["columns"], row)) for row in table["data"]]


async def day_candles(client: httpx.AsyncClient, path: str, day: date) -> list[dict]:
    """10-минутные свечи за календарный день `day`, начавшиеся до DAY_CUTOFF."""
    rows = await iss_table(client, f"{path}/candles", "candles", {
        "interval": 10, "from": day.isoformat(), "till": f"{day.isoformat()} 23:59:59",
    })
    result = []
    for row in rows:
        begin = datetime.fromisoformat(row["begin"])
        if begin.date() == day and begin.time() < DAY_CUTOFF:
            row["begin_dt"] = begin
            result.append(row)
    return sorted(result, key=lambda r: r["begin_dt"])


async def history(client: httpx.AsyncClient, path: str, day: date) -> list[dict]:
    """Дневные итоги торгов за три недели до `day` включительно."""
    return await iss_table(client, f"history/{path}", "history", {
        "from": (day - timedelta(days=21)).isoformat(), "till": day.isoformat(),
    })


def from_history(rows: list[dict], column: str, day: date, before: bool = False) -> tuple[date, float] | None:
    """(дата, значение) за день `day`, а при before=True — за последний торговый день до `day`."""
    found = []
    for row in rows:
        value = row.get(column)
        if not value:
            continue
        d = date.fromisoformat(row["TRADEDATE"])
        if (d < day) if before else (d == day):
            found.append((d, float(value)))
    return max(found, key=lambda x: x[0]) if found else None


def auction_published(candles: list[dict]) -> bool:
    return any(c["begin_dt"].time() >= AUCTION_CANDLE for c in candles)


@dataclass
class Snapshot:
    day: date
    close: float | None = None
    prev_close: float | None = None
    volume_rub: float | None = None
    index_close: float | None = None
    index_prev_close: float | None = None
    problems: list[str] = field(default_factory=list)  # блокируют автоматическую отправку
    notes: list[str] = field(default_factory=list)     # только для вас, не блокируют

    @property
    def change_pct(self) -> float | None:
        if self.close and self.prev_close:
            return (self.close / self.prev_close - 1) * 100
        return None

    @property
    def index_change_pct(self) -> float | None:
        if self.index_close and self.index_prev_close:
            return (self.index_close / self.index_prev_close - 1) * 100
        return None


async def build_snapshot(client: httpx.AsyncClient, day: date) -> Snapshot:
    today = datetime.now(MSK).date()
    share_hist, index_hist, share_c, index_c = await asyncio.gather(
        history(client, SHARE_PATH, day),
        history(client, INDEX_PATH, day),
        day_candles(client, SHARE_PATH, day),
        day_candles(client, INDEX_PATH, day),
    )
    share_day = from_history(share_hist, "LEGALCLOSEPRICE", day)
    if not share_c and not share_day:
        raise NoTrading(f"{day:%d.%m.%Y} торгов по {TICKER} не было")

    s = Snapshot(day=day)
    session_closed = day < today or auction_published(share_c)
    if not session_closed:
        s.problems.append("основная сессия ещё не завершена, цифры промежуточные")

    # Цена закрытия — официальная цена закрытия биржи (итог аукциона закрытия)
    last_trade = float(share_c[-1]["close"]) if share_c else None
    if share_day:
        s.close = share_day[1]
    elif session_closed and day == today:
        md = await iss_table(client, SHARE_PATH, "marketdata")
        official = (md[0].get("LCLOSEPRICE") or md[0].get("CLOSINGAUCTIONPRICE")) if md else None
        if official:
            s.close = float(official)
    if s.close is None and last_trade is not None:
        s.close = last_trade
        s.notes.append("официальная цена закрытия не опубликована, взята цена последней сделки до 19:00")
    if s.close and last_trade and abs(s.close / last_trade - 1) * 100 > MAX_CLOSE_VS_LAST_TRADE_PCT:
        s.problems.append(f"цена закрытия {s.close} расходится с последней сделкой {last_trade}")

    prev = from_history(share_hist, "LEGALCLOSEPRICE", day, before=True)
    if prev:
        s.prev_close = prev[1]
    else:
        s.problems.append("нет цены закрытия предыдущего дня")

    # Объем — сумма сделок в TQBR за день до 19:00 (утренняя и основная сессии с аукционом закрытия)
    if share_c:
        s.volume_rub = sum(float(c.get("value") or 0) for c in share_c)
    else:
        full_day = from_history(share_hist, "VALUE", day)
        s.volume_rub = full_day[1] if full_day else None
        s.notes.append("внутридневных данных нет, объем взят по итогам всего дня")
    if not s.volume_rub:
        s.problems.append("нет данных об объеме торгов")

    # Индекс МосБиржи — официальное значение на закрытие основной сессии
    index_day = from_history(index_hist, "CLOSE", day)
    if index_day:
        s.index_close = index_day[1]
    elif index_c:
        s.index_close = float(index_c[-1]["close"])
        if session_closed and index_c[-1]["begin_dt"].time() < INDEX_LAST_CANDLE:
            s.problems.append("значение индекса на закрытие ещё не опубликовано")
    else:
        s.problems.append("нет данных по Индексу МосБиржи")

    index_prev = from_history(index_hist, "CLOSE", day, before=True)
    if index_prev:
        s.index_prev_close = index_prev[1]
    else:
        s.problems.append("нет значения индекса за предыдущий день")
    if prev and index_prev and prev[0] != index_prev[0]:
        s.notes.append(f"предыдущий торговый день по акции {prev[0]:%d.%m} и по индексу {index_prev[0]:%d.%m} не совпадает")

    if s.change_pct is not None and abs(s.change_pct) > MAX_SHARE_MOVE_PCT:
        s.problems.append(f"подозрительно большое изменение цены: {s.change_pct:.1f}%")
    if s.index_change_pct is not None and abs(s.index_change_pct) > MAX_INDEX_MOVE_PCT:
        s.problems.append(f"подозрительно большое изменение индекса: {s.index_change_pct:.1f}%")
    return s


async def resolve_day(client: httpx.AsyncClient, requested: date | None) -> date:
    """Сегодня, если сегодня будний день с торгами, иначе последний торговый день."""
    if requested:
        return requested
    today = datetime.now(MSK).date()
    if today.isoweekday() <= 5 and await day_candles(client, SHARE_PATH, today):
        return today
    last = from_history(await history(client, SHARE_PATH, today), "LEGALCLOSEPRICE",
                        today + timedelta(days=1), before=True)
    if not last:
        raise NoTrading("не удалось найти последний торговый день")
    return last[0]

# ═══════════════════════════════ ФОРМАТ ══════════════════════════════════


def _rounded(x: float, decimals: int) -> Decimal:
    return Decimal(repr(x)).quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP)


def fmt_number(x: float | None, decimals: int) -> str:
    return "н/д" if x is None else str(_rounded(x, decimals)).replace(".", ",")


def fmt_change(pct: float | None) -> str:
    if pct is None:
        return "н/д"
    r = _rounded(pct, PCT_DECIMALS)
    if r == 0:
        return fmt_number(0.0, PCT_DECIMALS) + "%"
    return ("+" if r > 0 else "-") + str(abs(r)).replace(".", ",") + "%"


def format_message(s: Snapshot) -> str:
    return MESSAGE_TEMPLATE.format(
        date=f"{s.day:%d.%m.%Y}",
        close=fmt_number(s.close, PRICE_DECIMALS),
        change=fmt_change(s.change_pct),
        volume=fmt_number(s.volume_rub / 1e9 if s.volume_rub else None, VOLUME_DECIMALS),
        index_change=fmt_change(s.index_change_pct),
    )


def admin_report(s: Snapshot) -> str:
    lines = [format_message(s), "", "———"]
    lines.append("Автоматически НЕ отправится: " + "; ".join(s.problems) if s.problems
                 else "Данные полные, сообщение готово к отправке.")
    if s.notes:
        lines.append("Примечания: " + "; ".join(s.notes))
    volume = f"{s.volume_rub:,.0f}".replace(",", " ") if s.volume_rub else "н/д"
    lines.append(f"Контроль: закрытие {s.close}, пред. {s.prev_close}; "
                 f"IMOEX {s.index_close}, пред. {s.index_prev_close}; объем {volume} руб.")
    return "\n".join(lines)

# ═══════════════════════════════ TELEGRAM ════════════════════════════════


@dataclass
class Config:
    token: str | None
    admin_id: int | None
    recipients: list[int]


def load_config() -> Config:
    env_file = Path(__file__).with_name("config.env")
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
    admin = os.environ.get("ADMIN_CHAT_ID", "").strip()
    recipients = os.environ.get("RECIPIENT_CHAT_IDS", "")
    return Config(
        token=os.environ.get("BOT_TOKEN", "").strip() or None,
        admin_id=int(admin) if admin else None,
        recipients=[int(x) for x in recipients.replace(" ", "").split(",") if x],
    )


async def notify_admin(bot, cfg: Config, text: str) -> None:
    log.info("Админу: %s", text.replace("\n", " | "))
    if cfg.admin_id:
        try:
            await bot.send_message(cfg.admin_id, text)
        except Exception:
            log.exception("Не удалось написать админу")


async def capture_market_map() -> bytes:
    from playwright.async_api import async_playwright

    async with market_map_lock, asyncio.timeout(40), async_playwright() as playwright:
        browser_env = {
            key: os.environ[key]
            for key in ("PATH", "HOME", "LANG", "SYSTEMROOT", "TMP", "TEMP")
            if key in os.environ
        }
        browser = await playwright.chromium.launch(env=browser_env)
        try:
            page = await browser.new_page(
                viewport={"width": 1440, "height": 900}, locale="ru-RU", timezone_id="Europe/Moscow",
                service_workers="block",
            )
            await page.route(re.compile(r"^(?!https://smart-lab\.ru/)"), lambda route: route.abort())
            response = await page.goto(MARKET_MAP_URL, wait_until="domcontentloaded", timeout=20_000)
            if response is None or not response.ok:
                raise RuntimeError(f"Smart-Lab: HTTP {response.status if response else 'нет ответа'}")

            chart = page.locator("#chart_div_shares svg.highcharts-root")
            labels = chart.locator(".highcharts-data-label text:visible").filter(has_text=re.compile(r"\d%"))
            await labels.nth(4).wait_for(state="visible", timeout=12_000)
            return await chart.screenshot(
                type="png", animations="disabled", timeout=5_000,
                style="#cookies_warning, .custom-pop-up { visibility: hidden !important; }",
            )
        finally:
            await browser.close()


async def send_market_map(bot, recipients: list[int]) -> int | None:
    try:
        screenshot = await capture_market_map()
    except Exception:
        log.exception("Не удалось снять карту акций Smart-Lab")
        return None

    caption = f"Карта акций Smart-Lab за день · снимок в {datetime.now(MSK):%H:%M} МСК\n{MARKET_MAP_URL}"
    file_id = None
    sent = 0
    for chat_id in recipients:
        try:
            message = await bot.send_photo(
                chat_id, photo=file_id or InputFile(screenshot, filename="market-map.png"),
                caption=caption, write_timeout=30,
            )
            file_id = message.photo[-1].file_id
            sent += 1
        except TelegramError:
            log.exception("Не удалось отправить карту акций в chat_id %s", chat_id)
    return sent


async def send_report(bot, cfg: Config, wait: bool) -> None:
    today = datetime.now(MSK).date()
    give_up = datetime.combine(today, GIVE_UP_AT, MSK)
    async with iss_client() as client:
        if wait:
            empty_checks = 0
            while True:
                try:
                    candles = await day_candles(client, SHARE_PATH, today)
                    if auction_published(candles):
                        break
                    empty_checks = 0 if candles else empty_checks + 1
                    if empty_checks >= 3:
                        await notify_admin(bot, cfg, f"{today:%d.%m.%Y}: торгов нет, сообщение не отправлено.")
                        return
                except Exception as exc:
                    log.warning("ISS недоступен: %r", exc)
                if datetime.now(MSK) >= give_up:
                    await notify_admin(bot, cfg, f"К {GIVE_UP_AT:%H:%M} итоги аукциона закрытия не появились, "
                                                 "сообщение не отправлено. Проверьте /preview.")
                    return
                await asyncio.sleep(POLL_SECONDS)
            await asyncio.sleep(SETTLE_SECONDS)
        try:
            snap = await build_snapshot(client, today)
        except NoTrading as exc:
            await notify_admin(bot, cfg, f"Сообщение не отправлено: {exc}.")
            return
        except Exception as exc:
            await notify_admin(bot, cfg, f"Ошибка при сборе данных, сообщение не отправлено: {exc!r}")
            return

    if snap.problems or not cfg.recipients:
        reasons = snap.problems or ["не задан RECIPIENT_CHAT_IDS"]
        await notify_admin(bot, cfg, "Автоотправка остановлена: " + "; ".join(reasons)
                           + "\n\nЧерновик:\n\n" + format_message(snap))
        return
    text = format_message(snap)
    for chat_id in cfg.recipients:
        try:
            await bot.send_message(chat_id, text)
        except Exception as exc:
            await notify_admin(bot, cfg, f"Не удалось отправить в chat_id {chat_id}: {exc!r}")
            return
    sent_at = datetime.now(MSK)
    maps_sent = await send_market_map(bot, cfg.recipients)
    map_status = ("Скриншот карты Smart-Lab недоступен." if maps_sent is None
                  else f"Карта Smart-Lab отправлена: {maps_sent} из {len(cfg.recipients)}.")
    notes = ("\nПримечания: " + "; ".join(snap.notes)) if snap.notes else ""
    await notify_admin(bot, cfg, f"Сводка отправлена в {sent_at:%H:%M}.\n{map_status}{notes}")


def is_admin(update: Update, cfg: Config) -> bool:
    return cfg.admin_id is not None and update.effective_chat.id == cfg.admin_id


async def daily_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_report(context.bot, context.bot_data["cfg"], wait=True)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.bot_data["cfg"]
    chat, user = update.effective_chat, update.effective_user
    if cfg.admin_id is None or is_admin(update, cfg):
        await update.message.reply_text(
            f"Бот работает. Ваш chat_id: {chat.id}\n\n"
            "/preview — показать вам сводку и текущую карту акций\n"
            "/sendnow — отправить сегодняшнюю сводку получателям\n"
            "/id — показать chat_id")
        return
    await update.message.reply_text("Здравствуйте! Бот подключен.")
    username = f" (@{user.username})" if user.username else ""
    await notify_admin(context.bot, cfg, f"Боту написал {user.full_name}{username}, chat_id: {chat.id}")


async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"chat_id: {update.effective_chat.id}")


async def cmd_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update, context.bot_data["cfg"]):
        return
    try:
        async with iss_client() as client:
            snap = await build_snapshot(client, await resolve_day(client, None))
    except Exception as exc:
        await update.message.reply_text(f"Не удалось собрать данные: {exc}")
        return
    await update.message.reply_text(admin_report(snap))
    if not await send_market_map(context.bot, [update.effective_chat.id]):
        await update.message.reply_text("Не удалось отправить скриншот карты Smart-Lab.")


async def cmd_sendnow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.bot_data["cfg"]
    if is_admin(update, cfg):
        await send_report(context.bot, cfg, wait=False)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Ошибка в обработчике", exc_info=context.error)


def run_bot() -> None:
    cfg = load_config()
    if not cfg.token:
        sys.exit("Не задан BOT_TOKEN: впишите токен в config.env рядом со скриптом.")
    app = Application.builder().token(cfg.token).build()
    if app.job_queue is None:
        sys.exit('Нет планировщика: установите зависимости командой  pip install -r requirements.txt')
    app.bot_data["cfg"] = cfg
    for name, handler in (("start", cmd_start), ("id", cmd_id), ("preview", cmd_preview), ("sendnow", cmd_sendnow)):
        app.add_handler(CommandHandler(name, handler))
    app.add_error_handler(on_error)
    app.job_queue.run_daily(daily_job, time=SEND_TIME, days=SEND_DAYS, name="daily_report")
    log.info("Бот запущен. Сводка: пн–пт в %s МСК. Админ: %s. Получатели: %s",
             SEND_TIME.strftime("%H:%M"), cfg.admin_id, cfg.recipients or "не заданы")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


async def print_report(day: date | None) -> None:
    async with iss_client() as client:
        snap = await build_snapshot(client, await resolve_day(client, day))
    print(admin_report(snap))


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # иначе в лог попадает токен бота

    parser = argparse.ArgumentParser(description="Ежедневная сводка по акциям Аэрофлота в Telegram")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--print", action="store_true", help="собрать сводку и вывести в консоль, без Telegram")
    mode.add_argument("--market-map", type=Path, metavar="PNG", help="сохранить карту Smart-Lab, без Telegram")
    parser.add_argument("--date", type=date.fromisoformat, help="дата для --print в формате ГГГГ-ММ-ДД")
    args = parser.parse_args()
    if args.market_map:
        args.market_map.write_bytes(asyncio.run(capture_market_map()))
        print(f"Карта сохранена в {args.market_map}")
    elif args.print:
        try:
            asyncio.run(print_report(args.date))
        except NoTrading as exc:
            print(f"Нет данных: {exc}")
    else:
        run_bot()


if __name__ == "__main__":
    main()
