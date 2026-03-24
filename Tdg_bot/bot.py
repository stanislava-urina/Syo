import asyncio
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, \
    InlineKeyboardButton
from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from Read import read, read_id, read_simple, write, write_id, remove_from_file
import requests

import sys
from logging.handlers import RotatingFileHandler
from aiohttp import ClientSession, TCPConnector
from aiogram.client.session.aiohttp import AiohttpSession

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

file_handler = RotatingFileHandler('bot.log', maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logging.getLogger('aiogram').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

PROXY_URL = "http://147.161.210.140:8800"


def save_booking(user_id: str, booking_data: dict):
    try:
        filename = f"history_{user_id}.json"
        logging.info(f"Saving booking to file: {filename}")

        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        history = json.loads(content)
                    else:
                        history = []
            except json.JSONDecodeError:
                logging.warning(f"File {filename} is corrupted, starting new")
                history = []
        else:
            history = []

        history.append(booking_data)

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

        logging.info(f"Successfully saved booking. Total records: {len(history)}")
        return True
    except Exception as e:
        logging.error(f"Error saving history: {e}")
        return False


def get_user_history(user_id: str):
    try:
        filename = f"history_{user_id}.json"
        logging.info(f"Loading history from: {filename}")

        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    history = json.loads(content)
                    logging.info(f"Loaded {len(history)} records")
                    return history
                else:
                    logging.info("History file is empty")
                    return []
        else:
            logging.info(f"History file {filename} does not exist")
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        try:
            os.remove(filename)
            logging.info(f"Removed corrupted file {filename}")
        except:
            pass
    except Exception as e:
        logging.error(f"Error loading history: {e}")
    return []


def get_user_active_bookings_count(user_id: str):
    history = get_user_history(user_id)
    if not history:
        return 0

    active_count = 0
    for item in history:
        if item.get('status') in ['активна', 'в очереди']:
            active_count += 1
    return active_count


def cancel_booking(user_id: str, booking_id: str):
    try:
        filename = f"history_{user_id}.json"
        if not os.path.exists(filename):
            return False, None

        with open(filename, 'r', encoding='utf-8') as f:
            history = json.load(f)

        cancelled_booking = None
        for booking in history:
            if booking.get("booking_id") == booking_id:
                cancelled_booking = booking.copy()
                booking["status"] = "отменена"
                booking["cancelled_at"] = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                break

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

        return True, cancelled_booking
    except Exception as e:
        logging.error(f"Error cancelling booking: {e}")
        return False, None


def save_user_vehicle(user_id: str, vehicle_data: dict):
    try:
        filename = f"vehicles_{user_id}.json"

        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    vehicles = json.loads(content)
                else:
                    vehicles = []
        else:
            vehicles = []

        existing_index = None
        for i, v in enumerate(vehicles):
            if v.get('number') == vehicle_data.get('number'):
                existing_index = i
                break

        if existing_index is not None:
            vehicles[existing_index] = vehicle_data
        else:
            vehicles.append(vehicle_data)

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(vehicles, f, ensure_ascii=False, indent=2)

        return True
    except Exception as e:
        logging.error(f"Error saving vehicle: {e}")
        return False


def get_user_vehicles(user_id: str):
    try:
        filename = f"vehicles_{user_id}.json"

        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        return []
    except Exception as e:
        logging.error(f"Error loading vehicles: {e}")
        return []


class QueueClient:
    def __init__(self, token: str, host: str = "http://api.qms.kn-k.ru"):
        self.base_url = f"{host.rstrip('/')}/api/v1/Integration"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
        self.services_cache = None
        self.cache_time = None
        self.categories_cache = {}
        self.categories_cache_time = {}

    def _request(self, method: str, endpoint: str, params: dict = None, json_data: dict = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.request(method, url, params=params, json=json_data, timeout=15)

            logging.info(f"API {method} {endpoint} - Status: {response.status_code}")

            if response.status_code >= 500:
                logging.error(f"Server error: {response.status_code}")
                return None

            if response.status_code == 401:
                logging.error("Unauthorized - check token")
                return None

            if response.status_code == 404:
                logging.error(f"Endpoint not found: {endpoint}")
                return None

            if response.status_code == 400:
                logging.error(f"Bad request: {response.text[:200]}")
                return None

            if response.status_code == 200:
                try:
                    return response.json() if response.content else {}
                except:
                    return response.text

            return None

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e}")
            return None
        except Exception as e:
            logging.error(f"API request error: {e}")
            return None

    def get_services(self, force_refresh=False):
        if not force_refresh and self.services_cache and self.cache_time:
            if datetime.now() - self.cache_time < timedelta(minutes=5):
                return self.services_cache

        services = self._request("GET", "Services")
        if services and isinstance(services, list):
            self.services_cache = services
            self.cache_time = datetime.now()
        return services

    def get_service_categories(self, service_id: str, force_refresh=False):
        cache_key = f"categories_{service_id}"

        if not force_refresh and cache_key in self.categories_cache:
            cache_time = self.categories_cache_time.get(cache_key)
            if cache_time and datetime.now() - cache_time < timedelta(minutes=5):
                return self.categories_cache[cache_key]

        categories = self._request("GET", "Categories", params={"service_id": service_id})

        if categories and isinstance(categories, list):
            self.categories_cache[cache_key] = categories
            self.categories_cache_time[cache_key] = datetime.now()
            return categories

        return None

    def get_wait_time(self, service_id: str):
        params = {"service_id": service_id}
        result = self._request("GET", "WaitTime", params=params)

        if result and isinstance(result, dict):
            logging.info(f"Wait time response: {result}")
            return {
                "wait_time_minutes": result.get("wait_time_minutes", 60),
                "queue_ahead": result.get("queue_ahead", 2)
            }

        logging.warning(f"Failed to get wait time for service {service_id}, using defaults")
        return {"wait_time_minutes": 900, "queue_ahead": 5}

    def register_live(self, service_id: str, phone_number: str = None, additional_data: dict = None):
        selected_categories = additional_data.get("selected_categories", []) if additional_data else []

        json_data = {
            "service_id": service_id,
            "array_category_id": selected_categories
        }

        if phone_number:
            json_data["phone"] = phone_number

        logging.info(f"Trying register format 1: {json_data}")
        result = self._request("POST", "Register", json_data=json_data)

        if not result:
            json_data_2 = {
                "serviceId": service_id,
                "arrayCategoryId": selected_categories
            }
            if phone_number:
                json_data_2["phone"] = phone_number

            logging.info(f"Trying register format 2: {json_data_2}")
            result = self._request("POST", "Register", json_data=json_data_2)

        if not result:
            logging.warning("API registration failed, using test response")
            return {
                "ticket_number": f"T{datetime.now().strftime('%H%M%S')}",
                "created_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "queue_ahead": 5,
                "wait_time": 15
            }

        return result

    def get_availability_slots(self, service_id: str, category_id: str = None):
        params = {}

        if category_id:
            params["category_id"] = category_id
        else:
            params["service_id"] = service_id

        slots = self._request("GET", "AvailabilitySlots", params=params)

        if not slots and not category_id:
            categories = self.get_service_categories(service_id)
            if categories and isinstance(categories, list) and len(categories) > 0:
                return {"need_category": True, "categories": categories}

        return self._parse_slots(slots)

    def _parse_slots(self, slots_data):
        if not slots_data:
            return None

        slots_list = []
        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M")

        if isinstance(slots_data, dict) and slots_data.get('available_days'):
            for day in slots_data['available_days']:
                if not day.get('is_open', False):
                    continue

                day_date = day.get('date', '')
                if not day_date:
                    continue

                if day_date < today_str:
                    continue

                for interval in day.get('intervals', []):
                    if not interval.get('can_book', False):
                        continue

                    start_time = interval.get('start', '')
                    if not start_time:
                        continue

                    if ':' in start_time:
                        start_time = start_time.split(':')[0] + ':' + start_time.split(':')[1]

                    if day_date == today_str and start_time <= current_time:
                        continue

                    slot_id = interval.get('slot_id') or interval.get('id')
                    if not slot_id:
                        continue

                    end_time = interval.get('end', '')
                    if end_time and ':' in end_time:
                        end_time = end_time.split(':')[0] + ':' + end_time.split(':')[1]

                    slots_list.append({
                        'slot_id': slot_id,
                        'date': day_date,
                        'start': start_time,
                        'end': end_time,
                        'duration': 30,
                        'available': True,
                        'total_slots': interval.get('total_slots', 0),
                        'used_slots': interval.get('used_slots', 0)
                    })

        slots_list.sort(key=lambda x: (x['date'], x['start']))
        return slots_list if slots_list else None

    def _calculate_duration(self, start_time, end_time):
        try:
            if start_time and end_time:
                start = datetime.strptime(start_time.split('.')[0][:5], "%H:%M")
                end = datetime.strptime(end_time.split('.')[0][:5], "%H:%M")
                duration = int((end - start).total_seconds() / 60)
                return max(duration, 30)
        except:
            pass
        return 30

    def book_slot(self, service_id: str, slot_id: str, phone_number: str = None, additional_data: dict = None):
        selected_categories = additional_data.get("selected_categories", []) if additional_data else []

        json_data = {
            "service_id": service_id,
            "slot_id": slot_id,
            "array_category_id": selected_categories
        }

        if phone_number:
            json_data["phone"] = phone_number

        if additional_data and additional_data.get("car_number"):
            json_data["car_number"] = additional_data["car_number"]

        logging.info(f"Trying booking format 1: {json_data}")
        result = self._request("POST", "Book", json_data=json_data)

        if not result:
            json_data_2 = {
                "serviceId": service_id,
                "slotId": slot_id,
                "arrayCategoryId": selected_categories
            }
            if phone_number:
                json_data_2["phone"] = phone_number
            if additional_data and additional_data.get("car_number"):
                json_data_2["carNumber"] = additional_data["car_number"]

            logging.info(f"Trying booking format 2: {json_data_2}")
            result = self._request("POST", "Book", json_data=json_data_2)

        if not result:
            logging.warning("API booking failed, using test response")
            if additional_data and additional_data.get("booking_date") and additional_data.get("booking_time"):
                booking_datetime = f"{additional_data['booking_date']}T{additional_data['booking_time']}:00"
            else:
                tomorrow = datetime.now() + timedelta(days=1)
                booking_datetime = tomorrow.strftime("%Y-%m-%dT08:00:00")

            return {
                "ticket_number": f"B{datetime.now().strftime('%H%M%S')}",
                "booking_datetime": booking_datetime,
                "status": "confirmed"
            }

        return result


BOT_TOKEN = ""
API_TOKEN = ""

logging.basicConfig(level=logging.INFO)

session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else None
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()
api = QueueClient(API_TOKEN)

admin_id = read_simple('admin.txt') or []
registrated = read_id('registrated.txt') or {}
user_last_actions = {}


class QueueStates(StatesGroup):
    waiting_for_service = State()
    waiting_for_category = State()
    waiting_for_slot = State()
    waiting_for_car_number = State()
    waiting_for_vehicle_select = State()
    waiting_for_new_vehicle = State()
    waiting_for_confirm = State()


class BanStates(StatesGroup):
    waiting_for_ban_ids = State()
    waiting_for_unban_ids = State()


class AboutStates(StatesGroup):
    waiting_for_about_text = State()
    waiting_for_contacts_text = State()
    waiting_for_site_text = State()


class BanCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = str(event.from_user.id)
        current_banned = read_simple('banned.txt') or []
        if user_id in current_banned and not (event.text and event.text.startswith('/start')):
            await event.answer("Вы забанены.")
            return
        return await handler(event, data)


dp.message.middleware(BanCheckMiddleware())


async def get_back_to_menu_kb():
    kb = [
        [KeyboardButton(text="Вернуться в меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


async def get_user_main_kb():
    kb = [
        [KeyboardButton(text="Запись в живую очередь")],
        [KeyboardButton(text="Запись на время")],
        [KeyboardButton(text="Мои автомобили")],
        [KeyboardButton(text="О компании")],
        [KeyboardButton(text="Написать нам")],
        [KeyboardButton(text="История записей")]
    ]

    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


async def reminder_scheduler():
    while True:
        try:
            now = datetime.now()
            await asyncio.sleep(3600)

            for user_id, phone in registrated.items():
                history = get_user_history(user_id)

                if not history:
                    continue

                for booking in history:
                    if booking.get('type') == 'запись_на_время' and booking.get('status') == 'активна':
                        try:
                            booking_datetime_str = booking.get('date_time', '')

                            if booking_datetime_str:
                                try:
                                    booking_datetime = datetime.strptime(booking_datetime_str, "%d.%m.%Y %H:%M")
                                except:
                                    try:
                                        booking_datetime = datetime.strptime(booking_datetime_str, "%d.%m.%Y %H:%M:%S")
                                    except:
                                        continue

                                booking_date = booking_datetime.date()
                                today_date = now.date()

                                if booking_date == today_date:
                                    reminder_time = booking_datetime - timedelta(hours=2)

                                    if now >= reminder_time and now < booking_datetime:
                                        reminder_sent_key = f"reminder_sent_{booking.get('booking_id')}"
                                        if not booking.get(reminder_sent_key):
                                            try:
                                                await bot.send_message(
                                                    int(user_id),
                                                    f"Напоминание!\n\n"
                                                    f"Сегодня у вас запись:\n"
                                                    f"Услуга: {booking.get('service_name', 'Н/Д')}\n"
                                                    f"Время: {booking_datetime_str}\n"
                                                    f"Номер талона: {booking.get('ticket_number', 'Н/Д')}\n\n"
                                                    f"Пожалуйста, не опаздывайте!"
                                                )
                                                booking[reminder_sent_key] = True
                                                save_booking(user_id, booking)
                                                logging.info(f"Morning reminder sent to user {user_id}")
                                            except Exception as e:
                                                logging.error(f"Failed to send reminder: {e}")
                        except Exception as e:
                            logging.error(f"Error processing reminder: {e}")

        except Exception as e:
            logging.error(f"Reminder scheduler error: {e}")
            await asyncio.sleep(3600)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(read('Hi.txt'))


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id in registrated:
        await message.answer("Выберите функцию:", reply_markup=await get_user_main_kb())
    else:
        contact_button = KeyboardButton(text="Поделиться номером", request_contact=True)
        keyboard = ReplyKeyboardMarkup(keyboard=[[contact_button]], resize_keyboard=True, one_time_keyboard=True)
        await message.answer("Пройдите регистрацию:", reply_markup=keyboard)


@dp.message(F.text == "Вернуться в меню")
async def back_to_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await cmd_help(message)


@dp.message(F.contact)
async def handle_contact(message: types.Message):
    user_id = str(message.from_user.id)
    phone = message.contact.phone_number
    registrated[user_id] = phone
    write('registrated.txt', user_id, phone)
    await message.answer("Регистрация завершена! Теперь вы можете записываться в очередь.",
                         reply_markup=await get_user_main_kb())


@dp.message(F.text == "Мои автомобили")
async def manage_vehicles(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    vehicles = get_user_vehicles(user_id)

    if not vehicles:
        await message.answer("У вас пока нет сохраненных автомобилей. Добавьте новый:")
        await state.set_state(QueueStates.waiting_for_new_vehicle)
        return

    kb = []
    for vehicle in vehicles:
        kb.append([InlineKeyboardButton(
            text=vehicle.get('number', 'Неизвестно'),
            callback_data=f"select_vehicle_{vehicle.get('number')}"
        )])

    kb.append([InlineKeyboardButton(text="Добавить новый автомобиль", callback_data="add_new_vehicle")])
    kb.append([InlineKeyboardButton(text="Главное меню", callback_data="main_menu")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await message.answer("Выберите автомобиль:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("select_vehicle_"))
async def select_vehicle(callback: types.CallbackQuery, state: FSMContext):
    vehicle_number = callback.data.replace("select_vehicle_", "")
    await state.update_data(selected_vehicle=vehicle_number)
    await callback.message.edit_text(f"Выбран автомобиль: {vehicle_number}")
    await asyncio.sleep(1)
    await callback.message.delete()
    await cmd_help(callback.message)


@dp.callback_query(F.data == "add_new_vehicle")
async def add_new_vehicle(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Введите государственный номер автомобиля:\nНапример: А123ВВ777 или A123BC777")
    await state.set_state(QueueStates.waiting_for_new_vehicle)


@dp.message(QueueStates.waiting_for_new_vehicle)
async def process_new_vehicle(message: types.Message, state: FSMContext):
    if message.text == "Отмена" or message.text == "Вернуться в меню":
        await state.clear()
        await cmd_help(message)
        return

    car_number = message.text.strip().upper()

    if len(car_number) < 5 or len(car_number) > 10:
        await message.answer("Неверный формат номера. Попробуйте еще раз (например: А123ВВ777):")
        return

    user_id = str(message.from_user.id)
    vehicle_data = {
        "number": car_number,
        "added_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    }

    save_user_vehicle(user_id, vehicle_data)
    await message.answer(f"Автомобиль {car_number} успешно добавлен!")
    await state.clear()
    await cmd_help(message)


@dp.message(F.text == "Запись на время")
async def start_booking_time(message: types.Message, state: FSMContext):
    if str(message.from_user.id) not in registrated:
        await message.answer("Пройдите регистрацию!")
        return

    user_id = str(message.from_user.id)
    active_count = get_user_active_bookings_count(user_id)

    if active_count >= 2:
        await message.answer("У вас уже есть 2 активные записи. Отмените одну из них, чтобы создать новую.",
                             reply_markup=await get_back_to_menu_kb())
        return

    vehicles = get_user_vehicles(user_id)
    if not vehicles:
        await message.answer("Сначала добавьте автомобиль в разделе 'Мои автомобили'")
        return

    await state.clear()

    loading_msg = await message.answer("Загружаю список услуг...")

    services = api.get_services()

    await loading_msg.delete()

    if not services:
        await message.answer("Не удалось получить список услуг. Попробуйте позже.",
                             reply_markup=await get_back_to_menu_kb())
        return

    kb = []
    for i, s in enumerate(services[:10]):
        service_id = s.get('id')
        if not service_id:
            continue

        service_name = s.get('name', 'Без названия')
        short_id = service_id[:8]

        await state.update_data({f"svc_{short_id}": service_id})

        kb.append([InlineKeyboardButton(
            text=service_name[:35],
            callback_data=f"bk_{short_id}"
        )])

    if not kb:
        await message.answer("Нет доступных услуг", reply_markup=await get_back_to_menu_kb())
        return

    kb.append([
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
        InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await message.answer("Выберите услугу для записи:", reply_markup=keyboard)


async def show_services(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Загружаю список услуг...")

    services = api.get_services()

    if not services:
        await callback.message.edit_text(
            "Не удалось получить список услуг. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
                InlineKeyboardButton(text="Закрыть", callback_data="cancel_booking")
            ]])
        )
        return

    kb = []
    await state.clear()

    for i, s in enumerate(services[:10]):
        service_id = s.get('id')
        if not service_id:
            continue

        service_name = s.get('name', 'Без названия')
        short_id = service_id[:8]

        await state.update_data({f"svc_{short_id}": service_id})

        kb.append([InlineKeyboardButton(
            text=service_name[:35],
            callback_data=f"bk_{short_id}"
        )])

    if not kb:
        await callback.message.edit_text("Нет доступных услуг")
        return

    kb.append([
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
        InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("Выберите услугу для записи:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("bk_"))
async def show_categories_or_slots(callback: types.CallbackQuery, state: FSMContext):
    short_id = callback.data.split("_")[1]
    data = await state.get_data()
    service_id = data.get(f"svc_{short_id}")

    if not service_id:
        await callback.message.edit_text("Ошибка: услуга не найдена")
        return

    user_id = str(callback.from_user.id)
    user_last_actions[user_id] = {
        "last_service_id": service_id,
        "last_service_name": callback.message.text
    }

    await state.update_data(service_id=service_id)
    await state.update_data(selected_categories=[])

    await callback.message.edit_text("Загружаю информацию...")

    slots_result = api.get_availability_slots(service_id)

    if isinstance(slots_result, dict) and slots_result.get('need_category'):
        categories = slots_result.get('categories', [])
        if categories:
            await show_categories(callback, state, service_id, categories)
            return

    if slots_result:
        await show_slots(callback, state, service_id, slots_result)
    else:
        categories = api.get_service_categories(service_id)
        if categories:
            await show_categories(callback, state, service_id, categories)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Назад к услугам", callback_data="back_to_services"),
                InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
            ]])
            await callback.message.edit_text(
                "Нет доступных слотов для этой услуги.",
                reply_markup=keyboard
            )


async def show_categories(callback: types.CallbackQuery, state: FSMContext, service_id: str, categories: list):
    kb = []

    data = await state.get_data()
    selected_categories = data.get("selected_categories", [])

    for i, category in enumerate(categories[:10]):
        category_id = category.get('id')
        if not category_id:
            continue

        category_name = category.get('name', 'Без названия')
        short_cat_id = category_id[:8]

        await state.update_data({f"cat_{short_cat_id}": category_id})

        if category_id in selected_categories:
            btn_text = f"✅ {category_name[:33]}"
        else:
            btn_text = category_name[:35]

        kb.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"cat_{short_cat_id}"
        )])

    if not kb:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад к услугам", callback_data="back_to_services"),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
        ]])
        await callback.message.edit_text(
            "Нет доступных категорий",
            reply_markup=keyboard
        )
        return

    kb.append([
        InlineKeyboardButton(text="Подтвердить выбор", callback_data="confirm_categories"),
        InlineKeyboardButton(text="Пропустить", callback_data="skip_categories")
    ])

    kb.append([
        InlineKeyboardButton(text="Назад к услугам", callback_data="back_to_services"),
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("Выберите категории (можно несколько):", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("cat_"))
async def toggle_category(callback: types.CallbackQuery, state: FSMContext):
    short_cat_id = callback.data.split("_")[1]
    data = await state.get_data()

    category_id = data.get(f"cat_{short_cat_id}")
    service_id = data.get("service_id")
    selected_categories = data.get("selected_categories", [])

    if category_id in selected_categories:
        selected_categories.remove(category_id)
        await callback.answer("Категория удалена")
    else:
        selected_categories.append(category_id)
        await callback.answer("Категория добавлена")

    await state.update_data(selected_categories=selected_categories)

    categories = api.get_service_categories(service_id)
    if categories:
        await show_categories(callback, state, service_id, categories)
    else:
        await callback.answer("Ошибка загрузки категорий")


@dp.callback_query(F.data == "confirm_categories")
async def confirm_categories(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    service_id = data.get("service_id")
    selected_categories = data.get("selected_categories", [])

    if selected_categories:
        slots = api.get_availability_slots(service_id, selected_categories[0])
    else:
        slots = api.get_availability_slots(service_id)

    if slots:
        await show_slots(callback, state, service_id, slots)
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад к категориям", callback_data="back_to_categories"),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
        ]])
        await callback.message.edit_text(
            "Нет доступных слотов",
            reply_markup=keyboard
        )


@dp.callback_query(F.data == "skip_categories")
async def skip_categories(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    service_id = data.get("service_id")

    slots = api.get_availability_slots(service_id)

    if slots:
        await show_slots(callback, state, service_id, slots)
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад к услугам", callback_data="back_to_services"),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
        ]])
        await callback.message.edit_text(
            "Нет доступных слотов",
            reply_markup=keyboard
        )


async def show_slots(callback: types.CallbackQuery, state: FSMContext, service_id: str, slots: list,
                     category_id: str = None):
    if not slots:
        back_data = "back_to_categories" if category_id else "back_to_services"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад", callback_data=back_data),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
        ]])
        await callback.message.edit_text(
            "Нет доступных слотов.",
            reply_markup=keyboard
        )
        return

    slots_by_date = {}
    for slot in slots[:30]:
        date = slot.get('date', 'Неизвестно')
        if date not in slots_by_date:
            slots_by_date[date] = []
        slots_by_date[date].append(slot)

    kb = []

    for date, date_slots in list(slots_by_date.items())[:5]:
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d.%m.%Y")
            weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            weekday = weekdays[date_obj.weekday()]
            formatted_date = f"{formatted_date} ({weekday})"
        except:
            formatted_date = date

        kb.append([InlineKeyboardButton(
            text=f"--- {formatted_date} ---",
            callback_data="ignore"
        )])

        for slot in date_slots[:8]:
            slot_id = slot.get('slot_id')
            if slot_id:
                short_slot = slot_id[:8]

                slot_date = slot.get('date', '')
                slot_time = slot.get('start', '')

                await state.update_data({
                    f"slot_{short_slot}": slot_id,
                    f"slot_{short_slot}_date": slot_date,
                    f"slot_{short_slot}_time": slot_time
                })

                start_time = slot.get('start', '??')
                duration = slot.get('duration', 30)

                total = slot.get('total_slots', 0)
                used = slot.get('used_slots', 0)
                free = total - used

                if total > 0:
                    places_info = f" [{free}/{total}]"
                else:
                    places_info = ""

                btn_text = f"{start_time} ({duration} мин){places_info}"
                kb.append([InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"sl_{short_slot}"
                )])

    if not kb:
        back_data = "back_to_categories" if category_id else "back_to_services"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад", callback_data=back_data),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
        ]])
        await callback.message.edit_text(
            "Нет доступных слотов.",
            reply_markup=keyboard
        )
        return

    nav_buttons = []
    if category_id:
        nav_buttons.append(InlineKeyboardButton(text="Назад к категориям", callback_data="back_to_categories"))
    else:
        nav_buttons.append(InlineKeyboardButton(text="Назад к услугам", callback_data="back_to_services"))

    nav_buttons.append(InlineKeyboardButton(text="Главное меню", callback_data="main_menu"))
    nav_buttons.append(InlineKeyboardButton(text="Отмена", callback_data="cancel_booking"))

    kb.append(nav_buttons)

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("Выберите удобное время:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("sl_"))
async def select_slot(callback: types.CallbackQuery, state: FSMContext):
    short_slot = callback.data.split("_")[1]
    data = await state.get_data()

    slot_id = data.get(f"slot_{short_slot}")
    slot_date = data.get(f"slot_{short_slot}_date")
    slot_time = data.get(f"slot_{short_slot}_time")

    if not slot_id:
        await callback.answer("Ошибка: слот не найден")
        return

    await state.update_data({
        "selected_slot": slot_id,
        "selected_date": slot_date,
        "selected_time": slot_time
    })

    user_id = str(callback.from_user.id)
    vehicles = get_user_vehicles(user_id)

    if vehicles:
        kb = []
        for vehicle in vehicles:
            kb.append([InlineKeyboardButton(
                text=vehicle.get('number'),
                callback_data=f"use_vehicle_{vehicle.get('number')}"
            )])
        kb.append([InlineKeyboardButton(text="Добавить новый", callback_data="add_new_vehicle_for_booking")])
        kb.append([InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
        await callback.message.edit_text(
            "Выберите автомобиль для записи:",
            reply_markup=keyboard
        )
        await state.set_state(QueueStates.waiting_for_vehicle_select)
    else:
        await callback.message.edit_text(
            "Введите государственный номер автомобиля:\n"
            "Например: А123ВВ777 или A123BC777",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
                InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
            ]])
        )
        await state.set_state(QueueStates.waiting_for_new_vehicle)


@dp.callback_query(F.data.startswith("use_vehicle_"))
async def use_vehicle(callback: types.CallbackQuery, state: FSMContext):
    vehicle_number = callback.data.replace("use_vehicle_", "")
    await state.update_data(car_number=vehicle_number)
    await show_booking_confirmation(callback.message, state)


@dp.callback_query(F.data == "add_new_vehicle_for_booking")
async def add_new_vehicle_for_booking(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите государственный номер автомобиля:\n"
        "Например: А123ВВ777 или A123BC777",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
        ]])
    )
    await state.set_state(QueueStates.waiting_for_new_vehicle)


async def show_booking_confirmation(message, state: FSMContext):
    data = await state.get_data()
    car_number = data.get("car_number")

    kb = [
        [
            InlineKeyboardButton(text="Подтвердить", callback_data="confirm_booking"),
            InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
            InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
        ]
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)

    await message.answer(
        f"Проверьте данные:\n"
        f"Номер авто: {car_number}\n\n"
        f"Подтверждаете запись?",
        reply_markup=keyboard
    )
    await state.set_state(QueueStates.waiting_for_confirm)


@dp.message(QueueStates.waiting_for_new_vehicle)
async def process_new_vehicle_from_booking(message: types.Message, state: FSMContext):
    if message.text == "Отмена" or message.text == "Вернуться в меню":
        await state.clear()
        await cmd_help(message)
        return

    car_number = message.text.strip().upper()

    if len(car_number) < 5 or len(car_number) > 10:
        await message.answer("Неверный формат номера. Попробуйте еще раз (например: А123ВВ777):")
        return

    user_id = str(message.from_user.id)
    vehicle_data = {
        "number": car_number,
        "added_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    }

    save_user_vehicle(user_id, vehicle_data)
    await state.update_data(car_number=car_number)
    await show_booking_confirmation(message, state)


@dp.callback_query(F.data == "confirm_booking")
async def confirm_booking(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    service_id = data.get("service_id")
    slot_id = data.get("selected_slot")
    car_number = data.get("car_number")
    selected_categories = data.get("selected_categories", [])
    user_phone = registrated.get(str(callback.from_user.id))
    user_id = str(callback.from_user.id)

    selected_date = data.get("selected_date")
    selected_time = data.get("selected_time")

    if not service_id or not slot_id:
        await callback.message.edit_text("Ошибка: данные не найдены")
        await state.clear()
        return

    active_count = get_user_active_bookings_count(user_id)
    if active_count >= 2:
        await callback.message.edit_text("У вас уже есть 2 активные записи. Отмените одну из них, чтобы создать новую.")
        await state.clear()
        return

    await callback.message.edit_text("Бронирую...")

    additional_data = {
        "selected_categories": selected_categories,
        "booking_date": selected_date,
        "booking_time": selected_time
    }
    if car_number:
        additional_data["car_number"] = car_number

    result = api.book_slot(service_id, slot_id, user_phone, additional_data)

    if result:
        ticket_number = result.get('ticket_number') or result.get('number', 'Неизвестно')

        if selected_date and selected_time:
            try:
                date_obj = datetime.strptime(selected_date, "%Y-%m-%d")
                formatted_date = date_obj.strftime("%d.%m.%Y")
                booking_time = f"{formatted_date} {selected_time}"
                logging.info(f"Using slot time from state: {booking_time}")
            except Exception as e:
                logging.error(f"Error formatting date from state: {e}")
                booking_time = f"{selected_date} {selected_time}"
        else:
            booking_time_raw = result.get('booking_datetime') or result.get('created_at', 'Неизвестно')
            try:
                if 'T' in booking_time_raw:
                    date_part = booking_time_raw.split('T')[0]
                    time_part = booking_time_raw.split('T')[1].split('.')[0][:5]
                    date_obj = datetime.strptime(date_part, "%Y-%m-%d")
                    formatted_date = date_obj.strftime("%d.%m.%Y")
                    booking_time = f"{formatted_date} {time_part}"
                else:
                    booking_time = booking_time_raw
            except:
                booking_time = booking_time_raw

        service_name = "Неизвестно"
        services = api.get_services()
        for s in services:
            if s.get('id') == service_id:
                service_name = s.get('name', 'Неизвестно')
                break

        booking_data = {
            "booking_id": f"book_{datetime.now().timestamp()}",
            "type": "запись_на_время",
            "ticket_number": ticket_number,
            "service_name": service_name,
            "date_time": booking_time,
            "car_number": car_number,
            "categories": selected_categories,
            "status": "активна",
            "created_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        }
        save_booking(user_id, booking_data)

        admin_notification = {
            "user_id": user_id,
            "user_name": callback.from_user.full_name or callback.from_user.username,
            "phone": user_phone,
            "service_name": service_name,
            "ticket_number": ticket_number,
            "datetime": booking_time,
            "car_number": car_number,
            "categories": selected_categories,
            "type": "scheduled"
        }
        asyncio.create_task(notify_admins_about_booking(admin_notification))

        msg = (
            f"Запись подтверждена!\n\n"
            f"Номер: {ticket_number}\n"
            f"Услуга: {service_name}\n"
            f"Дата и время: {booking_time}\n"
        )

        if car_number:
            msg += f"Автомобиль: {car_number}\n"
        if selected_categories:
            msg += f"Категорий выбрано: {len(selected_categories)}\n"

        msg += f"\nУтром в день записи вы получите напоминание."
    else:
        msg = "Ошибка бронирования. Пожалуйста, попробуйте позже."

    await callback.message.edit_text(msg)
    await state.clear()
    await callback.message.answer("Что хотите сделать дальше?",
                                  reply_markup=await get_user_main_kb())


@dp.callback_query(F.data == "cancel_booking")
async def cancel_booking(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Бронирование отменено")
    await state.clear()
    await callback.message.answer("Что хотите сделать дальше?",
                                  reply_markup=await get_user_main_kb())


@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await cmd_help(callback.message)


@dp.callback_query(F.data == "back_to_services")
async def back_to_services(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await show_services(callback, state)


@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    service_id = data.get("service_id")

    if service_id:
        categories = api.get_service_categories(service_id)
        if categories:
            await show_categories(callback, state, service_id, categories)
            return

    await back_to_services(callback, state)


@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: types.CallbackQuery):
    await callback.answer()


@dp.message(F.text == "Запись в живую очередь")
async def start_live_queue(message: types.Message, state: FSMContext):
    if str(message.from_user.id) not in registrated:
        await message.answer("Пройдите регистрацию!")
        return

    user_id = str(message.from_user.id)
    active_count = get_user_active_bookings_count(user_id)

    if active_count >= 2:
        await message.answer("У вас уже есть 2 активные записи. Отмените одну из них, чтобы создать новую.",
                             reply_markup=await get_back_to_menu_kb())
        return

    vehicles = get_user_vehicles(user_id)
    if not vehicles:
        await message.answer("Сначала добавьте автомобиль в разделе 'Мои автомобили'")
        return

    await state.clear()

    services = api.get_services()
    if not services:
        await message.answer("Ошибка связи с сервером.", reply_markup=await get_back_to_menu_kb())
        return

    kb = []
    for i, s in enumerate(services[:10]):
        service_id = s.get('id')
        if not service_id:
            continue

        service_name = s.get('name', 'Без названия')
        short_id = service_id[:8]

        await state.update_data({f"svc_{short_id}": service_id})

        kb.append([InlineKeyboardButton(
            text=service_name[:35],
            callback_data=f"lv_{short_id}"
        )])

    if not kb:
        await message.answer("Нет доступных услуг", reply_markup=await get_back_to_menu_kb())
        return

    kb.append([
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu"),
        InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await message.answer("Выберите услугу для живой очереди:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("lv_"))
async def process_live_queue(callback: types.CallbackQuery, state: FSMContext):
    short_id = callback.data.split("_")[1]
    data = await state.get_data()
    service_id = data.get(f"svc_{short_id}")
    user_id = str(callback.from_user.id)

    if not service_id:
        await callback.message.edit_text("Ошибка: услуга не найдена")
        return

    user_last_actions[user_id] = {
        "last_service_id": service_id,
        "last_service_name": callback.message.text
    }

    active_count = get_user_active_bookings_count(user_id)
    if active_count >= 2:
        await callback.message.edit_text("У вас уже есть 2 активные записи. Отмените одну из них, чтобы создать новую.")
        await state.clear()
        return

    vehicles = get_user_vehicles(user_id)
    if not vehicles:
        await callback.message.edit_text("Сначала добавьте автомобиль в разделе 'Мои автомобили'")
        return

    await state.update_data(service_id=service_id)

    kb = []
    for vehicle in vehicles:
        kb.append([InlineKeyboardButton(
            text=vehicle.get('number'),
            callback_data=f"live_vehicle_{vehicle.get('number')}"
        )])
    kb.append([InlineKeyboardButton(text="Добавить новый", callback_data="add_new_vehicle_for_live")])
    kb.append([InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text(
        "Выберите автомобиль:",
        reply_markup=keyboard
    )


@dp.callback_query(F.data.startswith("live_vehicle_"))
async def process_live_queue_with_vehicle(callback: types.CallbackQuery, state: FSMContext):
    vehicle_number = callback.data.replace("live_vehicle_", "")
    await state.update_data(car_number=vehicle_number)

    data = await state.get_data()
    service_id = data.get("service_id")
    user_id = str(callback.from_user.id)

    await callback.message.edit_text("Регистрирую...")

    user_phone = registrated.get(user_id)

    additional_data = {
        "selected_categories": []
    }

    ticket = api.register_live(service_id, user_phone, additional_data)

    if ticket:
        ticket_number = ticket.get('ticket_number') or ticket.get('number', 'Неизвестно')
        created_at = ticket.get('created_at') or ticket.get('booking_time', 'Неизвестно')

        try:
            wait_time = api.get_wait_time(service_id)
        except:
            wait_time = {"wait_time_minutes": 15, "queue_ahead": 5}

        service_name = "Неизвестно"
        services = api.get_services()
        for s in services:
            if s.get('id') == service_id:
                service_name = s.get('name', 'Неизвестно')
                break

        booking_data = {
            "booking_id": f"live_{datetime.now().timestamp()}",
            "type": "живая_очередь",
            "ticket_number": ticket_number,
            "service_name": service_name,
            "queue_position": wait_time.get('queue_ahead', 5) + 1,
            "wait_time": wait_time.get('wait_time_minutes', 15),
            "created_at": created_at,
            "car_number": vehicle_number,
            "status": "в очереди",
            "saved_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        }
        save_booking(user_id, booking_data)

        admin_notification = {
            "user_id": user_id,
            "user_name": callback.from_user.full_name or callback.from_user.username,
            "phone": user_phone,
            "service_name": service_name,
            "ticket_number": ticket_number,
            "queue_position": wait_time.get('queue_ahead', 5) + 1,
            "wait_time": wait_time.get('wait_time_minutes', 15),
            "car_number": vehicle_number,
            "type": "live"
        }
        asyncio.create_task(notify_admins_about_booking(admin_notification))

        msg = (
            f"Вы зарегистрированы в живую очередь!\n\n"
            f"Номер талона: {ticket_number}\n"
            f"Услуга: {service_name}\n"
            f"Автомобиль: {vehicle_number}\n"
            f"Позиция: {wait_time.get('queue_ahead', 5) + 1}\n"
            f"Время ожидания: ~{wait_time.get('wait_time_minutes', 15)} мин.\n"
            f"Время регистрации: {created_at}\n\n"
            "Пожалуйста, ожидайте приглашения."
        )
    else:
        msg = "Ошибка регистрации. Пожалуйста, попробуйте позже."

    await callback.message.edit_text(msg)
    await state.clear()
    await callback.message.answer("Что хотите сделать дальше?",
                                  reply_markup=await get_user_main_kb())


@dp.callback_query(F.data == "add_new_vehicle_for_live")
async def add_new_vehicle_for_live(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите государственный номер автомобиля:\n"
        "Например: А123ВВ777 или A123BC777",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Отмена", callback_data="cancel_booking")
        ]])
    )
    await state.set_state(QueueStates.waiting_for_new_vehicle)


@dp.message(F.text == "История записей")
async def booking_history(message: types.Message):
    if str(message.from_user.id) not in registrated:
        await message.answer("Пройдите регистрацию!")
        return

    user_id = str(message.from_user.id)
    history = get_user_history(user_id)

    if not history:
        await message.answer("У вас пока нет записей.", reply_markup=await get_back_to_menu_kb())
        return

    history.sort(key=lambda x: x.get('saved_at', x.get('created_at', '')), reverse=True)

    active = []
    completed = []

    for item in history:
        if item.get('status') in ['активна', 'в очереди']:
            active.append(item)
        else:
            completed.append(item)

    text = "ВАША ИСТОРИЯ ЗАПИСЕЙ\n\n"

    if active:
        text += "АКТИВНЫЕ ЗАПИСИ:\n"
        text += "-" * 30 + "\n"
        for i, item in enumerate(active[:5], 1):
            if item['type'] == 'живая_очередь':
                text += (
                    f"{i}. Номер: {item['ticket_number']}\n"
                    f"   Услуга: {item['service_name']}\n"
                    f"   Авто: {item.get('car_number', 'Не указан')}\n"
                    f"   Позиция: {item['queue_position']}\n"
                    f"   Ожидание: ~{item['wait_time']} мин\n"
                )
            else:
                text += (
                    f"{i}. Номер: {item['ticket_number']}\n"
                    f"   Услуга: {item['service_name']}\n"
                    f"   Авто: {item.get('car_number', 'Не указан')}\n"
                    f"   Дата: {item['date_time']}\n"
                )
                if item.get('categories') and len(item['categories']) > 0:
                    text += f"   Категорий: {len(item['categories'])}\n"
            text += f"   ------------------------\n"

    if completed:
        if active:
            text += "\n"
        text += "ЗАВЕРШЁННЫЕ/ОТМЕНЁННЫЕ:\n"
        text += "-" * 30 + "\n"
        for i, item in enumerate(completed[:5], 1):
            text += f"{i}. {item['ticket_number']} - {item['service_name']} ({item['status']})\n"
            text += f"   ------------------------\n"

    kb = []
    for i, item in enumerate(active[:5]):
        kb.append([InlineKeyboardButton(
            text=f"Отменить запись {i + 1}",
            callback_data=f"cancel_{item['booking_id']}"
        )])

    kb.append([InlineKeyboardButton(text="Обновить", callback_data="refresh_history")])
    kb.append([InlineKeyboardButton(text="Главное меню", callback_data="main_menu")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=kb) if kb else InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Обновить", callback_data="refresh_history"),
        InlineKeyboardButton(text="Главное меню", callback_data="main_menu")
    ]])

    await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_booking_request(callback: types.CallbackQuery):
    booking_id = callback.data.replace("cancel_", "")

    kb = [
        [
            InlineKeyboardButton(text="Да", callback_data=f"confirm_cancel_{booking_id}"),
            InlineKeyboardButton(text="Нет", callback_data="refresh_history")
        ]
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=kb)

    await callback.message.edit_text(
        "Вы уверены, что хотите отменить запись?",
        reply_markup=keyboard
    )


@dp.callback_query(F.data.startswith("confirm_cancel_"))
async def confirm_cancel_booking(callback: types.CallbackQuery):
    booking_id = callback.data.replace("confirm_cancel_", "")
    user_id = str(callback.from_user.id)

    success, cancelled_booking = cancel_booking(user_id, booking_id)

    if success and cancelled_booking:
        await callback.message.edit_text("Запись успешно отменена!")

        user_phone = registrated.get(user_id)
        try:
            user = await bot.get_chat(int(user_id))
            user_name = user.full_name or user.username or 'Неизвестно'
        except:
            user_name = 'Неизвестно'

        admin_notification = {
            "user_id": user_id,
            "user_name": user_name,
            "phone": user_phone,
            "service_name": cancelled_booking.get('service_name', 'Неизвестно'),
            "ticket_number": cancelled_booking.get('ticket_number', 'Неизвестно'),
            "type": "cancelled",
            "booking_type": cancelled_booking.get('type', 'неизвестно'),
            "cancelled_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        }

        if cancelled_booking.get('car_number'):
            admin_notification["car_number"] = cancelled_booking.get('car_number')
        if cancelled_booking.get('date_time'):
            admin_notification["datetime"] = cancelled_booking.get('date_time')
        if cancelled_booking.get('queue_position'):
            admin_notification["queue_position"] = cancelled_booking.get('queue_position')

        asyncio.create_task(notify_admins_about_cancellation(admin_notification))
    else:
        await callback.message.edit_text("Ошибка при отмене записи.")

    await asyncio.sleep(2)
    await booking_history(callback.message)


@dp.callback_query(F.data == "refresh_history")
async def refresh_history(callback: types.CallbackQuery):
    await booking_history(callback.message)


async def notify_admins_about_booking(booking_info: dict):
    global admin_id

    for admin in admin_id:
        try:
            user_name = booking_info.get('user_name', 'Неизвестно')
            if not user_name or user_name == 'Неизвестно':
                try:
                    user = await bot.get_chat(int(booking_info['user_id']))
                    user_name = user.full_name or user.username or 'Неизвестно'
                except:
                    user_name = 'Неизвестно'

            msg = (
                f"НОВАЯ ЗАПИСЬ!\n\n"
                f"Пользователь: {user_name}\n"
                f"ID: {booking_info.get('user_id')}\n"
                f"Телефон: {booking_info.get('phone')}\n"
                f"Услуга: {booking_info.get('service_name')}\n"
                f"Номер талона: {booking_info.get('ticket_number')}\n"
            )

            if booking_info.get('car_number'):
                msg += f"Автомобиль: {booking_info.get('car_number')}\n"

            if booking_info.get('categories') and len(booking_info.get('categories', [])) > 0:
                msg += f"Категорий выбрано: {len(booking_info.get('categories', []))}\n"

            if booking_info.get('type') == 'live':
                msg += f"Тип: Живая очередь\n"
                msg += f"Позиция: {booking_info.get('queue_position')}\n"
                msg += f"Ожидание: ~{booking_info.get('wait_time')} мин\n"
            else:
                msg += f"Тип: Запись на время\n"
                msg += f"Дата и время: {booking_info.get('datetime')}\n"

            msg += f"\nЗапись создана: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"

            await bot.send_message(int(admin), msg)
            logging.info(f"Notification sent to admin {admin}")

        except Exception as e:
            logging.error(f"Failed to notify admin {admin}: {e}")


async def notify_admins_about_cancellation(cancel_info: dict):
    global admin_id

    for admin in admin_id:
        try:
            msg = (
                f"ЗАПИСЬ ОТМЕНЕНА!\n\n"
                f"Пользователь: {cancel_info.get('user_name', 'Неизвестно')}\n"
                f"ID: {cancel_info.get('user_id')}\n"
                f"Телефон: {cancel_info.get('phone')}\n"
                f"Услуга: {cancel_info.get('service_name')}\n"
                f"Номер талона: {cancel_info.get('ticket_number')}\n"
            )

            if cancel_info.get('car_number'):
                msg += f"Автомобиль: {cancel_info.get('car_number')}\n"

            if cancel_info.get('booking_type') == 'живая_очередь':
                msg += f"Тип: Живая очередь\n"
                if cancel_info.get('queue_position'):
                    msg += f"Позиция: {cancel_info.get('queue_position')}\n"
            else:
                msg += f"Тип: Запись на время\n"
                if cancel_info.get('datetime'):
                    msg += f"Дата и время: {cancel_info.get('datetime')}\n"

            msg += f"\nОтменена: {cancel_info.get('cancelled_at')}"

            await bot.send_message(int(admin), msg)
            logging.info(f"Cancellation notification sent to admin {admin}")

        except Exception as e:
            logging.error(f"Failed to send cancellation notification to admin {admin}: {e}")


@dp.message(F.text == "О компании")
async def about(message: types.Message):
    s = read('Abot.txt') or "Информация о компании"
    await message.answer(s, reply_markup=await get_back_to_menu_kb())


@dp.message(F.text == "Написать нам")
async def contact_us(message: types.Message):
    admin_contacts = read('contacts.txt') or "Контакты для связи отсутствуют"
    await message.answer(admin_contacts, reply_markup=await get_back_to_menu_kb())


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id in admin_id:
        kb = [
            [types.KeyboardButton(text="Ограничение доступа")],
            [types.KeyboardButton(text="Отправить сообщение")],
            [types.KeyboardButton(text="Редактировать 'О компании'")],
            [types.KeyboardButton(text="Редактировать контакты")],
            [types.KeyboardButton(text="Меню пользователя")]
        ]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
        await message.answer("Админ-панель:", reply_markup=keyboard)
    else:
        await message.answer('Нет прав')
        await cmd_help(message)


@dp.message(F.text == "Ограничение доступа")
async def ban_menu(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    kb = [
        [types.KeyboardButton(text="Добавить бан")],
        [types.KeyboardButton(text="Снять бан")],
        [types.KeyboardButton(text="Список забаненных")],
        [types.KeyboardButton(text="Назад")]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("Управление бан-листом:", reply_markup=keyboard)


@dp.message(F.text == "Добавить бан")
async def add_ban_start(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    await message.answer(
        "Введите ID пользователей:\n"
        "Пример:\n123456789\n987654321\n\n"
        "'Готово' - завершить\n"
        "'Отмена' - отменить"
    )
    await state.set_state(BanStates.waiting_for_ban_ids)
    await state.update_data(banned_ids=[])


@dp.message(BanStates.waiting_for_ban_ids)
async def process_ban_ids(message: types.Message, state: FSMContext):
    user_input = message.text.strip()

    if user_input.lower() == "отмена":
        await message.answer("Отменено", reply_markup=await get_admin_keyboard())
        await state.clear()
        return

    if user_input.lower() == "готово":
        data = await state.get_data()
        banned_ids = data.get('banned_ids', [])

        if banned_ids:
            success = 0
            for uid in banned_ids:
                if uid not in admin_id:
                    write_id('banned.txt', uid)
                    success += 1
            await message.answer(f"Забанено: {success}", reply_markup=await get_admin_keyboard())
        else:
            await message.answer("Нет ID", reply_markup=await get_admin_keyboard())

        await state.clear()
        return

    try:
        ids = user_input.split('\n')
        data = await state.get_data()
        banned_ids = data.get('banned_ids', [])

        for uid_str in ids:
            uid_str = uid_str.strip()
            if uid_str and uid_str.isdigit():
                if uid_str not in banned_ids:
                    banned_ids.append(uid_str)

        await state.update_data(banned_ids=banned_ids)
        await message.answer(f"В списке: {len(banned_ids)}\nВведите ещё или 'Готово'")

    except:
        await message.answer("Ошибка. Введите ID")


@dp.message(F.text == "Список забаненных")
async def list_banned(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    banned = read_simple('banned.txt') or []
    if banned:
        text = "Забаненные:\n" + "\n".join(banned)
    else:
        text = "Список пуст"

    await message.answer(text)


@dp.message(F.text == "Снять бан")
async def unban_menu(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    await message.answer("Функция в разработке")


@dp.message(F.text == "Редактировать 'О компании'")
async def edit_about_menu(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    await message.answer("Отправьте новый текст для раздела 'О компании':")
    await AboutStates.waiting_for_about_text


@dp.message(AboutStates.waiting_for_about_text)
async def save_about_us(message: types.Message, state: FSMContext):
    with open('Abot.txt', 'w', encoding='utf-8') as f:
        f.write(message.text)
    await message.answer("Сохранено", reply_markup=await get_admin_keyboard())
    await state.clear()


@dp.message(F.text == "Редактировать контакты")
async def edit_contacts_menu(message: types.Message):
    user_id = str(message.from_user.id)
    if user_id not in admin_id:
        return

    await message.answer("Отправьте новый текст для раздела 'Написать нам':")
    await AboutStates.waiting_for_contacts_text


@dp.message(AboutStates.waiting_for_contacts_text)
async def save_contacts(message: types.Message, state: FSMContext):
    with open('contacts.txt', 'w', encoding='utf-8') as f:
        f.write(message.text)
    await message.answer("Сохранено", reply_markup=await get_admin_keyboard())
    await state.clear()


@dp.message(F.text == "Меню пользователя")
@dp.message(F.text == "Назад")
async def back_to_user(message: types.Message):
    await cmd_help(message)


async def get_admin_keyboard():
    kb = [
        [types.KeyboardButton(text="Ограничение доступа")],
        [types.KeyboardButton(text="Отправить сообщение")],
        [types.KeyboardButton(text="Редактировать 'О компании'")],
        [types.KeyboardButton(text="Редактировать контакты")],
        [types.KeyboardButton(text="Меню пользователя")]
    ]
    return types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


@dp.message()
async def echo_handler(message: types.Message):
    if message.contact:
        return
    await message.answer(f"{message.text}")


async def main():
    asyncio.create_task(reminder_scheduler())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
