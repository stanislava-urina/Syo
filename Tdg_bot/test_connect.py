import requests
from typing import List, Dict, Any, Optional


class QueueAPIError(Exception):
    """Базовое исключение для нашего API"""
    pass


class QueueClient:
    def __init__(self, token: str, host: str = "http://api.qms.kn-k.ru"):
        self.base_url = f"{host.rstrip('/')}/api/v1/Integration"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    def _request(self, method: str, endpoint: str, params: dict = None, json_data: dict = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                timeout=15
            )

            # Если сервер выдал 500, пробуем прочитать детали ошибки
            if response.status_code == 500:
                raise QueueAPIError(f"Ошибка сервера (500). Проверьте корректность ID. Ответ: {response.text[:100]}")

            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            raise QueueAPIError(f"Ошибка запроса: {e}")

    # 1. Список услуг
    def get_services(self) -> List[Dict[str, Any]]:
        return self._request("GET", "Services")

    # 2. Расчет времени ожидания
    def get_wait_time(self, service_id: str) -> Dict[str, Any]:
        return self._request("GET", "WaitTime", params={"service_id": service_id})

    # 3. Доступные интервалы
    def get_availability_slots(self, service_id: str) -> Dict[str, Any]:
        return self._request("GET", "AvailabilitySlots", params={"service_id": service_id})

    # 4. Регистрация в живую очередь
    def register_live(self, service_id: str, category_ids: List[str] = None) -> Dict[str, Any]:
        payload = {
            "service_id": service_id,
            "array_category_id": category_ids or []
        }
        return self._request("POST", "Register", json_data=payload)

    # 5. Бронирование слота
    def book_slot(self, service_id: str, slot_id: str, category_ids: List[str] = None) -> Dict[str, Any]:
        payload = {
            "service_id": service_id,
            "slot_id": slot_id,
            "array_category_id": category_ids or []
        }
        return self._request("POST", "Book", json_data=payload)

    # 6. Получение категорий
    def get_categories(self, service_id: str) -> List[Dict[str, Any]]:
        data = self._request("GET", "Categories", params={"service_id": service_id})
        return data.get("categories", [])

    # 7. Закрытие талона
    def close_ticket(self, ticket_id: str) -> Dict[str, Any]:
        payload = {"ticket_id": ticket_id}
        return self._request("PUT", "Close", json_data=payload)
# --- Логика приложения (CLI) ---

class QueueApp:
    def __init__(self, client: QueueClient):
        self.api = client
        self.cached_services = []

    def show_services(self):
        try:
            self.cached_services = self.api.get_services()
            print(f"\n✅ Список услуг ({len(self.cached_services)}):")
            for idx, s in enumerate(self.cached_services, 1):
                print(f"  {idx}. [{s['id']}] {s['name']}")
        except QueueAPIError as e:
            print(f"❌ {e}")

    def run(self):
        while True:
            print("\n--- МЕНЮ ---")
            print("1. Список услуг\n2. Время ожидания\n3. Регистрация\n0. Выход")
            choice = input(">> ")

            if choice == "1":
                self.show_services()
            elif choice == "2":
                sid = input("Введите ID услуги: ")
                try:
                    res = self.api.get_wait_time(sid)
                    print(f"⏱ Ожидание: {res['wait_time_minutes']} мин. Очередь: {res['queue_ahead']}")
                except QueueAPIError as e:
                    print(f"❌ {e}")
            elif choice == "0":
                break


if __name__ == "__main__":
    TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBdXRoZW50aWNhdGlvblR5cGUiOiJVc2VybmFtZVBhc3N3b3JkIiwicm9sZXMiOiJhZG1pbiIsInJvbGVJZCI6ImFmMjUxMTA5LTY4ZjgtNGYzZi04YjZhLTY2YjE4NmQ3ZDBiMiIsInN1YiI6IjNmZWY5M2JjLThlNTYtNGM0Mi04YmI5LTRmMDE0NTY2Njc5NSIsImV4cCI6MTczNzk2NzM3MywiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1NDk4OC8iLCJhdWQiOiJodHRwOi8vbG9jYWxob3N0OjU0OTg4LyJ9.9hNwFNN94CgNRZM1363tgRASVw6ualS0zPMeO2EbMbg"
    client = QueueClient(TOKEN)
    app = QueueApp(client)
    app.run()