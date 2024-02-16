import os.path
import re

import selenium.common.exceptions
from selenium import webdriver
import time
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.common.by import By
import logging
import json
import requests
import datetime
from threading import Thread
from queue import Queue
from config import num_workers

tasks_queue = Queue()


RESULT = [[] for i in range(num_workers)]


def json_save(data, path):
    with open(path, 'w', encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def json_read(path):
    with open(path, 'r', encoding="utf-8") as f:
        return json.load(f)


def get_phone_page(url) -> BeautifulSoup:
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 YaBrowser/23.11.0.0 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        # service = Service(ChromeDriverManager().install()),
        browser = webdriver.Chrome(options=options)
        browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                  const newProto = navigator.__proto__
                  delete newProto.webdriver
                  navigator.__proto__ = newProto
                  """
        })
        browser.get(url)

        WebDriverWait(browser, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//button[normalize-space()="Показать телефон"]'))).click()

        WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[4]/div/div[2]/div/button")))

        html = browser.page_source

        browser.quit()

        soup = BeautifulSoup(html, "lxml")

        return soup
    except selenium.common.exceptions.TimeoutException:
        pass

    except Exception as e:
        logging.error(f"Произошла ошибка selenium: {str(e)}")


def normalize_time_publication(time_str: str):
    res = time_str.replace("\xa0", ' ')
    if res == "Добавлено вчера":
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5) - datetime.timedelta(hours=24)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    elif "менее" in res:
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    elif "назад" in res:
        res = int(res.split("Добавлено ")[1].split(" ")[0])
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5) - datetime.timedelta(hours=res)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    elif "202" in res:

        res = res.replace("Добавлено ", "")
        res = res.replace(" г.", '').replace("декабря", "12").replace("октября", "10").replace("февраля", "2") \
            .replace("января", "1").replace("марта", "3").replace("апреля", "4").replace("мая", "5") \
            .replace("июня", "6").replace("июля", "7").replace("августа", "8").replace("сентября", "9") \
            .replace("ноября", "11")

        res = datetime.datetime.strptime(res, "%d %m %Y")
        res = str(res).replace(" ", "T") + "Z"
    else:
        res = res.replace("Добавлено ", "")
        res = res.replace(" г.", '').replace("декабря", "12").replace("октября", "10").replace("февраля", "2") \
            .replace("января", "1").replace("марта", "3").replace("апреля", "4").replace("мая", "5") \
            .replace("июня", "6").replace("июля", "7").replace("августа", "8").replace("сентября", "9") \
            .replace("ноября", "11").strip()
        res = datetime.datetime.strptime(res, "%d %m")
        res = res.replace(year=2024)
        res = str(res).replace(" ", "T") + "Z"

    return res



def normalize_time_update(time_str: str):
    res = time_str.replace("\xa0", ' ')
    if "обновлено менее" in res:
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    elif "назад" in res:
        res = int(res.split("обновлено ")[1].split(" ")[0])
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5) - datetime.timedelta(hours=res)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    elif "вчера" in res:
        res = datetime.datetime.utcnow() + datetime.timedelta(hours=5) - datetime.timedelta(days=1)
        res = f"{res:%Y-%m-%dT%H:%M:%S%ZZ}"
    else:
        res = res.replace("обновлено ", "")
        res = res.replace(" г.", '').replace("декабря", "12").replace("октября", "10").replace("февраля", "2") \
            .replace("января", "1").replace("марта", "3").replace("апреля", "4").replace("мая", "5") \
            .replace("июня", "6").replace("июля", "7").replace("августа", "8").replace("сентября", "9") \
            .replace("ноября", "11").strip()
        res = datetime.datetime.strptime(res, "%d %m")
        res = res.replace(year=2024)
        res = str(res).replace(" ", "T") + "Z"

    return res


def get_plots_params(data):
    apartment_params = {}

    # Параметры квартиры
    plot_type = None
    plot_area = None
    try:
        apart_data = data.get("realtyObject", None)


        plot_type = apart_data["status2"]
        if plot_type is not None:
            plot_type = plot_type.get("formatted", None)

        plot_area = round(float(apart_data["area"].get("value", None)), 1)


    except Exception as e:
        logging.error(f"Ошибка в получении параметров участка: {str(e)}")

    apartment_params["plot_type"] = plot_type
    apartment_params["plot_area"] = plot_area

    return apartment_params

def get_rooms_params(data):
    apartment_params = {}

    # Параметры квартиры
    sell_type = None
    total_area = None
    rooms_to_sell = None
    rooms_in_apartment = None
    kitchen_area = None
    floor = None
    floors = None
    building_year = None
    walls_material = None
    try:
        apart_data = data.get("realtyObject", None)

        rooms_to_sell = apart_data.get("roomCount", None)

        rooms_in_apartment = apart_data["enclosingFlat"].get("roomsCount", None)

        total_area = apart_data.get("livingArea", None)
        if total_area is not None:
            total_area = float(total_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        kitchen_area = apart_data["enclosingFlat"].get("kitchenArea", None)
        if kitchen_area is not None:
            kitchen_area = float(kitchen_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        floor = apart_data["enclosingFlat"].get("floor", None)

        floors = apart_data["enclosingFlat"]["building"].get("floorsTotal", None)

        walls_material = apart_data["enclosingFlat"]["building"].get("wallsType", None)
        match walls_material:
            case "PANEL":
                walls_material = "Панель"
            case "UNKNOWN":
                walls_material = None
            case "BRICK":
                walls_material = "Кирпич"
            case "MONOLITH":
                walls_material = "Монолит"
            case "BLOCK":
                walls_material = "Блок"
            case _:
                walls_material = None
                logging.error("Unknown wall type")

        building_year = apart_data["enclosingFlat"]["building"]["newBuildingStatus"]
        if building_year is not None:
            building_year.get("builtYear", None)

    except Exception as e:
        logging.error(f"Ошибка в получении параметров комнаты: {str(e)}")
    # Условие продажи
    try:
        sell_type = data["dealType"]["sellTypeFormatted"]
    except Exception as e:
        logging.error(f"Ошибка в получении условия продажи: {str(e)}")

    apartment_params["total_area"] = total_area
    apartment_params["rooms_to_sell"] = rooms_to_sell
    apartment_params["rooms_in_flat"] = rooms_in_apartment
    apartment_params["kitchen_area"] = kitchen_area
    apartment_params["floor"] = floor
    apartment_params["floors"] = floors
    apartment_params["building_year"] = building_year
    apartment_params["walls_material"] = walls_material
    apartment_params["sell_type"] = sell_type

    return apartment_params

def get_house_params(data):
    apartment_params = {}

    # Параметры дома
    house_type = None
    rooms_count = None
    total_area = None
    plot_area = None
    floors = None
    building_year = None
    try:

        apart_data = data.get("realtyObject", None)

        house_type = apart_data["category2"].get("formatted", None)

        rooms_count = apart_data.get("roomsCount", None)

        total_area = apart_data.get("totalArea", None)
        if total_area is not None:
            total_area = float(total_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        plot_area = apart_data["land"]["area"].get("value", None)


        floors = apart_data["floorsTotal"]

        if apart_data["builtYear"] != 0:
            building_year = apart_data["builtYear"]


    except Exception as e:
        logging.error(f"Ошибка в получении параметров дома: {str(e)}")

    apartment_params["house_type"] = house_type
    apartment_params["rooms_count"] = rooms_count
    apartment_params["total_area"] = total_area
    apartment_params["plot_area"] = plot_area
    apartment_params["floors"] = floors
    apartment_params["building_year"] = building_year


    return apartment_params


def get_flats_params(data):
    apartment_params = {}

    # Параметры квартиры
    apart_type = None
    total_area = None
    kitchen_area = None
    living_area = None
    floor = None
    floors = None
    building_year = None
    walls_material = None
    try:
        apart_data = data.get("realtyObject", None)

        apart_type = apart_data.get("roomsCount", None)

        total_area = apart_data.get("totalArea", None)
        if total_area is not None:
            total_area = float(total_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        kitchen_area = apart_data.get("kitchenArea", None)
        if kitchen_area is not None:
            kitchen_area = float(kitchen_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        living_area = apart_data.get("livingArea", None)
        if living_area is not None:
            living_area = float(living_area.get("formatted").replace("\xa0м²", '').replace(",", '.'))

        floor = apart_data.get("floor", None)

        floors = apart_data.get("building", None).get("floorsTotal", None)

        walls_material = apart_data["building"].get("wallsType", None)
        match walls_material:
            case "PANEL":
                walls_material = "Панель"
            case "UNKNOWN":
                walls_material = None
            case "BRICK":
                walls_material = "Кирпич"
            case "MONOLITH":
                walls_material = "Монолит"
            case "BLOCK":
                walls_material = "Блок"
            case _:
                walls_material = None
                logging.error("Unknown wall type")

        building_year = apart_data["building"]["newBuildingStatus"]
        if building_year is not None:
            building_year.get("builtYear", None)

    except Exception as e:
        logging.error(f"Ошибка в получении параметров комнаты: {str(e)}")
    # Условие продажи
    sell_type = None
    try:
        sell_type = data["dealType"]["sellTypeFormatted"]
    except Exception as e:
        logging.error(f"Ошибка в получении условия продажи: {str(e)}")

    apartment_params["apart_type"] = apart_type
    apartment_params["total_area"] = total_area
    apartment_params["kitchen_area"] = kitchen_area
    apartment_params["living_area"] = living_area
    apartment_params["floor"] = floor
    apartment_params["floors"] = floors
    apartment_params["building_year"] = building_year
    apartment_params["walls_material"] = walls_material
    apartment_params["sell_type"] = sell_type

    return apartment_params


def get_apartment_data(soup: BeautifulSoup, apart_name: str):
    try:
        obj = {}
        contact_information = {}
        date = {}
        location = {}

        raw_script_data = soup.find("script", id="vtbeco-search-initial-state").text
        script_data: dict = json.loads(raw_script_data)

        # ID and data
        ID = None
        data = None
        for key in script_data.keys():
            if 'Offer{"offerId":' in key:
                data = script_data[key]["data"]["offer"]
                ID = data["id"]

        obj["ID"] = ID

        json_save(data, "script.json")  # TODO delete

        # Цена
        price = None
        try:
            price = int(data["dealType"]["price"]["value"] / 100)
        except Exception as e:
            logging.error(str(e))
        obj["price"] = price

        # Контактная информация
        company = None
        name = None
        try:
            raw_company = data["seller"]["organizationName"]
            if raw_company is not None:
                if len(raw_company) != 0:
                    company = raw_company
            raw_name = data["seller"]["name"]
            if raw_name is not None:
                if len(raw_name) != 0:
                    company = raw_name
        except Exception as e:
            logging.error(f"Ошибка в получении контактной информации: {str(e)}")
        contact_information["company"] = company
        contact_information["name"] = name

        # Телефон
        phone = None
        try:
            phone = soup.find(class_="OfferPhoneModalDesktop__phone").get("href").split("+")[-1]
        except Exception as e:
            logging.error(f"Ошибка в получении телефона: {str(e)}")

        contact_information["phone"] = phone

        # url
        url = None
        try:
            url = soup.find("link", rel="canonical")["href"]
        except Exception as e:
            logging.error(f"Ошибка в получении ссылки: {str(e)}")
        obj["url"] = url


        # Дата добавления
        available = True
        update_date = None
        publication_date = None
        data_block = data["status"]["createdActualizeFormatted"].split(", ")
        try:
            publication_date = data_block[0]
            publication_date = normalize_time_publication(publication_date)
            if not re.match(r"([0-9]){4}(-)([0-9]){2}(-)([0-9]){2}(T)([0-9]){2}(:)([0-9]){2}(:)([0-9]){2}(Z)",
                            publication_date):
                logging.error(f"Ошибка в получении даты: {publication_date}, url={url}")

            update_date = data_block[1]
            update_date = normalize_time_update(update_date)

            if not re.match(r"([0-9]){4}(-)([0-9]){2}(-)([0-9]){2}(T)([0-9]){2}(:)([0-9]){2}(:)([0-9]){2}(Z)",
                            update_date):
                logging.error(f"Ошибка в получении даты: {update_date}, url={url}")

            available = not data["status"]["isDeleted"]
        except Exception as e:
            logging.error(f"Ошибка в получении даты: {str(e)}")

        date["publication_date"] = publication_date
        date["update_date"] = update_date
        date["available"] = available

        # Адрес
        loc = data.get("location", None)
        region = loc.get("narrowRegion", None)
        locality = loc.get("locality", None)
        district = loc.get("narrowDistrict", None)
        street = loc.get("street", None)
        building_number = loc.get("house", None)
        coordinates = loc.get("coordinates", None)
        try:

            if region is not None:
                region = region.get("name")
            if locality is not None:
                locality = locality.get("name")
            if district is not None:
                district = district.get("name")
            if street is not None:
                street = street.get("name")
            if building_number is not None:
                building_number = building_number.get("name")
            if coordinates is not None:
                coordinates = [coordinates.get("longitude"), coordinates.get("latitude")]
        except Exception as e:
            logging.error(f"Ошибка в получении адреса: {str(e)}")

        location["region"] = region
        location["locality"] = locality
        location["district"] = district
        location["street"] = street
        location["building_number"] = building_number
        location["coordinates"] = coordinates

        apartment_params = {}
        match apart_name:
            case "flats":
                apartment_params = get_flats_params(data)
            case "rooms":
                apartment_params = get_rooms_params(data)
            case "houses":
                apartment_params = get_house_params(data)
            case "plots":
                apartment_params = get_plots_params(data)

        description = data.get("description", None)

        photos = []

        other_photo = data["gallery"]["images"]
        for photo in other_photo:
            photos.append("https://img.m2.ru" + photo["originPath"])

        obj["contact_information"] = contact_information
        obj["date"] = date
        obj["location"] = location
        obj["apartment_params"] = apartment_params
        obj["description"] = description
        obj["photos"] = photos

        logging.info(f"Получены данные квартиры: {ID}")
        return obj
    except Exception as e:
        logging.error(f"Произошла ошибка при получении данных: {str(e)}")


def get_pagination(url):
    res_get = requests.get(
        url=url)

    soup = BeautifulSoup(res_get.text, 'lxml')
    pagination = int(soup.find(class_="paginator-module__pages___1azUM").findAll("li")[-2].text)

    logging.info(f"pagination: {pagination}")
    return pagination


def get_page_urls(url):
    res = requests.get(url)

    soup = BeautifulSoup(res.text, "lxml")

    raw_url_list = soup.find(class_="OffersSearchList__list").findAll(class_="OffersSearchList__item")

    res = []

    for apart in raw_url_list:
        url = apart.find(class_="LinkSnippet LinkSnippet_fullWidth LinkSnippet_hover")
        if url is not None:
            url = url["href"]
            res.append(url)

    return res


def parse_page(page_num: int, url: str, sort: str, path: str, name: str):
    start_time = time.time()
    if page_num == 1:
        url = f"{url}?sort={sort}"
    else:
        url = f"{url}?pageNumber={page_num}&sort={sort}"

    apart_urls = get_page_urls(url)

    page_res = []
    for apart_url in apart_urls:
        tasks_queue.put((apart_url, name))

    tasks_queue.join()

    for i in range(num_workers):

        for j in range(len(RESULT[i])):
            res = RESULT[i][j]
            page_res.append(res)

        RESULT[i] = []

    json_save(page_res, f"{path}/raw_json/{page_num}_page.json")
    logging.info(f"{page_num} страница обработана, времени затрачено: {time.time() - start_time}")


def run_worker(i: int):
    while True:
        task = tasks_queue.get()
        if task is None:
            break

        url = task[0]
        name = task[1]

        soup = get_phone_page(url)
        apart_data = get_apartment_data(soup, name)

        RESULT[i].append(apart_data)

        tasks_queue.task_done()


def register_workers():
    for i in range(num_workers):
        t = Thread(target=run_worker, args=(i,))
        t.start()


def union_pages(name):
    result = []

    directory = os.listdir(f'{name}/raw_json/')
    for json_filename in directory:
        apart_data = json_read(f"{name}/raw_json/" + json_filename)
        for apart in apart_data:
            if apart is not None:
                result.append(apart)
        os.remove(f"{name}/raw_json/" + json_filename)

    json_save(result, f"{name}/result.json")


def scraper(path: str, url: str, deep: int, sort: str, name: str) -> object:
    register_workers()
    pagination = get_pagination(f'{url}?sort={sort}')
    if deep is not None:
        if deep <= pagination:
            pagination = deep
    # pagination = 1
    for page in range(1, pagination + 1):
        if os.path.isfile(f"{path}/raw_json/{page}_page.json"):
            data = json_read(f"{path}raw_json/{page}_page.json")
            if len(data) == 20:
                logging.info(f"Найдена {page} страница")
                continue
        parse_page(page, url, sort, path, name)

    tasks_queue.join()

    # Останавливаем все потоки
    for i in range(num_workers):
        tasks_queue.put(None)

    union_pages(path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, filemode="a",
                        format="%(asctime)s %(levelname)s %(message)s")
    main()
