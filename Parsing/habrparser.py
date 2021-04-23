import re, string
import datetime

import requests
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, Future

import pandas as pd

# utility
import time


class HabrParser:
    """
    Мой класс содержащий основные необходимые функции.
    Общая идея идти назад во времени, скачивая посты и в случае достаточного размера сохранять их.
    Pandas для хранения, tqdm для слежки за прогрессом и BeautifulSoup для парсинга.
    """

    def __init__(self):
        self.pages = []
        self.last_parse_date = None
        self.log_list = []

    def _big_num_fix(self, number_string: str) -> float:
        # Строку по типу 21к привратить в число 21000.
        if number_string[-1] == "k":
            return float(number_string.replace(",", ".")[:-1]) * 1000
        return float(number_string.replace(",", "."))

    def _beautiful_num_fix(self, text_string: str) -> float:
        return float(
            text_string.replace(",", ".").replace("–", "-").replace("\xa0", "")
        )

    def text_cleaner(self, text: str) -> str:
        # Удаляются ссылки, знаки пунктуации а также \r\n
        text_no_slash = text.replace("\r", " ").replace("\n", " ")
        url_regex = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""
        text_no_url = re.sub(url_regex, "", text_no_slash)

        clean_text = text_no_url.translate(str.maketrans("", "", string.punctuation))
        return clean_text

    def parse_page(self, post_idx: int, min_post_size=2000) -> tuple:
        # Скачиваю и обрабатываю страницу, возвращая tuple
        req = requests.get("http://habrahabr.ru/post/" + str(post_idx), timeout=5)
        if req.status_code != 200:
            return ()
        soup = BeautifulSoup(req.text)

        # Обрабатываю текст и проверяю размер, если меньше 2к то выходим
        raw_post_text = soup.find("div", {"id": "post-content-body"}).text
        clean_text = self.text_cleaner(raw_post_text)
        if len(clean_text) < min_post_size:
            return ()
        # Разбираю на части
        date = soup.find("span", {"class": "post__time"})
        date = datetime.datetime.strptime(
            date.get("data-time_published"), "%Y-%m-%dT%H:%MZ"
        )
        title = soup.find("span", {"class": "post__title-text"}).text

        hubs = []
        temp = soup.find(
            "ul", {"class": "inline-list inline-list_fav-tags js-post-hubs"}
        ).findChildren("a")
        for hub in temp:
            hubs.append(hub.text.strip())

        tags = []
        temp = soup.find(
            "ul", {"class": "inline-list inline-list_fav-tags js-post-tags"}
        ).findChildren("a")
        for tag in temp:
            tags.append(tag.text.strip())

        # Блоки try для случаев отсутствия имени автора или титула (титула не видел но на всякий)
        try:
            author = soup.find("a", {"class": "user-info__fullname"}).text
        except Exception:
            author = None
        nickname = soup.find("a", {"class": "user-info__nickname"}).text

        try:
            author_title = soup.find("div", {"class": "user-info__specialization"}).text
        except Exception:
            author_title = None

        # Карма и рэйтинг типа int
        try:
            author_karma = self._beautiful_num_fix(
                soup.find("a", {"class": "user-info__stats-item stacked-counter"})
                .contents[1]
                .text
            )
        except Exception:
            author_karma = None
        try:
            author_rating = self._beautiful_num_fix(
                soup.find(
                    "a",
                    {
                        "class": "user-info__stats-item stacked-counter stacked-counter_rating"
                    },
                )
                .contents[1]
                .text
            )
        except Exception:
            author_rating = None

        temp = soup.find("div", {"class": "voting-wjt voting-wjt_post js-post-vote"})
        post_rating = self._beautiful_num_fix(temp.findChildren("span")[0].text)

        times_saved = self._big_num_fix(
            soup.find("span", {"class": "bookmark__counter js-favs_count"}).text
        )
        views = self._big_num_fix(
            soup.find("span", {"class": "post-stats__views-count"}).text
        )

        # Получаем количество комментариев, с проверкой на слово "Комментировать", что равносильно 0
        temp = soup.find("span", {"id": "post-stats-comments-count"}).text
        if temp == "Комментировать":
            comment_count = 0
        else:
            comment_count = self._big_num_fix(temp)

        return (
            post_idx,
            author,
            nickname,
            author_title,
            author_karma,
            author_rating,
            title,
            date,
            hubs,
            tags,
            post_rating,
            times_saved,
            views,
            comment_count,
            raw_post_text,
            clean_text,
        )

    def _threaded_parse_page(self, post_idx: int, min_post_size=2000) -> tuple:
        # log_file = None ONLY beacause it's always given the log and order of arguments
        page_log = None
        try:
            post = self.parse_page(post_idx, min_post_size)
        except TimeoutError:
            print("Timeout Error\n")
            print(post_idx)
            post = ()
            page_log = str(post_idx) + "\n"
        except:
            # в случае ошибок (не считая проблем с запросами, напримем 404) сохраняю индекс в лог
            print(post_idx)
            post = ()
            page_log = str(post_idx) + "\n"
        return (post, page_log)

    def multi_run(
        self,
        number_of_pages=100000,
        starting_idx=540000,
        min_post_size=2000,
        time_between=0.5,
        num_of_processes=10,
    ):
        """
        Многопоточный парсинг.
        Парсим number_of_pages страниц двигаясь вниз по индексу с номера starting_idx с
        паузами time_between секунд.
        Все данные сохранённые в классе будут УДАЛЕНЫ.

        """
        self.pages = []
        self.log_list = []
        # Индекст страницы для скачивания
        page_idx = starting_idx
        # Время для создания лог-файла и названия файла сохранения
        now = datetime.datetime.now()
        self.last_parse_date = now
        # создаю tqdm
        t = tqdm(total=number_of_pages)
        # создаю пул потоков
        executor = ThreadPoolExecutor(max_workers=num_of_processes)
        # счётчик подходящих страниц (раньше использовался напрямую tqdm, но во избежания ошибок интеграции заменён)
        counter = 0
        while counter < number_of_pages:
            # создаю временные переменные для обработки потоков и их результатов
            post = []
            futures = []
            for thr in range(num_of_processes):
                futures.append(
                    executor.submit(
                        self._threaded_parse_page, page_idx - thr, min_post_size
                    )
                )
            for thr in range(num_of_processes):
                res = futures[thr].result()
                # проверяю что нашлось. () - неподходящие данные или несуществующая страница
                # и если есть код страницы то сохраняю его в лог (а страница удаляется, т.к. ошибка)
                if res[0] != ():
                    post.append(res[0])

                elif res[1] is not None:
                    self.log_list.append(res[1])

                sleep(time_between)
            # сохраняю результаты и обновляю индекс
            self.pages.extend(post)
            t.update(len(post))
            counter += len(post)
            page_idx -= num_of_processes

        # по окончании вывожу дополнительную информацию в лог и консоль
        final_message = f"""
        Finished!
        Total pages: {len(self.pages)}
        Starting index: {starting_idx}
        Final index: {page_idx}
        Starting time : {now.strftime("%d/%m/%y--%H:%M:%S")}
        Ending time: {datetime.datetime.now().strftime("%d/%m/%y--%H:%M:%S")}"""

        self.log_list.append(final_message)
        print(final_message)

    def save(self, save_path="data/"):
        """

        Сохранить скаченные данные в DataFrame picklе, а также сохранить лог в файл.
        Если данных нет, то выводится сообщение.

        """
        if self.pages == 0 or self.last_parse_date is None:
            print("No data to save")
        else:
            df = pd.DataFrame(self.pages)
            df = df.set_axis(
                [
                    "post_idx",
                    "author",
                    "nickname",
                    "author_title",
                    "author_karma",
                    "author_rating",
                    "title",
                    "date",
                    "hubs",
                    "tags",
                    "post_rating",
                    "times_saved",
                    "views",
                    "comment_count",
                    "raw_post_text",
                    "clean_text",
                ],
                axis=1,
            )
            name = (
                "habr_" + self.last_parse_date.strftime("%d_%m_%y--%H_%M_%S") + ".pkl"
            )
            df.to_pickle(save_path + name)
            # сохранение логов
            log = open(
                save_path
                + "habr_logs_"
                + self.last_parse_date.strftime("%d_%m_%y--%H_%M_%S")
                + ".txt",
                "w",
            )
            log.write("".join(self.log_list))
            log.close()