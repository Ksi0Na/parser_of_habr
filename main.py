from typing import Any
from elasticsearch import Elasticsearch
import pika
import requests
from bs4 import BeautifulSoup
import hashlib
import json


class Parser:
    """
    Класс для парсинга данных с веб-страницы.
    """

    def __init__(
            self,
            url: str = 'https://habr.com/ru/flows/develop/top/weekly/',
            post_count: int = 0,
            post_author: list = None,
            post_header: list = None,
            post_time: list = None,
            post_link: list = None,
            post_text: list = None,
            post_id: list = None) -> None:
        """
        :param url:             адрес веб-страницы (по умолчанию 'https://habr.com/ru/flows/develop/top/weekly/')
        :param post_count:      количество постов на странице (по умолчанию 0)
        :param post_author:     список авторов постов (по умолчанию пустой список)
        :param post_header:     список заголовков постов (по умолчанию пустой список)
        :param post_time:       список времен создания постов (по умолчанию пустой список)
        :param post_link:       список ссылок на посты (по умолчанию пустой список)
        :param post_text:       список текстов постов (по умолчанию пустой список)
        :param post_id:         список хэшей md5 для каждого поста (по умолчанию пустой список)
        """
        self.url = url
        self.post_count = post_count

        self.post_author = [] if (post_author is None) else post_author
        self.post_header = [] if (post_header is None) else post_header
        self.post_time = [] if (post_time is None) else post_time
        self.post_link = [] if (post_link is None) else post_link
        self.post_text = [] if (post_text is None) else post_text
        self.post_id = [] if (post_id is None) else post_id

    def get_post_id(self):
        """
        Вычисляет идентификатор документа (хэш)
        """
        for i in range(self.post_count):
            data = f"{self.post_header[i]}{self.post_time[i]}".encode("utf-8")
            self.post_id.append(hashlib.md5(data).hexdigest())

    def get_soup(self, url: str) -> requests.Response:
        """
        Возвращает объект requests.Response для указанного URL-адреса.

        :param url: адрес веб-страницы
        :return: объект requests.Response
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.print_error_info(url, e)
            raise SystemExit

    @staticmethod
    def print_error_info(url: str, e: Exception) -> None:
        """
        Выводит информацию об ошибке при получении HTML-кода страницы.

        :param url: адрес веб-страницы
        :param e: объект исключения
        """
        error_text = f"Error occurred while getting HTML from {url}:"
        print(error_text)
        print(e)

    def get_data(self, response) -> None:
        """
        Извлекает данные с веб-страницы и заполняет соответствующие списки.

        :param response: объект Response от запроса к веб-странице
        """
        the_soup = BeautifulSoup(response.content, 'lxml')
        self.post_count = the_soup.find_all('article', {'class': 'tm-articles-list__item'}).__len__()


        for i in range(self.post_count):
            posts_of_page = the_soup.find_all('article', {'class': 'tm-articles-list__item'}).__getitem__(i)
            h2 = posts_of_page.find('h2')
            a = h2.find('a')
            author = posts_of_page.find('a', {'class': 'tm-user-info__username'}).get_text(separator='\n')

            self.post_author.append('\n'.join(line.strip() for line in author.splitlines() if line.strip()))
            self.post_header.append(a.text)
            self.post_time.append((posts_of_page.find('time').get_attribute_list('datetime'))[0])
            self.post_link.append('https://habr.com' + a.get_attribute_list('href')[0])

    def print_data(self) -> None:
        """
        Выводит данные о постах на экран.
        """
        for i in range(self.post_count):
            print('-' * 100)
            print(i)
            print(self.post_author[i])
            print(self.post_header[i])
            print(self.post_time[i])
            print(self.post_link[i])
            print(self.post_text[i])

    def get_text(self) -> None:
        """
        Получает тексты постов по каждой ссылке и заполняет список post_text.
        """
        count = self.post_count

        for i in range(count):
            response = self.get_soup(self.post_link[i])
            the_soup = BeautifulSoup(response.content, 'lxml')
            post = the_soup.find('article', {'class': ['tm-article-presenter__content']})

            text = post.find('div', {'id': 'post-content-body'}).get_text()

            self.post_text.append(text)


class ElasticsearchSearch:
    def __init__(self,
                 index_name: str,
                 doc_id: list = None) -> None:
        """
        :param index_name: наименование индекса
        :param doc_id: хэши документов (по умолчанию - пустой список)
        """
        self.index_name = index_name
        self.doc_id = [] if (doc_id is None) else doc_id

    def connect(self) -> None:
        """
        Устанавливаем соединение с Elasticsearch
        """
        self.es = Elasticsearch(
            hosts="http://elastic:elastic@localhost:9200/"
        )

    def create_index(self,
                     index_name: str) -> None:
        """
        Создаем индекс с основными полями

        :param index_name: наименование индекса
        """
        mappings = {
            "properties": {
                "link":     {"type": "keyword"},
                "author":   {"type": "keyword"},
                "time":     {"type": "date"},
                "header":   {"type": "text"},
                "text":     {"type": "text"}
            }
        }

        self.es.indices.create(index=index_name, mappings=mappings)

    def index_document(self,
                       doc_id: str,  header: str,
                       time:   str,  text:   str,
                       link:   str,  author: str) -> None:
        """
        Индексируем документ в Elasticsearch

        :param doc_id: хэш документа
        :param header: заголовок документа
        :param time: время создания поста на хабре
        :param text: текст документа
        :param link: ссылка на пост с хабра
        :param author: автор поста
        """
        doc = {
            "link": link,
            "author": author,
            "time": time,
            "header": header,
            "text": text
        }
        self.es.index(index=self.index_name, id=doc_id, document=doc)

    def document_exists(self,
                        doc_id: str) -> bool:
        """
        Проверяет наличие документа в индексе

        :param doc_id: хэш документа
        :return: True/False
        """
        return self.es.exists(index=self.index_name, id=doc_id)

    def search_documents(self,
                         my_query: str) -> Any:
        """
        Ищет документы с заданной строкой

        :param my_query: строка, по которой ищем документы
        :return: документы, в которых найдена строка
        """
        query = {
            "match": {
                "text": my_query
            }
        }

        response = self.es.search(index=self.index_name, query=query)
        hits = response["hits"]["hits"]
        formatted_hits = json.dumps(hits, indent=4, ensure_ascii=False)\
            .replace("\\n", "\n").replace("\\r", "\r")
        print(len(hits))
        return formatted_hits

    def logic_search_documents(self, my_query: str) -> Any:
        """
        Ищет документы с заданной строкой

        :param my_query: строка, по которой ищем документы
        :return: найденные документы
        """
        query = {
            "bool": {
                "should": [
                    {"match": {"field1": my_query}},
                    {"match": {"field2": my_query}},
                    {"match": {"field3": my_query}}
                ],
                "must": [
                    {"match": {"field4": my_query}}
                ],
                "must_not": [
                    {"match": {"field5": my_query}}
                ],
                "filter": [
                    {"term": {"field6": "value"}}
                ]
            }
        }

        response = self.es.search(index=self.index_name, query=query)
        hits = response["hits"]["hits"]
        formatted_hits = json.dumps(hits, indent=4, ensure_ascii=False) \
            .replace("\\n", "\n").replace("\\r", "\r")
        return formatted_hits

    def get_all_documents(self) -> Any:
        """
        Найдет все документы по заданному индексу

        :return: все документы
        """
        query = {"match_all": {}}
        response = self.es.search(index=self.index_name, query=query)
        hits = response["hits"]["hits"]
        formatted_hits = json.dumps(hits, indent=4, ensure_ascii=False)\
            .replace("\\n", "\n").replace("\\r", "\r")
        return formatted_hits

    def delete_index(self,
                     index_name: str) -> None:
        """
        Удаление данных индекса из elasticsearch

        :param index_name: наименование индекса
        """
        self.es.indices.clear_cache(index=index_name)
        self.es.indices.delete(index=index_name)


class RabbitMQ:
    """
    Класс для работы с RabbitMQ.
    """

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 5672,
                 username: str = 'guest',
                 password: str = 'guest',
                 vhost: str = '/',
                 queue_name: str = 'default_queue',
                 exchange_type: str = 'direct',
                 connection: pika.BlockingConnection = None,
                 channel: pika.BlockingConnection = None) -> None:
        """
        :param host: адрес хоста RabbitMQ (по умолчанию 'localhost')
        :param port: порт RabbitMQ (по умолчанию 5672)
        :param username: имя пользователя для аутентификации в RabbitMQ (по умолчанию 'guest')
        :param password: пароль для аутентификации в RabbitMQ (по умолчанию 'guest')
        :param vhost: виртуальный хост RabbitMQ (по умолчанию '/')
        :param queue_name: имя очереди RabbitMQ (по умолчанию 'default_queue')
        :param exchange_type: тип обменника RabbitMQ (по умолчанию 'direct')
        :param connection: объект подключения к RabbitMQ (по умолчанию None)
        :param channel: объект канала RabbitMQ (по умолчанию None)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.queue_name = queue_name
        self.exchange_name = queue_name
        self.exchange_type = exchange_type
        self.connection = connection
        self.channel = channel

    def channel_start(self) -> None:
        """
        Создает необходимые объекты для запуска.
        """
        self.create_connection()
        self.channel = self.connection.channel()

    def queue_start(self,
                    name: str = None) -> None:
        """
        Создает очередь и exchange с заданным именем 'name', а также связывает их.

        :param name: имя очереди и обменника (по умолчанию None)
        """
        self.queue_declare(name)
        self.exchange_declare(name)
        self.queue_bind(name, name)

    def send_task_to_queue(self,
                           task_result: str,
                           queue: str = None) -> None:
        """
        Отправка результата задачи в очередь.

        :param task_result: результат задачи (строка)
        :param queue: имя очереди (по умолчанию None)
        """
        # Задаем queue_name, если не указан
        if queue is None:
            queue = self.queue_name

        # Преобразуем task_result в строку
        task_result_str = str(task_result)

        # Отправляем результат задачи в очередь
        self.channel.basic_publish(exchange='',
                                   routing_key=queue,
                                   body=task_result_str)

    def execute_function_from_queue(self,
                                    queue: str = None) -> Any:
        """
        Получает результат задачи из очереди RabbitMQ и выполняет функцию.

        :param queue: имя очереди (по умолчанию None)
        :return: результат задачи
        """
        # Задаем queue_name, если не указан
        if queue is None:
            queue = self.queue_name

        try:
            method_frame, _, body = self.channel.basic_get(queue, auto_ack=False)

            if not method_frame:
                return None

            # Получаем результат задачи
            result = body.decode('utf-8')

            # Подтверждаем получение сообщения
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            return result
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error occurred while connecting to RabbitMQ: {e}")
            raise SystemExit
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"Channel was closed by broker: {e}")
            raise SystemExit
        except pika.exceptions.ConnectionClosed as e:
            print(f"Connection was closed: {e}")
            raise SystemExit

    def create_connection(self) -> pika.BlockingConnection:
        """
        Устанавливает соединение с RabbitMQ.

        :return: объект типа pika.BlockingConnection при успешном соединении, иначе None
        """
        try:
            credentials = pika.PlainCredentials(username=self.username,
                                                password=self.password)
            parameters = pika.ConnectionParameters(host=self.host,
                                                   port=self.port,
                                                   virtual_host=self.vhost,
                                                   credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            return self.connection
        except pika.exceptions.AMQPConnectionError as error:
            print(f"Failed to connect to RabbitMQ on {self.host}:{self.port}: {error}")
            raise SystemExit

    def queue_close(self,
                    queue: str = None) -> None:
        """
        Закрывает очередь RabbitMQ.

        :param queue: имя очереди (по умолчанию None)
        """
        # Задаем queue_name, если не указан
        if queue is None:
            queue = self.queue_name
        if self.connection is not None:
            # Удаляем очередь
            self.channel.queue_delete(queue=queue)

    def channel_close(self) -> None:
        """
        Закрывает канал и соединение
        """
        # Закрываем канал и соединение
        self.channel.close()
        self.connection.close()

    def queue_declare(self,
                      queue: str = None) -> None:
        """
        Создает очередь RabbitMQ.

        :param queue: имя очереди (по умолчанию None)
        """
        # Задаем queue_name, если не указан
        if queue is None:
            queue = self.queue_name

        # Создание очереди
        self.channel.queue_declare(queue=queue,
                                   durable=True)

    def exchange_declare(self,
                         exchange: str = None) -> None:
        """
        Создает обменник RabbitMQ.

        :param exchange: имя обменника (по умолчанию None)
        """
        # Задаем exchange_name, если не указан
        if exchange is None:
            exchange = self.exchange_name

        self.channel.exchange_declare(exchange=exchange,
                                      exchange_type=self.exchange_type,
                                      durable=True)

    def queue_bind(self,
                   queue: str = None,
                   exchange: str = None) -> None:
        """
        Привязывает очередь RabbitMQ к обменнику.

        :param queue: имя очереди (по умолчанию None)
        :param exchange: имя обменника (по умолчанию None)
        """
        # Задаем queue_name, если не указан
        if queue is None:
            queue = self.queue_name

        # Задаем exchange_name, если не указан
        if exchange is None:
            queue = self.exchange_name

        self.channel.queue_bind(queue=queue,
                                exchange=exchange,
                                routing_key='')


def parser_quick_start() -> Parser:
    """
    Функция отвечает за:                          \n
    *   создание объекта класса Parser            \n
    *   получение данных со страницы по url       \n
    *   заполнение атрибутов объекта класса:      \n
        -   post_count:      количество постов на странице (по умолчанию 0)                 \n
        -   post_author:     список авторов постов (по умолчанию пустой список)             \n
        -   post_header:     список заголовков постов (по умолчанию пустой список)          \n
        -   post_time:       список времен создания постов (по умолчанию пустой список)     \n
        -   post_link:       список ссылок на посты (по умолчанию пустой список)            \n
        -   post_text:       список текстов постов (по умолчанию пустой список)             \n
        -   post_id:         список хэшей md5 для каждого поста (по умолчанию пустой список)\n

    :return: объект класса Parser
    """
    p = Parser()
    resp = p.get_soup(p.url)
    p.get_data(resp)
    p.get_text()
    p.get_post_id()
    return p


def parser_end(p: Parser) -> None:
    """
    Очищает списки объекта класса Parser и удаляет сам объект

    :param p: объект класса Parser
    """
    p.post_author.clear()
    p.post_header.clear()
    p.post_time.clear()
    p.post_link.clear()
    p.post_text.clear()
    p.post_id.clear()
    del p


def rabbit_quick_start(p: Parser) -> RabbitMQ:
    """
    Функция отвечает за:                    \n
    *   создание объекта класса RabbitMQ    \n
    *   запуск канала                       \n
    *   создание очередей                   \n
    *   отправку сообщений в очереди

    :param p: объект класса Parser
    :return: объект класса RabbitMQ
    """
    try:
        # Создаем объект класса RabbitMQ
        q = RabbitMQ()

        # Запускаем channel
        q.channel_start()

        # Запускаем очереди
        queue_names = ["authors", "headers", "times", "links", "texts"]
        for queue in queue_names:
            q.queue_start(queue)

        # Отправляем сообщения в очереди
        for i in range(p.post_count):
            queue_data = [p.post_author[i], p.post_header[i], p.post_time[i], p.post_link[i], p.post_text[i]]
            for queue, data in zip(queue_names, queue_data):
                q.send_task_to_queue(data, queue=queue)

        return q

    except Exception as e:
        # Закрытие очередей и соединения в случае ошибки
        for queue in queue_names:
            q.queue_close(queue=queue)
        q.channel_close()

        # Вывод ошибки
        print(f"An error occurred during execution: {e}")
        raise SystemExit


def rabbit_end(q: RabbitMQ,
               es: ElasticsearchSearch,
               post_count: int) -> None:
    """
    Функция отвечает за:                            \n
    *   получение результатов из всех очередей      \n
    *   заполнение документов ElasticsearchSearch   \n
    *   закрытие очередей и соединения с RabbitMQ   \n

    :param q: объект класса RabbitMQ
    :param es: объект класса ElasticsearchSearch
    :param post_count: количество документов/постов
    """
    queue_names = ["authors", "headers", "times", "links", "texts"]
    author, header, time, link, text = [], [], [], [], []
    try:
        for i in range(post_count):
            # Получение результатов из очереди
            author.append(q.execute_function_from_queue(queue=queue_names[0]))
            header.append(q.execute_function_from_queue(queue=queue_names[1]))
            time.append(q.execute_function_from_queue(queue=queue_names[2]))
            link.append(q.execute_function_from_queue(queue=queue_names[3]))
            text.append(q.execute_function_from_queue(queue=queue_names[4]))

        # Закрываем очереди и соединение
        for queue in queue_names:
            q.queue_close(queue=queue)
        q.channel_close()

    except Exception as e:
        # Закрытие очередей и соединения в случае ошибки
        for queue in queue_names:
            q.queue_close(queue=queue)
        q.channel_close()

        # Вывод ошибки
        print(f"An error occurred during execution: {e}")
        raise SystemExit

    for i in range(post_count):
            # Проверка наличия файла
        if es.document_exists(es.doc_id[i]):
            print(f"Document {es.doc_id[i]} is exist!")
        else:
            # Создание файла
            es.index_document(es.doc_id[i],
                              author=author[i],
                              header=header[i],
                              time=time[i],
                              link=link[i],
                              text=text[i])


def elastic_quick_start(doc_id: list) -> ElasticsearchSearch:
    """
    Функция отвечает за:                            \n
    *   создание объекта класса ElasticsearchSearch \n
    *   соединение с elasticsearchsearch            \n
    *   создание индекса с основными полями         \n

    :param doc_id: хэш каждого документа
    :return: объект класса ElasticsearchSearch
    """
    index_name = "my_index"
    es = ElasticsearchSearch(index_name, doc_id)
    es.connect()
    es.create_index(index_name)

    return es


def elastic_end(es):

    # Выполняем поиск документов
    # es.search_documents('код AND компьютер')


    # es.delete_index(index_name)
    pass


def main():
    p = parser_quick_start()

    # Сохраняем количество постов и хэши
    post_count = p.post_count
    post_id = p.post_id.copy()

    q = rabbit_quick_start(p)
    parser_end(p)

    es = elastic_quick_start(post_id)
    rabbit_end(q, es, post_count)

    elastic_end(es)


if __name__ == '__main__':
    main()
