import pika
import requests
from bs4 import BeautifulSoup
from typing import Optional


class Parser:
    """
    Класс для парсинга данных с веб-страницы.
    """
    def __init__(
            self,
            url: str = 'https://habr.com/ru/flows/develop/top/weekly/',
            post_count: int = 0,
            post_author: list = None,
            post_headers: list = None,
            post_times: list = None,
            post_links: list = None,
            post_texts: list = None
    ):
        """
        :param url:             адрес веб-страницы (по умолчанию 'https://habr.com/ru/flows/develop/top/weekly/')
        :param post_count:      количество постов на странице (по умолчанию 0)
        :param post_author:     список авторов постов (по умолчанию пустой список)
        :param post_headers:    список заголовков постов (по умолчанию пустой список)
        :param post_times:      список времен создания постов (по умолчанию пустой список)
        :param post_links:      список ссылок на посты (по умолчанию пустой список)
        :param post_texts:      список текстов постов (по умолчанию пустой список)
        """
        self.url = url
        self.post_count = post_count

        self.post_authors = [] if (post_author is None) else post_author
        self.post_headers = [] if (post_headers is None) else post_headers
        self.post_times = [] if (post_times is None) else post_times
        self.post_links = [] if (post_links is None) else post_links
        self.post_texts = [] if (post_texts is None) else post_texts

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

            self.post_authors.append('\n'.join(line.strip() for line in author.splitlines() if line.strip()))
            self.post_headers.append(a.text)
            self.post_times.append(posts_of_page.find('time').get_attribute_list('title'))
            self.post_links.append('https://habr.com' + a.get_attribute_list('href')[0])

    def print_data(self) -> None:
        """
        Выводит данные о постах на экран.
        """
        for i in range(self.post_count):
            print('-' * 100)
            print(i)
            print(self.post_authors[i])
            print(self.post_headers[i])
            print(self.post_times[i])
            print(self.post_links[i])
            print(self.post_texts[i])

    def get_text(self) -> None:
        """
        Получает тексты постов по каждой ссылке и заполняет список post_texts.
        """
        count = self.post_count

        for i in range(count):
            response = self.get_soup(self.post_links[i])
            the_soup = BeautifulSoup(response.content, 'lxml')
            post = the_soup.find('article', {'class': ['tm-article-presenter__content']})

            text = post.find('div', {'id': 'post-content-body'}).get_text()

            self.post_texts.append(text)


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
                                    queue: str = None) -> None:
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


def main():
    try:
        # Создаем объект класса Parser
        p = Parser()
        resp = p.get_soup(p.url)
        p.get_data(resp)
        p.get_text()

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
            queue_data = [p.post_authors[i], p.post_headers[i], p.post_times[i], p.post_links[i], p.post_texts[i]]
            for queue, data in zip(queue_names, queue_data):
                q.send_task_to_queue(data, queue=queue)

        # Сохраняем количество постов перед очищением
        post_count = p.post_count

        # Очищаем списки после отправки сообщений в очереди
        p.post_authors.clear()
        p.post_headers.clear()
        p.post_times.clear()
        p.post_links.clear()
        p.post_texts.clear()
        del p

        # Получаем результаты задач из очереди
        for queue in queue_names:
            for i in range(post_count):
                q.execute_function_from_queue(queue=queue)

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


if __name__ == '__main__':
    main()
