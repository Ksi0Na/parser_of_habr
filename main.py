import pika
import json
from typing import Optional, Any, Callable
import requests
import lxml
from bs4 import BeautifulSoup


def get_html(url: str) -> str:
    result = requests.get(url)
    return result.text


def get_data(html: str) -> None:
    soup = BeautifulSoup(html, 'lxml')
    posts_count = soup.find_all('article', {'class': 'tm-articles-list__item'}).__len__()

    for i in range(posts_count):
        posts_of_page = soup.find_all('article', {'class': 'tm-articles-list__item'}).__getitem__(i)
        h2 = posts_of_page.find('h2')
        a = h2.find('a')

        post_header = a.text
        post_time = posts_of_page.find('time').get_attribute_list('title')
        post_link = 'https://habr.com' + a.get_attribute_list('href')[0]

        print('-'*100)
        print(i)
        print(post_header)
        print(post_time)
        print(post_link)


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
                 exchange_type: str = 'direct') -> None:
        """
        host - строка, представляющая адрес хоста, на котором запущен RabbitMQ.
        port - целое число, представляющее порт, на котором запущен RabbitMQ.
        username - строка, представляющая имя пользователя для аутентификации в RabbitMQ.
        password - строка, представляющая пароль для аутентификации в RabbitMQ.
        vhost - строка, представляющая виртуальный хост RabbitMQ.
        queue_name - строка, представляющая имя очереди RabbitMQ.
        exchange_name - строка, представляющая имя обменника RabbitMQ.
        exchange_type - строка, представляющая тип обменника RabbitMQ.
        connection - объект подключения к RabbitMQ.
        channel - объект канала RabbitMQ.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.queue_name = queue_name
        self.exchange_name = queue_name
        self.exchange_type = exchange_type
        self.connection = None
        self.channel = None


    def queue_start(self) -> None:
        """Создает необходимые объекты для запуска."""
        self.create_connection()
        self.channel = self.connection.channel()

        self.queue_declare()
        self.exchange_declare()
        self.queue_bind()

    def send_task_to_queue(self,
                           task: Callable[..., Any],
                           *args,
                           **kwargs):
        """Отправка задачи в очередь"""


        # Сериализуем задачу в JSON-строку
        task_json = json.dumps({'task': task.__name__, 'args': args, 'kwargs': kwargs})

        # Отправляем задачу в очередь
        self.channel.basic_publish(exchange='',
                              routing_key=self.queue_name,
                              body=task_json)

    def execute_function_from_queue(self) -> Any:
        """Получает задачу из очереди и выполняет функцию с аргументами."""

        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
        if not method_frame:
            raise ValueError("No tasks in the queue")

        # Десериализуем задачу из JSON-строки
        task = json.loads(body)
        task_name = task['task']
        task_args = task['args']
        task_kwargs = task['kwargs']

        # Выполняем задачу
        result = globals()[task_name](*task_args, **task_kwargs)

        return result


    def create_connection(self) -> Optional[pika.BlockingConnection]:
        """
        Устанавливает соединение с RabbitMQ.
        Возвращает объект типа pika.BlockingConnection при успешном соединении, иначе None.
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
            return None


    def queue_close(self) -> None:
        """Закрывает соединение с RabbitMQ."""
        if self.connection is not None:
            # Удаляем очередь
            q.channel.queue_delete(queue=self.queue_name)

            # Закрываем канал и соединение
            q.channel.close()
            q.connection.close()


    def queue_declare(self) -> None:
        """Создает очередь."""
        self.channel.queue_declare(queue=self.queue_name,
                                   durable=True)


    def exchange_declare(self) -> None:
        """Создает exchange."""
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      durable=True)

    def queue_bind(self) -> None:
        """Привязывает очередь к exchange."""
        self.channel.queue_bind(queue=self.queue_name,
                                exchange=self.exchange_name,
                                routing_key='')


q = RabbitMQ()
q.queue_start()
q.send_task_to_queue(get_html, 'https://habr.com/ru/flows/develop/top/weekly/')
html = q.execute_function_from_queue()
q.send_task_to_queue(get_data, html)
q.execute_function_from_queue()

q.queue_close()
