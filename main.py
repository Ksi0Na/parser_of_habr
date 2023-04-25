import pika
import json
from typing import Optional, Any, Callable
import requests
import lxml
from bs4 import BeautifulSoup
import inspect


def get_soup(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print_error_info(e)
        raise SystemExit


def print_error_info(url: str,
                     e: Exception) -> None:
    error_text = f"Error occurred while getting HTML from {url}:"
    print(error_text)
    print(e)


def get_data(response) -> list:
    the_soup = BeautifulSoup(response, 'lxml')
    post_count = the_soup.find_all('article', {'class': 'tm-articles-list__item'}).__len__()
    post_times = []
    post_links = []
    post_headers = []

    for i in range(post_count):
        posts_of_page = the_soup.find_all('article', {'class': 'tm-articles-list__item'}).__getitem__(i)
        h2 = posts_of_page.find('h2')
        a = h2.find('a')

        post_headers.append(a.text)
        post_times.append(posts_of_page.find('time').get_attribute_list('title'))
        post_links.append('https://habr.com' + a.get_attribute_list('href')[0])

    m = []
    m.append(post_count)
    m.append(post_headers)
    m.append(post_times)
    m.append(post_links)
    return m


def print_data(m: list) -> None:
    post_count, post_headers, post_times, post_links = m

    for i in range(post_count):
        print('-' * 100)
        print(i)
        print(post_times[i])
        print(post_headers[i])
        print(post_links[i])

def print_all(m: list) -> None:
    for i in range(m[0]):
        print('-' * 100)
        print(i)
        print(m[2][i])
        print(m[1][i])
        print(m[3][i])
        print(m[4][i])

def get_text(m: list) -> None:
    post_links = m[3]
    post_count =m[0]
    post_texts = []
    for i in range(post_count):
        response = get_soup(post_links[i])
        the_soup = BeautifulSoup(response, 'lxml')
        post = the_soup.find('article', {'class': ['tm-article-presenter__content']})
        text = post.find('div', {'id': 'post-content-body'}).get_text(separator='\n')

        post_texts.append(text)
    return post_texts




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

    #     # Установка параметра prefetch_count
    #     self.channel.basic_qos(prefetch_count=1)
    #
    #     # Подписка на очередь с флагом Ack
    #     self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
    #
    #
    # # Обработчик сообщений
    # def callback(self, method, body):
    #     print("Received %r" % body)
    #     self.channel.basic_ack(delivery_tag=method.delivery_tag)  # Подтверждение доставки сообщения
    #
    # def execute_function_from_queue(self):
    #     method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=False)
    #     if method_frame:
    #         task = json.loads(body.decode('utf-8'))
    #         # обязательно подтвердить получение сообщения, используя метод basic_ack
    #         self.callback(method_frame, body)
    #         return task
    #     else:
    #         return None
    #
    #     task_name = task['function_name']
    #     task_args = task['args']
    #     task_kwargs = task['kwargs']
    #
    #     # Извлекаем имя класса и метода
    #     class_name, method_name = task_name.split('.')
    #
    #     # Получаем класс по имени
    #     cls = globals().get(class_name)
    #     if cls is None:
    #         print(f"Error: Can't find class {class_name}")
    #         return None
    #
    #     # Получаем метод по имени из класса
    #     func = getattr(cls, method_name, None)
    #     if func is None:
    #         print(f"Error: Can't find method {method_name} in class {class_name}")
    #         return None
    #
    #     # Если метод является методом экземпляра класса, создаем экземпляр класса
    #     # и передаем его первым аргументом
    #     if inspect.ismethod(func) and not inspect.isclass(cls):
    #         instance = cls()
    #         return instance, func, task_args, task_kwargs, method_frame.delivery_tag
    #     else:
    #         return None, func, task_args, task_kwargs, method_frame.delivery_tag


    def execute_function_from_queue(self):
        try:
            method_frame, frame, body = self.channel.basic_get(self.queue_name, auto_ack=False)

            if not method_frame:
                raise ValueError("No tasks in the queue")

            # Десериализуем задачу из JSON-строки
            task = json.loads(body.decode('utf-8'))
            task_name = task['task']
            task_args = task['args']
            task_kwargs = task['kwargs']
            try:
                # Выполняем задачу
                result = globals()[task_name](*task_args, **task_kwargs)
                self.channel.basic_ack(method_frame.delivery_tag)
                return result
            except KeyError:
                print(f"No function named {task_name}")
                self.channel.basic_nack(method_frame.delivery_tag, requeue=False)
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
            raise SystemExit

    def queue_close(self) -> None:
        if self.connection is not None:
            # Удаляем очередь
            self.channel.queue_delete(queue=self.queue_name)

    @staticmethod
    def channel_close(self) -> None:
        # Закрываем канал и соединение
        self.channel.close()
        self.connection.close()

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

q.queue_name = 'take_info'
q.queue_start()
q.send_task_to_queue(get_soup, 'https://habr.com/ru/flows/develop/top/weekly/')
resp = q.execute_function_from_queue()
q.send_task_to_queue(get_data, resp)
m = q.execute_function_from_queue()
q.send_task_to_queue(print_data, m)
q.execute_function_from_queue()

q.queue_name = 'do_info'
q.queue_start()
q.send_task_to_queue(get_text, m)
m.append(q.execute_function_from_queue())
q.send_task_to_queue(print_all, m)
q.execute_function_from_queue()

q.queue_name = 'take_info'
q.queue_close()
q.queue_name = 'do_info'
q.queue_close()
q.channel_close(q)



