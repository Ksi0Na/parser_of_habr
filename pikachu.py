import asyncio
import aio_pika
import json
from typing import Any, Callable, Optional
import aiohttp


class RabbitMQ:
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

    async def queue_start(self) -> None:
        """Создает необходимые объекты для запуска."""
        await self.create_connection()
        self.channel = await self.connection.channel()

        await self.queue_declare()
        await self.exchange_declare()
        await self.queue_bind()

    async def send_task_to_queue(self, task: Callable[..., Any], *args, **kwargs) -> None:
        """Отправка задачи в очередь"""

        # Сериализуем задачу в JSON-строку
        task_json = json.dumps({'task': task.__name__, 'args': args, 'kwargs': kwargs})

        # Отправляем задачу в очередь
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=task_json.encode()),
            routing_key=self.queue_name
        )

    async def execute_function_from_queue(self) -> Any:
        """Получает задачу из очереди и выполняет функцию с аргументами."""

        async with self.channel.transaction():
            message = await self.channel.get(self.queue_name, no_ack=True)

            if not message:
                raise ValueError("No tasks in the queue")

            # Десериализуем задачу из JSON-строки
            task = json.loads(message.body.decode())
            task_name = task['task']
            task_args = task['args']
            task_kwargs = task['kwargs']

            # Выполняем задачу
            result = globals()[task_name](*task_args, **task_kwargs)

            return result

    async def create_connection(self) -> Optional[aio_pika.RobustConnection]:
        """
        Устанавливает соединение с RabbitMQ.
        Возвращает объект типа aio_pika.RobustConnection при успешном соединении, иначе None.
        """
        try:
            credentials = aio_pika.PlainCredentials(username=self.username, password=self.password)
            parameters = aio_pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=credentials
            )

            self.connection = await aio_pika.connect_robust(parameters)
            return self.connection
        except aio_pika.exceptions.AMQPConnectionError as error:
            print(f"Failed to connect to RabbitMQ on {self.host}:{self.port}: {error}")
            return None

    async def queue_close(self) -> None:
        """Закрывает соединение с RabbitMQ."""
        if self.connection is not None:
            # Удаляем очередь
            await self.channel.queue_delete(queue_name=self.queue_name)

            # Закрываем канал и соединение
            await self.channel.close()
            await self.connection.close()

    async def queue_declare(self) -> None:
        """Создает очередь."""
        await self.channel.declare_queue(queue_name=self.queue_name,
                                         durable=True)

    async def exchange_declare(self) -> None:
        """Создает exchange."""
        await self.channel.declare_exchange(exchange_name=self.exchange_name,
                                            exchange_type=self.exchange_type,
                                            durable=True)

    async def queue_bind(self) -> None:
        """Привязывает очередь к exchange."""
        await self.channel.bind_queue(queue_name=self.queue_name,
                                      exchange_name=self.exchange_name,
                                      routing_key='')
